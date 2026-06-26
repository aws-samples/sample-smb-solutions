# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""FastMCP server core, input validation, and response shaping.

This module owns the FastMCP instance and the building blocks the tool layer
(task 11.2) and ``main()`` (task 11.3) compose: a shared helper that builds a
validated :class:`~aws_events_mcp.models.EventQuery` from raw tool arguments,
cross-field/semantic validators the input schema cannot express, and the
response-shaping helpers that produce the listing/success, validation-error,
source-error, and not-found dictionaries described in the design's "Standard
response shapes" and "Error Handling" sections.

Validation philosophy
----------------------
Per the design, ``pydantic.Field(...)`` constraints on the tool arguments
(declared in task 11.2) reject schema-level violations such as out-of-range
``page_size`` or over-length strings before a tool body runs. The validators
here cover the *semantic* and *cross-field* rules a JSON schema cannot express
(Requirement NFR Usability): whitespace-only strings, case-insensitive enum
membership with a corrective message listing the accepted values, the
``YYYY-MM-DD`` date format, the ``start_date <= end_date`` ordering, and
page-token decoding. A failed validator raises :class:`ToolValidationError`,
which the tool layer converts into the structured validation-error response via
:func:`validation_response`; every such response carries no event records
(Requirements 1.6, 3.6, 3.7, 5.4, 6.6, 6.7, 7.6, 7.7, 8.5, 9.5).

Logging
-------
loguru is configured exactly as the sibling project: the default sink is
removed and a single stderr sink is added at the ``FASTMCP_LOG_LEVEL`` level.
All logs go to stderr because stdout is reserved for the MCP stdio transport's
JSON-RPC stream.
"""

import os
import re
import sys
from aws_events_mcp import consts
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.errors import (
    CatalogSourceError,
    not_found,
    source_error,
    validation_error,
)
from aws_events_mcp.models import (
    Event,
    EventPage,
    EventQuery,
    LearningLevel,
    LocationMode,
)
from aws_events_mcp.pagination import PageToken, PageTokenError
from aws_events_mcp.query import apply_query, paginate
from aws_events_mcp.source import JsonApiCatalogSource
from datetime import date, datetime, timezone
from loguru import logger
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from typing import Any, Dict, List, Literal, Optional, Union, cast


# --- Logging configuration ---------------------------------------------------
# Configured exactly as the sibling project: drop loguru's default sink and add
# a single stderr sink. stdout is reserved for the MCP stdio JSON-RPC stream, so
# logs must never be written there.
logger.remove()
logger.add(sys.stderr, level=os.getenv('FASTMCP_LOG_LEVEL', 'INFO'))


# --- FastMCP instance --------------------------------------------------------

SERVER_INSTRUCTIONS = """AWS Events MCP Server

This server exposes the public AWS Events catalog (Tech Talks, webinars, summits,
roadshows, networking events, and partner events) as Model Context Protocol tools.
It lets an agent discover, search, and filter AWS events and retrieve full event
details with registration links. No AWS credentials are required; only the public
catalog is contacted.

**Available Tools:**
1. `list_events` - List events ordered by start date ascending, with optional
   filters for learning level, location mode/text, event type, partner, and date
   range, plus pagination.
2. `list_upcoming_events` - Same as `list_events` but constrained to events whose
   start date is today (UTC) or later.
3. `search_events` - Keyword substring search over event title/description,
   combinable with every `list_events` filter.
4. `search_upcoming_events` - Keyword substring search constrained to upcoming
   events (start date today UTC or later); the search counterpart of
   `list_upcoming_events`.
5. `get_event_details` - Retrieve a single event by identifier with all twelve
   presentation fields always present.

**Filtering conventions:**
- Learning level matches one of Foundational, Intermediate, Advanced, or Expert
  (case-insensitive); supply one value or a list of up to four.
- Location mode is `virtual` or `physical`; location text is a case-insensitive
  substring of the event location.
- Dates use the ISO 8601 calendar format `YYYY-MM-DD`; `start_date` must not be
  later than `end_date`.
- Page size defaults to 20 and may range from 1 to 100; opaque page tokens walk
  successive pages of a result set.
"""

#: The FastMCP application instance. Tools are registered onto this in task 11.2
#: and it is served by ``main()`` in task 11.3.
mcp = FastMCP(
    'aws-events-mcp',
    instructions=SERVER_INSTRUCTIONS,
    dependencies=['pydantic', 'loguru', 'httpx'],
)


# --- Catalog cache wiring ----------------------------------------------------
# The tool layer depends on a single process-wide CatalogCache so the parsed
# catalog is fetched once per TTL window and shared across tool invocations
# (NFR Performance). The cache is built lazily on first use, backed by the
# primary JSON-API source, so importing this module performs no network I/O.

#: Process-wide catalog cache, created lazily by ``get_catalog_cache``.
_catalog_cache: Optional[CatalogCache] = None


def _resolve_cache_ttl_seconds() -> int:
    """Resolve the catalog cache TTL from the environment, with a safe default.

    Returns:
        The TTL in seconds from ``AWS_EVENTS_CACHE_TTL_SECONDS`` when it holds a
        positive integer; otherwise ``consts.DEFAULT_CACHE_TTL_SECONDS``.
    """
    raw = os.getenv(consts.ENV_CACHE_TTL_SECONDS)
    if raw is None:
        return consts.DEFAULT_CACHE_TTL_SECONDS
    try:
        ttl = int(raw)
    except ValueError:
        logger.warning(
            f'Ignoring invalid {consts.ENV_CACHE_TTL_SECONDS}={raw!r}; '
            f'using default {consts.DEFAULT_CACHE_TTL_SECONDS}s.'
        )
        return consts.DEFAULT_CACHE_TTL_SECONDS
    if ttl <= 0:
        logger.warning(
            f'Ignoring non-positive {consts.ENV_CACHE_TTL_SECONDS}={ttl}; '
            f'using default {consts.DEFAULT_CACHE_TTL_SECONDS}s.'
        )
        return consts.DEFAULT_CACHE_TTL_SECONDS
    return ttl


def get_catalog_cache() -> CatalogCache:
    """Return the shared catalog cache, creating it on first use.

    The cache is backed by :class:`~aws_events_mcp.source.JsonApiCatalogSource`
    (the primary content-directory strategy) and reused across all tool calls so
    the catalog is fetched and parsed at most once per TTL window.

    Returns:
        The process-wide :class:`~aws_events_mcp.catalog.CatalogCache` instance.
    """
    global _catalog_cache
    if _catalog_cache is None:
        _catalog_cache = CatalogCache(
            JsonApiCatalogSource(), ttl_seconds=_resolve_cache_ttl_seconds()
        )
    return _catalog_cache


def set_catalog_cache(cache: Optional[CatalogCache]) -> None:
    """Replace (or clear) the shared catalog cache.

    Primarily a seam for ``main()`` (task 11.3) to inject a configured cache and
    for tests to substitute a cache backed by a mock source. Passing ``None``
    clears the cache so the next :func:`get_catalog_cache` call rebuilds the
    default JSON-API-backed cache.

    Args:
        cache: The cache to install, or ``None`` to reset to lazy default.
    """
    global _catalog_cache
    _catalog_cache = cache


# --- Validation primitives ---------------------------------------------------


class ToolValidationError(Exception):
    """Raised when a tool argument fails a semantic or cross-field validator.

    Carries the offending field name and a corrective, human-readable message so
    the tool layer can build a structured validation-error response with no event
    records (Requirement NFR Usability).

    Attributes:
        field: Name of the input argument that failed validation.
        message: Human-readable message stating how to correct the input.
    """

    def __init__(self, field: str, message: str) -> None:
        """Initialize the error.

        Args:
            field: Name of the input argument that failed validation.
            message: Human-readable message stating how to correct the input.
        """
        self.field = field
        self.message = message
        super().__init__(message)


#: ISO 8601 calendar date shape accepted for date arguments (Requirement 7.6).
_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')

#: Case-insensitive lookup from a learning-level string to its enum member.
_LEARNING_LEVELS_BY_CASEFOLD = {level.value.casefold(): level for level in LearningLevel}

#: Case-insensitive lookup from a location-mode string to its enum member.
_LOCATION_MODES_BY_CASEFOLD = {mode.value.casefold(): mode for mode in LocationMode}

#: Accepted learning-level values, listed in corrective validation messages.
_ACCEPTED_LEARNING_LEVELS = ', '.join(level.value for level in LearningLevel)

#: Accepted location-mode values, listed in corrective validation messages.
_ACCEPTED_LOCATION_MODES = ', '.join(mode.value for mode in LocationMode)


def _require_non_blank(value: str, field: str, max_length: int) -> str:
    """Validate and normalize a bounded free-text filter argument.

    Trims surrounding whitespace and rejects values that are empty, whitespace
    only, or longer than ``max_length`` characters (the length bound mirrors the
    schema-level ``Field(max_length=...)`` so the rule still holds when a caller
    bypasses the schema).

    Args:
        value: The raw argument value.
        field: The argument name, used in the error and returned unchanged.
        max_length: The inclusive maximum number of characters permitted.

    Returns:
        The whitespace-trimmed value.

    Raises:
        ToolValidationError: If the value is blank or exceeds ``max_length``.
    """
    if len(value) > max_length:
        raise ToolValidationError(
            field,
            f'The {field} must be between 1 and {max_length} characters; '
            f'the supplied value is {len(value)} characters.',
        )
    trimmed = value.strip()
    if not trimmed:
        raise ToolValidationError(
            field,
            f'The {field} must be a non-empty string between 1 and {max_length} characters.',
        )
    return trimmed


def _normalize_learning_levels(
    learning_level: Union[str, List[str], None],
) -> List[LearningLevel]:
    """Normalize the learning-level argument into a list of enum members.

    Accepts a single string or a list of strings, matching each value
    case-insensitively against the four accepted learning levels. An absent or
    empty argument yields an empty list, which imposes no learning-level filter
    (Requirement 5.5).

    Args:
        learning_level: A single learning-level string, a list of such strings,
            or ``None``.

    Returns:
        The matched ``LearningLevel`` members, in the supplied order.

    Raises:
        ToolValidationError: If any supplied value is blank or is not a
            case-insensitive match to one of the four accepted values
            (Requirement 5.4).
    """
    if learning_level is None:
        return []
    raw_values = [learning_level] if isinstance(learning_level, str) else list(learning_level)

    levels: List[LearningLevel] = []
    for raw in raw_values:
        candidate = raw.strip() if isinstance(raw, str) else ''
        matched = _LEARNING_LEVELS_BY_CASEFOLD.get(candidate.casefold()) if candidate else None
        if matched is None:
            raise ToolValidationError(
                'learning_level',
                f"Learning level '{raw}' is invalid. Accepted values are: "
                f'{_ACCEPTED_LEARNING_LEVELS}.',
            )
        levels.append(matched)
    return levels


def _normalize_location_mode(location_mode: Optional[str]) -> Optional[LocationMode]:
    """Normalize the location-mode argument into an enum member.

    Args:
        location_mode: A location-mode string or ``None``.

    Returns:
        The matched ``LocationMode`` member, or ``None`` when no mode is supplied
        (which imposes no location-mode filter, Requirement 6.4).

    Raises:
        ToolValidationError: If the value is not a case-insensitive match to
            ``virtual`` or ``physical`` (Requirement 6.6).
    """
    if location_mode is None:
        return None
    candidate = location_mode.strip()
    matched = _LOCATION_MODES_BY_CASEFOLD.get(candidate.casefold()) if candidate else None
    if matched is None:
        raise ToolValidationError(
            'location_mode',
            f"Location mode '{location_mode}' is invalid. Accepted values are: "
            f'{_ACCEPTED_LOCATION_MODES}.',
        )
    return matched


def _parse_date_argument(value: Optional[str], field: str) -> Optional[date]:
    """Parse and validate an ISO 8601 calendar-date argument.

    Args:
        value: The raw date string in ``YYYY-MM-DD`` form, or ``None``.
        field: The argument name, used in the error message.

    Returns:
        The parsed ``date``, or ``None`` when no value is supplied.

    Raises:
        ToolValidationError: If the value is not a valid ``YYYY-MM-DD`` calendar
            date (Requirement 7.6).
    """
    if value is None:
        return None
    if not _DATE_PATTERN.match(value):
        raise ToolValidationError(
            field,
            f"The {field} '{value}' is invalid; the expected date format is YYYY-MM-DD.",
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ToolValidationError(
            field,
            f"The {field} '{value}' is invalid; the expected date format is YYYY-MM-DD.",
        ) from exc


def build_event_query(
    *,
    learning_level: Union[str, List[str], None] = None,
    location_mode: Optional[str] = None,
    location_text: Optional[str] = None,
    event_type: Optional[str] = None,
    partner: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    keyword: Optional[str] = None,
) -> EventQuery:
    """Build a validated ``EventQuery`` from raw tool arguments.

    Applies the cross-field and semantic validators the input schema cannot
    express, so every listing and search tool shares one consistent definition of
    a valid query (Requirements 5-8). Absent or empty arguments impose no filter
    along their dimension (Requirements 5.5, 6.4). All string filters are trimmed
    of surrounding whitespace.

    Args:
        learning_level: A single learning-level string or a list of up to four
            (case-insensitive; Requirements 5.1, 5.2, 5.4).
        location_mode: ``virtual`` or ``physical`` (case-insensitive;
            Requirement 6.6).
        location_text: Case-insensitive substring matched against the event
            location, 1-200 characters (Requirements 6.3, 6.7).
        event_type: Case-insensitive exact match against the event type, 1-256
            characters (Requirements 8.1, 8.5).
        partner: Case-insensitive substring matched against the partner name,
            1-256 characters (Requirements 8.2, 8.5).
        start_date: Inclusive lower bound on the event start date, ``YYYY-MM-DD``
            (Requirements 7.1, 7.6).
        end_date: Inclusive upper bound on the event start date, ``YYYY-MM-DD``
            (Requirements 7.2, 7.6).
        keyword: Case-insensitive substring matched against title or description,
            1-256 characters; used by the search tool (Requirements 4.1, 4.4).

    Returns:
        A normalized, validated ``EventQuery``.

    Raises:
        ToolValidationError: If any argument fails a semantic or cross-field
            validator. The carried ``field``/``message`` drive the structured
            validation-error response, which returns no event records
            (Requirements 4.4, 5.4, 6.6, 6.7, 7.6, 7.7, 8.5).
    """
    levels = _normalize_learning_levels(learning_level)
    mode = _normalize_location_mode(location_mode)

    normalized_keyword = (
        _require_non_blank(keyword, 'keyword', 256) if keyword is not None else None
    )
    normalized_location_text = (
        _require_non_blank(location_text, 'location_text', 200)
        if location_text is not None
        else None
    )
    normalized_event_type = (
        _require_non_blank(event_type, 'event_type', 256) if event_type is not None else None
    )
    normalized_partner = (
        _require_non_blank(partner, 'partner', 256) if partner is not None else None
    )

    parsed_start = _parse_date_argument(start_date, 'start_date')
    parsed_end = _parse_date_argument(end_date, 'end_date')
    if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
        raise ToolValidationError(
            'start_date',
            'The start date must not be later than the end date.',
        )

    return EventQuery(
        keyword=normalized_keyword,
        learning_levels=levels,
        location_mode=mode,
        location_text=normalized_location_text,
        event_type=normalized_event_type,
        partner=normalized_partner,
        start_date=parsed_start,
        end_date=parsed_end,
    )


def resolve_page_offset(page_token: Optional[str], query: EventQuery) -> int:
    """Resolve the starting offset for a request from an optional page token.

    Args:
        page_token: The opaque page token from a prior response, or ``None`` to
            start from the first page.
        query: The active query the token must match; its fingerprint guards
            against replaying a token against a different query.

    Returns:
        The zero-based offset into the matched set: ``0`` when no token is
        supplied, otherwise the offset recovered from the token.

    Raises:
        ToolValidationError: If the token is malformed or does not match the
            active query (Requirement 3.7).
    """
    if page_token is None:
        return 0
    try:
        return PageToken.decode(page_token, query).offset
    except PageTokenError as exc:
        raise ToolValidationError(
            'page_token',
            'The page token is invalid; omit it to start from the first page or supply a '
            'token returned by a prior response for the same query.',
        ) from exc


# --- Response shaping --------------------------------------------------------


def build_listing_response(page: EventPage, page_size: int, query: EventQuery) -> Dict[str, Any]:
    """Build the success response for a listing or search tool.

    Serializes each event with ``model_dump(mode='json')`` and echoes the applied
    ``page_size`` and the matched ``total_count``. A ``next_page_token`` is added
    only when the page reports a ``next_offset`` (more matches remain), minting
    the token from that offset bound to the active query (Requirements 2.2, 2.3,
    3.3, 3.4).

    Args:
        page: The paginated result produced by ``query.paginate``.
        page_size: The applied page size to echo back to the caller.
        query: The active query, used to mint the next-page token's fingerprint.

    Returns:
        A response dict with ``status`` ``'success'``, the serialized ``items``,
        the ``total_count``, the ``page_size``, and a ``next_page_token`` only
        when a further page remains.
    """
    response: Dict[str, Any] = {
        'status': 'success',
        'items': [event.model_dump(mode='json') for event in page.items],
        'total_count': page.total_count,
        'page_size': page_size,
    }
    if page.next_offset is not None:
        response['next_page_token'] = PageToken.create(page.next_offset, query).encode()
    return response


def build_event_details_response(event: Event) -> Dict[str, Any]:
    """Build the success response for ``get_event_details``.

    Serializes the event with ``model_dump(mode='json')`` so all twelve
    presentation fields are present, with absent optional values emitted as
    explicit ``null`` rather than omitted (Requirements 9.2, 9.3).

    Args:
        event: The matched event.

    Returns:
        A response dict with ``status`` ``'success'`` and the serialized
        ``event``.
    """
    return {
        'status': 'success',
        'event': event.model_dump(mode='json'),
    }


def validation_response(exc: ToolValidationError) -> Dict[str, Any]:
    """Convert a ``ToolValidationError`` into a structured validation response.

    Args:
        exc: The validation error raised by a query builder or token resolver.

    Returns:
        The structured validation-error response, carrying the offending field,
        the corrective message, an empty ``items`` list, and a ``total_count`` of
        ``0`` (no event records).
    """
    return validation_error(exc.field, exc.message)


def build_source_error_response(exc: CatalogSourceError) -> Dict[str, Any]:
    """Map a ``CatalogSourceError`` onto its structured source-error response.

    Uses the exception's ``error_type`` attribute, so each subtype routes to the
    correct response ``error_type`` (``source_unreachable``, ``source_timeout``,
    ``source_unparseable``, or ``source_partial``). The response excludes all
    partial or unparsed catalog data (Requirements 2.5, 11.1-11.5).

    Args:
        exc: The catalog source/parse error raised by the cache layer.

    Returns:
        A structured source-error response with the mapped ``error_type``, an
        empty ``items`` list, and a ``total_count`` of ``0``.
    """
    message = str(exc) or 'The AWS Events catalog could not be retrieved.'
    return source_error(exc.error_type, message)


# --- Tool helpers ------------------------------------------------------------


def _constrain_to_upcoming(query: EventQuery) -> EventQuery:
    """Constrain a query's start-date lower bound to today (UTC) or later.

    Implements the ``list_upcoming_events`` semantics (Requirement 7.3): the
    effective lower bound is the later of today's UTC date and any user-supplied
    ``start_date``. Baking the bound into the returned query means the shared
    date-range filter in :func:`aws_events_mcp.query.apply_query` enforces it,
    and the same query drives page-token fingerprinting so paging stays
    consistent within the request.

    Args:
        query: The query built from the tool arguments.

    Returns:
        A copy of ``query`` whose ``start_date`` is ``max(today_utc,
        query.start_date)`` (or today's UTC date when no start date was given).
    """
    today_utc = datetime.now(timezone.utc).date()
    effective_start = today_utc if query.start_date is None else max(query.start_date, today_utc)
    return query.model_copy(update={'start_date': effective_start})


async def _run_listing(
    query: EventQuery, page_size: int, page_token: Optional[str]
) -> Dict[str, Any]:
    """Resolve paging, fetch the catalog, then filter and paginate.

    Shared by every listing/search tool so filtering, pagination, and error
    mapping behave identically (Requirements 2.x, 3.x). Resolves the page offset
    from the token (validating it against ``query``), fetches the cached catalog,
    applies the query, and slices a single bounded page.

    Args:
        query: The validated, normalized query to apply.
        page_size: The applied page size (already schema-validated to 1-100).
        page_token: An opaque page token from a prior response, or ``None``.

    Returns:
        A listing success response, or a structured validation/source-error
        response carrying no event records.
    """
    try:
        offset = resolve_page_offset(page_token, query)
    except ToolValidationError as exc:
        return validation_response(exc)

    try:
        events = await get_catalog_cache().get_events()
    except CatalogSourceError as exc:
        return build_source_error_response(exc)

    matched = apply_query(events, query)
    page = paginate(matched, page_size, offset)
    return build_listing_response(page, page_size, query)


def _require_event_id(event_id: str) -> str:
    """Validate the ``get_event_details`` identifier argument.

    Args:
        event_id: The raw identifier argument.

    Returns:
        The whitespace-trimmed identifier.

    Raises:
        ToolValidationError: If the identifier is missing, empty, or
            whitespace-only (Requirement 9.5).
    """
    trimmed = event_id.strip() if isinstance(event_id, str) else ''
    if not trimmed:
        raise ToolValidationError(
            'event_id',
            'A valid event identifier is required; it must be a non-empty, non-whitespace string.',
        )
    return trimmed


# --- Tool argument descriptions ----------------------------------------------
# Field descriptions are shared verbatim across the listing/search tools so the
# generated input schemas document each argument identically (NFR Usability).

_DESC_LEARNING_LEVEL = (
    'Optional learning levels to match (case-insensitive). Supply one value or '
    'up to four of: Foundational, Intermediate, Advanced, Expert. An event '
    'matches when its level equals any supplied value. Omit to apply no '
    'learning-level filter.'
)
_DESC_LOCATION_MODE = (
    "Optional delivery mode: 'virtual' for online events or 'physical' for "
    'in-person events (case-insensitive). Omit to include both modes.'
)
_DESC_LOCATION_TEXT = (
    'Optional location text (1-200 characters) matched as a case-insensitive '
    "substring of the event location, e.g. 'Seattle'. Omit to apply no location "
    'text filter.'
)
_DESC_EVENT_TYPE = (
    'Optional event type (1-256 characters) matched case-insensitively and '
    "exactly against the event category, e.g. 'Tech Talk' or 'Summit'. Omit to "
    'apply no event-type filter.'
)
_DESC_PARTNER = (
    'Optional partner name (1-256 characters) matched as a case-insensitive '
    'substring of the event partner. Omit to apply no partner filter.'
)
_DESC_START_DATE = (
    'Optional inclusive lower bound on the event start date, in ISO 8601 '
    'calendar format YYYY-MM-DD. Must not be later than end_date.'
)
_DESC_END_DATE = (
    'Optional inclusive upper bound on the event start date, in ISO 8601 '
    'calendar format YYYY-MM-DD. Must not be earlier than start_date.'
)
_DESC_PAGE_SIZE = (
    'Maximum number of events to return in this page, an integer between 1 and '
    '100 inclusive. Defaults to 20.'
)
_DESC_PAGE_TOKEN = (
    'Opaque token from a prior response used to fetch the next page of the same '
    'query. Omit to start from the first page.'
)


# --- Catalog tools -----------------------------------------------------------


@mcp.tool()
async def list_events(
    learning_level: Optional[List[str]] = Field(default=None, description=_DESC_LEARNING_LEVEL),
    location_mode: Optional[str] = Field(default=None, description=_DESC_LOCATION_MODE),
    location_text: Optional[str] = Field(
        default=None, max_length=200, description=_DESC_LOCATION_TEXT
    ),
    event_type: Optional[str] = Field(default=None, max_length=256, description=_DESC_EVENT_TYPE),
    partner: Optional[str] = Field(default=None, max_length=256, description=_DESC_PARTNER),
    start_date: Optional[str] = Field(default=None, description=_DESC_START_DATE),
    end_date: Optional[str] = Field(default=None, description=_DESC_END_DATE),
    page_size: int = Field(
        default=consts.DEFAULT_PAGE_SIZE, ge=1, le=100, description=_DESC_PAGE_SIZE
    ),
    page_token: Optional[str] = Field(default=None, description=_DESC_PAGE_TOKEN),
) -> Dict[str, Any]:
    """List AWS events ordered by start date ascending, with optional filters.

    Returns a page of events from the public AWS Events catalog. Every filter is
    optional and combines with logical AND; omitting a filter applies no
    constraint along that dimension. Results are ordered by start date ascending
    and paginated (Requirements 2.x, 3.x, 5.x, 6.x, 7.x, 8.x).

    Args:
        learning_level: One value or a list of up to four learning levels to
            match (case-insensitive): Foundational, Intermediate, Advanced, or
            Expert.
        location_mode: ``virtual`` or ``physical`` (case-insensitive).
        location_text: Case-insensitive substring (1-200 chars) of the location.
        event_type: Case-insensitive exact match (1-256 chars) of the event type.
        partner: Case-insensitive substring (1-256 chars) of the partner name.
        start_date: Inclusive lower bound on the start date (``YYYY-MM-DD``).
        end_date: Inclusive upper bound on the start date (``YYYY-MM-DD``).
        page_size: Maximum events per page, an integer in 1-100 (default 20).
        page_token: Opaque token from a prior response for the next page.

    Returns:
        A success response with the matched ``items``, the ``total_count`` of all
        matching events, the applied ``page_size``, and a ``next_page_token``
        when more matches remain; or a structured validation/source-error
        response carrying no records.
    """
    try:
        query = build_event_query(
            learning_level=learning_level,
            location_mode=location_mode,
            location_text=location_text,
            event_type=event_type,
            partner=partner,
            start_date=start_date,
            end_date=end_date,
        )
    except ToolValidationError as exc:
        return validation_response(exc)
    return await _run_listing(query, page_size, page_token)


@mcp.tool()
async def list_upcoming_events(
    learning_level: Optional[List[str]] = Field(default=None, description=_DESC_LEARNING_LEVEL),
    location_mode: Optional[str] = Field(default=None, description=_DESC_LOCATION_MODE),
    location_text: Optional[str] = Field(
        default=None, max_length=200, description=_DESC_LOCATION_TEXT
    ),
    event_type: Optional[str] = Field(default=None, max_length=256, description=_DESC_EVENT_TYPE),
    partner: Optional[str] = Field(default=None, max_length=256, description=_DESC_PARTNER),
    start_date: Optional[str] = Field(default=None, description=_DESC_START_DATE),
    end_date: Optional[str] = Field(default=None, description=_DESC_END_DATE),
    page_size: int = Field(
        default=consts.DEFAULT_PAGE_SIZE, ge=1, le=100, description=_DESC_PAGE_SIZE
    ),
    page_token: Optional[str] = Field(default=None, description=_DESC_PAGE_TOKEN),
) -> Dict[str, Any]:
    """List upcoming AWS events (start date today UTC or later), with filters.

    Behaves like :func:`list_events` but additionally constrains results to
    events whose start date is the current UTC date or later (Requirement 7.3).
    When a ``start_date`` is also supplied, the effective lower bound is the
    later of today (UTC) and that date, so the tool never returns past events.
    All other filters and pagination behave as for :func:`list_events`.

    Args:
        learning_level: One value or a list of up to four learning levels to
            match (case-insensitive): Foundational, Intermediate, Advanced, or
            Expert.
        location_mode: ``virtual`` or ``physical`` (case-insensitive).
        location_text: Case-insensitive substring (1-200 chars) of the location.
        event_type: Case-insensitive exact match (1-256 chars) of the event type.
        partner: Case-insensitive substring (1-256 chars) of the partner name.
        start_date: Inclusive lower bound on the start date (``YYYY-MM-DD``);
            intersected with today (UTC) so the later of the two applies.
        end_date: Inclusive upper bound on the start date (``YYYY-MM-DD``).
        page_size: Maximum events per page, an integer in 1-100 (default 20).
        page_token: Opaque token from a prior response for the next page.

    Returns:
        A success response with only upcoming events, the ``total_count`` of all
        upcoming matches, the applied ``page_size``, and a ``next_page_token``
        when more matches remain; or a structured validation/source-error
        response carrying no records.
    """
    try:
        query = build_event_query(
            learning_level=learning_level,
            location_mode=location_mode,
            location_text=location_text,
            event_type=event_type,
            partner=partner,
            start_date=start_date,
            end_date=end_date,
        )
    except ToolValidationError as exc:
        return validation_response(exc)
    query = _constrain_to_upcoming(query)
    return await _run_listing(query, page_size, page_token)


@mcp.tool()
async def search_events(
    keyword: str = Field(
        ...,
        max_length=256,
        description=(
            'Required search keyword (1-256 characters) matched as a '
            'case-insensitive substring of each event title or description.'
        ),
    ),
    learning_level: Optional[List[str]] = Field(default=None, description=_DESC_LEARNING_LEVEL),
    location_mode: Optional[str] = Field(default=None, description=_DESC_LOCATION_MODE),
    location_text: Optional[str] = Field(
        default=None, max_length=200, description=_DESC_LOCATION_TEXT
    ),
    event_type: Optional[str] = Field(default=None, max_length=256, description=_DESC_EVENT_TYPE),
    partner: Optional[str] = Field(default=None, max_length=256, description=_DESC_PARTNER),
    start_date: Optional[str] = Field(default=None, description=_DESC_START_DATE),
    end_date: Optional[str] = Field(default=None, description=_DESC_END_DATE),
    page_size: int = Field(
        default=consts.DEFAULT_PAGE_SIZE, ge=1, le=100, description=_DESC_PAGE_SIZE
    ),
    page_token: Optional[str] = Field(default=None, description=_DESC_PAGE_TOKEN),
) -> Dict[str, Any]:
    """Search AWS events by keyword, combinable with every list_events filter.

    Returns events whose title or description contains ``keyword`` as a
    case-insensitive substring (Requirement 4.1), further narrowed by any of the
    optional filters shared with :func:`list_events` (Requirements 5.x-8.x).
    Results are ordered by start date ascending and paginated.

    Args:
        keyword: Required substring (1-256 chars) matched case-insensitively
            against each event's title or description.
        learning_level: One value or a list of up to four learning levels to
            match (case-insensitive): Foundational, Intermediate, Advanced, or
            Expert.
        location_mode: ``virtual`` or ``physical`` (case-insensitive).
        location_text: Case-insensitive substring (1-200 chars) of the location.
        event_type: Case-insensitive exact match (1-256 chars) of the event type.
        partner: Case-insensitive substring (1-256 chars) of the partner name.
        start_date: Inclusive lower bound on the start date (``YYYY-MM-DD``).
        end_date: Inclusive upper bound on the start date (``YYYY-MM-DD``).
        page_size: Maximum events per page, an integer in 1-100 (default 20).
        page_token: Opaque token from a prior response for the next page.

    Returns:
        A success response with the matched ``items``, the ``total_count`` of all
        matching events, the applied ``page_size``, and a ``next_page_token``
        when more matches remain; or a structured validation/source-error
        response carrying no records.
    """
    try:
        query = build_event_query(
            keyword=keyword,
            learning_level=learning_level,
            location_mode=location_mode,
            location_text=location_text,
            event_type=event_type,
            partner=partner,
            start_date=start_date,
            end_date=end_date,
        )
    except ToolValidationError as exc:
        return validation_response(exc)
    return await _run_listing(query, page_size, page_token)


@mcp.tool()
async def search_upcoming_events(
    keyword: str = Field(
        ...,
        max_length=256,
        description=(
            'Required search keyword (1-256 characters) matched as a '
            'case-insensitive substring of each event title or description.'
        ),
    ),
    learning_level: Optional[List[str]] = Field(default=None, description=_DESC_LEARNING_LEVEL),
    location_mode: Optional[str] = Field(default=None, description=_DESC_LOCATION_MODE),
    location_text: Optional[str] = Field(
        default=None, max_length=200, description=_DESC_LOCATION_TEXT
    ),
    event_type: Optional[str] = Field(default=None, max_length=256, description=_DESC_EVENT_TYPE),
    partner: Optional[str] = Field(default=None, max_length=256, description=_DESC_PARTNER),
    start_date: Optional[str] = Field(default=None, description=_DESC_START_DATE),
    end_date: Optional[str] = Field(default=None, description=_DESC_END_DATE),
    page_size: int = Field(
        default=consts.DEFAULT_PAGE_SIZE, ge=1, le=100, description=_DESC_PAGE_SIZE
    ),
    page_token: Optional[str] = Field(default=None, description=_DESC_PAGE_TOKEN),
) -> Dict[str, Any]:
    """Search upcoming AWS events by keyword, restricted to today (UTC) or later.

    Combines the keyword substring search of :func:`search_events` with the
    upcoming-only constraint of :func:`list_upcoming_events`. Returns events
    whose title or description contains ``keyword`` as a case-insensitive
    substring (Requirement 12.1) and whose start date is the current UTC date or
    later (Requirement 12.3), further narrowed by any of the optional filters
    shared with :func:`list_events` (Requirements 12.5). When a ``start_date`` is
    also supplied, the effective lower bound is the later of today (UTC) and that
    date, so the tool never returns past events (Requirement 12.4). Results are
    ordered by start date ascending and paginated (Requirements 12.6, 12.7).

    Args:
        keyword: Required substring (1-256 chars) matched case-insensitively
            against each event's title or description.
        learning_level: One value or a list of up to four learning levels to
            match (case-insensitive): Foundational, Intermediate, Advanced, or
            Expert.
        location_mode: ``virtual`` or ``physical`` (case-insensitive).
        location_text: Case-insensitive substring (1-200 chars) of the location.
        event_type: Case-insensitive exact match (1-256 chars) of the event type.
        partner: Case-insensitive substring (1-256 chars) of the partner name.
        start_date: Inclusive lower bound on the start date (``YYYY-MM-DD``);
            intersected with today (UTC) so the later of the two applies.
        end_date: Inclusive upper bound on the start date (``YYYY-MM-DD``).
        page_size: Maximum events per page, an integer in 1-100 (default 20).
        page_token: Opaque token from a prior response for the next page.

    Returns:
        A success response with only upcoming events matching the keyword, the
        ``total_count`` of all such matches, the applied ``page_size``, and a
        ``next_page_token`` when more matches remain; or a structured
        validation/source-error response carrying no records.
    """
    try:
        query = build_event_query(
            keyword=keyword,
            learning_level=learning_level,
            location_mode=location_mode,
            location_text=location_text,
            event_type=event_type,
            partner=partner,
            start_date=start_date,
            end_date=end_date,
        )
    except ToolValidationError as exc:
        return validation_response(exc)
    query = _constrain_to_upcoming(query)
    return await _run_listing(query, page_size, page_token)


@mcp.tool()
async def get_event_details(
    event_id: str = Field(
        ...,
        description=(
            'Required unique identifier of the event to retrieve, as returned in the '
            "'event_id' field of a listing or search result. Must be non-empty."
        ),
    ),
) -> Dict[str, Any]:
    """Retrieve the full details of a single AWS event by its identifier.

    Returns the event whose identifier exactly matches ``event_id`` with all
    twelve presentation fields always present (absent optional values serialized
    as explicit ``null``; Requirements 9.1-9.3). When no event has the supplied
    identifier, a not-found result is returned instead (Requirement 9.4).

    Args:
        event_id: The unique, non-empty identifier of the event to retrieve.

    Returns:
        A success response with the ``event`` when a match is found; a not-found
        response when no event matches the identifier; or a structured
        validation/source-error response carrying no event.
    """
    try:
        target_id = _require_event_id(event_id)
    except ToolValidationError as exc:
        return validation_response(exc)

    try:
        events = await get_catalog_cache().get_events()
    except CatalogSourceError as exc:
        return build_source_error_response(exc)

    for event in events:
        if event.event_id == target_id:
            return build_event_details_response(event)
    return not_found(f"No event matched identifier '{event_id}'.")


# --- Startup, transport handling, and entry point ----------------------------
# ``main()`` is intentionally synchronous: FastMCP.run owns the event loop. It
# composes two small, independently unit-testable helpers (task 11.11 targets
# them directly) that each abort startup with a descriptive error rather than
# starting a half-configured server:
#   * ``_verify_tools_registered`` -> Requirement 1.2 (registration failure)
#   * ``_resolve_transport``       -> Requirements 1.4/1.5 (transport selection)

#: Transport string accepted by ``FastMCP.run`` (mirrors ``consts.SUPPORTED_TRANSPORTS``).
TransportLiteral = Literal['stdio', 'sse', 'streamable-http']

#: Names of the tools that MUST be registered for the server to start (Req 1.2).
#: ``main()`` verifies the live FastMCP registry contains exactly these before
#: serving; a missing or malformed tool aborts startup naming the offender.
EXPECTED_TOOL_NAMES = (
    'list_events',
    'list_upcoming_events',
    'search_events',
    'search_upcoming_events',
    'get_event_details',
)


class StartupError(RuntimeError):
    """Raised to abort server startup with a descriptive, actionable message.

    Used for the two fatal startup conditions the design calls out: one or more
    expected tools failed to register (Requirement 1.2) and no supported
    transport is configured (Requirement 1.5). Raising (rather than logging and
    returning) keeps ``main()`` testable while still terminating the process with
    a non-zero exit code when it propagates out of ``__main__``.
    """


def _verify_tools_registered(expected_names: tuple[str, ...] = EXPECTED_TOOL_NAMES) -> None:
    """Verify the expected catalog tools are registered with a usable schema.

    Tools register at import time via the ``@mcp.tool()`` decorators, so this
    introspects the live FastMCP tool registry and confirms every expected tool
    is present with a unique name, a non-empty description, and an input schema
    (Requirement 1.1). Any expected tool that is missing, or registered without a
    description or input schema, aborts startup with an error naming that tool
    (Requirement 1.2).

    Args:
        expected_names: The tool names that must be registered. Defaults to
            :data:`EXPECTED_TOOL_NAMES`; overridable so tests can force a
            registration failure.

    Raises:
        StartupError: If an expected tool is missing, duplicated, or lacks a
            non-empty description or an input schema. The message names the
            offending tool(s).
    """
    registered = mcp._tool_manager.list_tools()

    names_seen: Dict[str, int] = {}
    for tool in registered:
        names_seen[tool.name] = names_seen.get(tool.name, 0) + 1

    duplicates = sorted(name for name, count in names_seen.items() if count > 1)
    if duplicates:
        raise StartupError(
            f'Tool registration failed: duplicate tool name(s) registered: '
            f'{", ".join(duplicates)}.'
        )

    by_name = {tool.name: tool for tool in registered}
    for name in expected_names:
        tool = by_name.get(name)
        if tool is None:
            raise StartupError(
                f"Tool registration failed: required tool '{name}' is not registered."
            )
        if not (tool.description and tool.description.strip()):
            raise StartupError(f"Tool registration failed: tool '{name}' has no description.")
        if tool.parameters is None:
            raise StartupError(f"Tool registration failed: tool '{name}' has no input schema.")


def _resolve_transport(configured: Optional[str] = None) -> TransportLiteral:
    """Resolve and validate the transport to serve the registered tools over.

    Honors an explicitly configured transport (the ``configured`` argument, or
    the ``FASTMCP_TRANSPORT`` environment variable when ``configured`` is
    ``None``). When nothing is configured the server defaults to ``stdio``
    (Requirement 1.4). A configured-but-unsupported transport aborts startup with
    an error listing the supported transports (Requirement 1.5).

    Args:
        configured: An explicit transport, or ``None`` to read the environment.
            An empty/whitespace-only value is treated as "not configured".

    Returns:
        The resolved transport, guaranteed to be one of
        :data:`consts.SUPPORTED_TRANSPORTS`.

    Raises:
        StartupError: If a transport is configured but is not supported.
    """
    raw = configured if configured is not None else os.getenv(consts.ENV_TRANSPORT)
    if raw is None or not raw.strip():
        return cast(TransportLiteral, consts.DEFAULT_TRANSPORT)

    candidate = raw.strip()
    if candidate not in consts.SUPPORTED_TRANSPORTS:
        supported = ', '.join(consts.SUPPORTED_TRANSPORTS)
        raise StartupError(
            f"Unsupported transport '{candidate}'. A supported transport is required; "
            f'set {consts.ENV_TRANSPORT} to one of: {supported}.'
        )
    return cast(TransportLiteral, candidate)


def main() -> None:
    """Run the AWS Events MCP Server.

    Verifies the catalog tools are registered (aborting startup naming any
    failed tool, Requirement 1.2), resolves the configured transport (defaulting
    to stdio, or aborting if an unsupported transport is configured,
    Requirements 1.4/1.5), then serves the registered tools over that transport
    via the synchronous ``FastMCP.run`` (which owns the event loop).

    Raises:
        StartupError: If tool registration cannot be verified or the configured
            transport is unsupported. Propagating out of ``__main__`` terminates
            the process with a non-zero exit code.
    """
    _verify_tools_registered()
    transport = _resolve_transport()
    logger.info(f'Starting AWS Events MCP Server (transport={transport}).')
    mcp.run(transport=transport)


# Re-export the not-found helper so the tool layer sources every response shape
# from this module.
__all__ = [
    'EXPECTED_TOOL_NAMES',
    'StartupError',
    'ToolValidationError',
    'build_event_details_response',
    'build_event_query',
    'build_listing_response',
    'build_source_error_response',
    'get_catalog_cache',
    'get_event_details',
    'list_events',
    'list_upcoming_events',
    'main',
    'mcp',
    'not_found',
    'resolve_page_offset',
    'search_events',
    'search_upcoming_events',
    'set_catalog_cache',
    'validation_response',
]


if __name__ == '__main__':
    main()
