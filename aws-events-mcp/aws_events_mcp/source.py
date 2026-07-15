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

"""Catalog retrieval strategies for the AWS Events MCP Server.

This module hides the upstream AWS Events catalog behind a single
``EventCatalogSource`` interface so the rest of the package depends only on a
flat ``list[dict]`` of raw records. Two strategies implement the interface:

- :class:`JsonApiCatalogSource` (primary) issues async ``httpx`` GET requests
  against the configured public content-directory endpoint, following upstream
  pagination, and returns the raw record dicts.
- :class:`HtmlScrapeCatalogSource` (fallback) fetches the public catalog page
  and extracts record data embedded in the served markup.

Both strategies contact only the configured public endpoint, send a descriptive
``User-Agent`` (NFR Security), use a 30-second total timeout (Requirement 11.2),
and use **no credentials**. Transport failures are translated into the typed
``CatalogSourceError`` hierarchy: a connection failure raises
``CatalogUnreachableError`` (Requirement 11.1), a timeout raises
``CatalogTimeoutError`` (Requirement 11.2), and a response whose body cannot be
decoded into any candidate records raises ``CatalogUnparseableError``
(Requirement 11.3).

Confirmed contract (task 8.2):
    The upstream endpoint, directory identifier, query parameters, and record
    shape were confirmed against the live AWS content-directory API and pinned
    in :mod:`aws_events_mcp.consts`. The JSON API returns
    ``{"items": [{"item": {...}, "tags": [...]}, ...], "metadata": {...}}``;
    :func:`_extract_items` unwraps each ``item`` (and attaches its sibling
    ``tags`` so the parser can derive delivery mode and category) and
    :func:`_extract_total_hits` reads ``metadata.totalHits`` to drive upstream
    pagination. Each strategy returns the raw record dicts unchanged otherwise;
    mapping a raw record to an ``Event`` is the parser's responsibility
    (:mod:`aws_events_mcp.parser`) and is the single place the field mapping is
    adjusted should the contract change.

    The HTML-scrape fallback is best-effort: the public catalog page is
    client-rendered and does **not** server-render the event records (it only
    embeds static directory configuration), so the fallback cannot reconstruct
    the live catalog from the page alone. It is retained behind the same
    interface for resilience and for any locale/fragment that does embed record
    data, and it raises ``CatalogUnparseableError`` when no records are found.
"""

import asyncio
import html
import httpx
import json
import re
from aws_events_mcp import consts
from aws_events_mcp.errors import (
    CatalogTimeoutError,
    CatalogUnparseableError,
    CatalogUnreachableError,
)
from loguru import logger
from typing import Any, Optional, Protocol, runtime_checkable


# Safety cap on upstream page requests so a misbehaving or misconfigured
# endpoint cannot drive an unbounded fetch loop. The catalog holds ~1,300
# events; at the default page size this bounds the loop generously.
_MAX_UPSTREAM_PAGES = 50

# Default public catalog page used by the HTML-scrape fallback. Provisional and
# overridable via the same configuration surface as the JSON endpoint; this is a
# public AWS page contacted without credentials (NFR Security).
DEFAULT_CATALOG_PAGE_URL = 'https://aws.amazon.com/events/explore-aws-events/'

# Matches the contents of <script> blocks, where client-rendered pages of this
# style embed their backing data as JSON.
_SCRIPT_RE = re.compile(r'<script[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)

# Extracts the short-lived guest bearer token embedded in the Builder Loft
# calendar HTML shell's ``applicationSettings`` block, e.g.
# ``authorization: '9f3c...<hex>'``.
_BUILDER_LOFT_TOKEN_RE = re.compile(r"authorization:\s*'([a-f0-9]{32,})'", re.IGNORECASE)

# Strips HTML tags from a Builder Loft event description to recover plain text.
_HTML_TAG_RE = re.compile(r'<[^>]+>')


@runtime_checkable
class EventCatalogSource(Protocol):
    """Interface for a strategy that retrieves raw event records upstream.

    Implementations isolate all network and upstream-format concerns; callers
    depend only on the returned ``list[dict]`` and the typed error contract.
    """

    async def fetch_raw_records(self) -> list[dict]:
        """Retrieve raw event records from the upstream catalog.

        Returns:
            A flat list of raw record dicts. An empty list denotes a reachable,
            decodable, but empty catalog and is not an error.

        Raises:
            CatalogUnreachableError: The source could not be reached.
            CatalogTimeoutError: The request exceeded the 30 second limit.
            CatalogUnparseableError: The response body could not be decoded into
                any candidate records at all.
        """
        ...


async def _request(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
) -> httpx.Response:
    """Issue a GET request and translate transport failures into typed errors.

    Args:
        client: The async HTTP client to use.
        url: The absolute URL to request.
        params: Optional query parameters.
        headers: Optional per-request headers merged over the client's defaults.

    Returns:
        The successful HTTP response.

    Raises:
        CatalogTimeoutError: The request timed out (Requirement 11.2).
        CatalogUnreachableError: A connection failure, any other transport
            error, or a non-success HTTP status (Requirement 11.1).
    """
    try:
        response = await client.get(url, params=params, headers=headers)
    except httpx.TimeoutException as exc:
        # Checked before ConnectError: httpx.ConnectTimeout subclasses both, and
        # a timeout must surface as CatalogTimeoutError (Requirement 11.2).
        raise CatalogTimeoutError(
            f'The AWS Events catalog request exceeded the {consts.REQUEST_TIMEOUT_SECONDS:.0f} '
            'second limit.'
        ) from exc
    except httpx.ConnectError as exc:
        raise CatalogUnreachableError(
            'The AWS Events catalog is unreachable: connection failed.'
        ) from exc
    except httpx.RequestError as exc:
        # Any other transport-level failure (read/write/protocol) also means the
        # request could not complete; treat it as unreachable (Requirement 11.1).
        raise CatalogUnreachableError(
            'The AWS Events catalog is unreachable: the request could not complete.'
        ) from exc

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise CatalogUnreachableError(
            f'The AWS Events catalog returned an error status {response.status_code}.'
        ) from exc
    return response


def _extract_items(payload: Any) -> Optional[list[dict]]:
    """Extract and unwrap the candidate record list from a decoded JSON payload.

    The confirmed content-directory contract (task 8.2) returns records under an
    ``items`` array whose entries are wrappers ``{"item": {...}, "tags": [...]}``.
    Each wrapper is unwrapped to its inner ``item`` dict, and the sibling
    ``tags`` list is attached onto that dict (under the ``tags`` key) so the
    parser can derive delivery mode and category from the catalog's tag
    namespaces. A bare array of dicts and already-unwrapped item dicts are also
    accepted defensively.

    Args:
        payload: The decoded JSON value from an upstream response.

    Returns:
        The list of unwrapped record dicts (possibly empty) when a recognizable
        items container is present, or ``None`` when no container can be
        located. A ``None`` result signals an undecodable contract; an empty
        list signals a reachable but empty page.
    """
    if isinstance(payload, list):
        container: Optional[list] = payload
    elif isinstance(payload, dict):
        items = payload.get('items')
        container = items if isinstance(items, list) else None
    else:
        container = None

    if container is None:
        return None

    records: list[dict] = []
    for entry in container:
        if not isinstance(entry, dict):
            continue
        inner = entry.get('item')
        if isinstance(inner, dict):
            record = dict(inner)
            # Attach the wrapper's sibling tags so the parser can read the
            # GLOBAL#aws-event-type / content-type namespaces (delivery mode,
            # category). The inner item carries no tags of its own.
            if 'tags' not in record:
                record['tags'] = entry.get('tags', [])
            records.append(record)
        else:
            # Defensive: a bare record dict with no wrapper.
            records.append(entry)
    return records


def _extract_total_hits(payload: Any) -> Optional[int]:
    """Read the total match count from an upstream payload's metadata.

    Used only to decide when upstream pagination is exhausted. The confirmed
    contract (task 8.2) advertises ``metadata.totalHits``; ``metadata.total`` is
    accepted as a defensive alternative. ``metadata.count`` is deliberately
    ignored because it reports the current page's size, not the full total.

    Args:
        payload: The decoded JSON value from an upstream response.

    Returns:
        The reported total number of matching records, or ``None`` when the
        payload does not advertise one.
    """
    if isinstance(payload, dict):
        metadata = payload.get('metadata')
        if isinstance(metadata, dict):
            for key in ('totalHits', 'total'):
                value = metadata.get(key)
                if isinstance(value, int) and not isinstance(value, bool):
                    return value
    return None


def _iter_json_candidates(block: str):
    """Yield substrings of a script block that might parse as JSON.

    Best-effort: yields the whole stripped block plus the widest array- or
    object-delimited slice, so embedded data assigned to a variable can still be
    recovered. Provisional extraction (task 8.2).

    Args:
        block: The raw text contents of a single ``<script>`` element.

    Yields:
        Candidate JSON strings to attempt to decode.
    """
    text = block.strip()
    if not text:
        return
    yield text
    for open_ch, close_ch in (('[', ']'), ('{', '}')):
        start = text.find(open_ch)
        end = text.rfind(close_ch)
        if 0 <= start < end:
            yield text[start : end + 1]


def _largest_dict_list(parsed: Any) -> list[dict]:
    """Find the largest homogeneous list of dicts within a decoded structure.

    Walks the structure breadth-first and returns the longest list whose every
    element is a dict, treating that as the most likely record collection.
    Provisional heuristic (task 8.2).

    Args:
        parsed: A decoded JSON value.

    Returns:
        The largest list of dicts found, or an empty list when none is present.
    """
    best: list[dict] = []
    stack: list[Any] = [parsed]
    while stack:
        current = stack.pop()
        if isinstance(current, list):
            dicts = [element for element in current if isinstance(element, dict)]
            if dicts and len(dicts) == len(current) and len(dicts) > len(best):
                best = dicts
            for element in current:
                if isinstance(element, (list, dict)):
                    stack.append(element)
        elif isinstance(current, dict):
            for value in current.values():
                if isinstance(value, (list, dict)):
                    stack.append(value)
    return best


def _extract_embedded_records(html: str) -> list[dict]:
    """Extract embedded record dicts from a client-rendered catalog page.

    Scans every ``<script>`` block for JSON and returns the largest list of
    record-like dicts found. Provisional extraction (task 8.2).

    Args:
        html: The full HTML body of the catalog page.

    Returns:
        The recovered record dicts, or an empty list when none are found.
    """
    best: list[dict] = []
    for block in _SCRIPT_RE.findall(html):
        for candidate in _iter_json_candidates(block):
            try:
                parsed = json.loads(candidate)
            except ValueError:
                continue
            records = _largest_dict_list(parsed)
            if len(records) > len(best):
                best = records
    return best


class JsonApiCatalogSource:
    """Primary strategy: fetch records from the content-directory JSON API.

    Issues async ``httpx`` GET requests against the configured public endpoint,
    following upstream pagination until the reported total is reached or a page
    returns no records, and returns the accumulated raw record dicts. Sends a
    descriptive ``User-Agent`` and a 30-second total timeout, and uses no
    credentials.

    The endpoint URL, directory identifier, and query parameters default to the
    confirmed values in :mod:`aws_events_mcp.consts` (pinned in task 8.2); they
    may also be supplied explicitly (primarily for testing) and remain
    overridable via environment variables.
    """

    def __init__(
        self,
        *,
        endpoint_url: str = consts.CATALOG_ENDPOINT_URL,
        directory_id: str = consts.CATALOG_DIRECTORY_ID,
        query_params: Optional[dict[str, str]] = None,
        tag_exclusions: Optional[tuple[str, ...]] = None,
        timeout_seconds: float = consts.REQUEST_TIMEOUT_SECONDS,
        user_agent: str = consts.USER_AGENT,
    ) -> None:
        """Initialize the JSON-API catalog source.

        Args:
            endpoint_url: Content-directory endpoint URL.
            directory_id: Content-directory identifier for the AWS Events
                catalog, sent as a query parameter.
            query_params: Base upstream query parameters; defaults to
                ``consts.CATALOG_QUERY_PARAMS``.
            tag_exclusions: ``tags.id`` exclusion filters sent as repeated
                ``tags.id=!<tag>`` query parameters, matching the live catalog
                page (drops third-party/archived records). Defaults to
                ``consts.CATALOG_TAG_EXCLUSIONS``.
            timeout_seconds: Total request timeout in seconds (Requirement 11.2).
            user_agent: Descriptive ``User-Agent`` header value (NFR Security).
        """
        self._endpoint_url = endpoint_url
        self._directory_id = directory_id
        self._query_params = dict(
            query_params if query_params is not None else consts.CATALOG_QUERY_PARAMS
        )
        self._tag_exclusions = tuple(
            tag_exclusions if tag_exclusions is not None else consts.CATALOG_TAG_EXCLUSIONS
        )
        self._timeout = httpx.Timeout(timeout_seconds)
        self._user_agent = user_agent

    def _build_params(self, page: int) -> dict[str, Any]:
        """Build the query parameters for a single upstream page request.

        Adds the confirmed ``item.directoryId`` (the events alias), the
        zero-based ``page`` index, and the ``tags.id`` exclusion filters (as a
        list value, which httpx serializes to repeated ``tags.id`` parameters)
        to the configured base parameters (which carry ``item.locale``, ``size``,
        and the ``sort_by``/``sort_order`` ordering). These match the live
        request issued by the public catalog page (task 8.2, re-verified).

        Args:
            page: Zero-based page index.

        Returns:
            The merged query parameters for the request. The ``tags.id`` value is
            a list of exclusion filters when any are configured.
        """
        params: dict[str, Any] = dict(self._query_params)
        params['item.directoryId'] = self._directory_id
        params['page'] = str(page)
        if self._tag_exclusions:
            params['tags.id'] = list(self._tag_exclusions)
        return params

    async def _get_json(self, client: httpx.AsyncClient, params: dict[str, Any]) -> Any:
        """Request one upstream page and decode its JSON body.

        Args:
            client: The async HTTP client to use.
            params: Query parameters for the request.

        Returns:
            The decoded JSON value.

        Raises:
            CatalogUnparseableError: The response body is not valid JSON.
            CatalogTimeoutError: The request timed out.
            CatalogUnreachableError: The source could not be reached.
        """
        response = await _request(client, self._endpoint_url, params)
        try:
            return response.json()
        except ValueError as exc:
            raise CatalogUnparseableError(
                'The AWS Events catalog response could not be interpreted as JSON.'
            ) from exc

    async def fetch_raw_records(self) -> list[dict]:
        """Fetch all raw records from the JSON API, following pagination.

        Returns:
            A flat list of raw record dicts across all upstream pages. An empty
            list denotes a reachable, decodable, but empty catalog.

        Raises:
            CatalogUnreachableError: The source could not be reached.
            CatalogTimeoutError: The request exceeded the 30 second limit.
            CatalogUnparseableError: The first page could not be decoded into any
                candidate records (Requirement 11.3).
        """
        records: list[dict] = []
        headers = {'User-Agent': self._user_agent, 'Accept': 'application/json'}
        async with httpx.AsyncClient(timeout=self._timeout, headers=headers) as client:
            total_hits: Optional[int] = None
            for page in range(_MAX_UPSTREAM_PAGES):
                payload = await self._get_json(client, self._build_params(page))
                items = _extract_items(payload)
                if items is None:
                    if page == 0:
                        raise CatalogUnparseableError(
                            'The AWS Events catalog response contained no recognizable records.'
                        )
                    break
                records.extend(items)
                if total_hits is None:
                    total_hits = _extract_total_hits(payload)
                if not items:
                    break
                if total_hits is not None and len(records) >= total_hits:
                    break
        return records


class HtmlScrapeCatalogSource:
    """Fallback strategy: scrape records embedded in the catalog page markup.

    Used only when the JSON API is unavailable. Fetches the public catalog page
    and extracts the largest list of record-like dicts embedded in its
    ``<script>`` blocks. Sends a descriptive ``User-Agent`` and a 30-second total
    timeout, and uses no credentials.

    Limitation (confirmed in task 8.2): the live catalog page is client-rendered
    and does **not** server-render the event records; its scripts embed only
    static directory configuration (field mappings, level legends), so against
    the current page this fallback recovers no real events and raises
    ``CatalogUnparseableError``. It is retained behind the same interface for
    resilience and for any fragment/locale that does embed record data.
    """

    def __init__(
        self,
        *,
        page_url: str = DEFAULT_CATALOG_PAGE_URL,
        timeout_seconds: float = consts.REQUEST_TIMEOUT_SECONDS,
        user_agent: str = consts.USER_AGENT,
    ) -> None:
        """Initialize the HTML-scrape catalog source.

        Args:
            page_url: Public catalog page URL to fetch.
            timeout_seconds: Total request timeout in seconds (Requirement 11.2).
            user_agent: Descriptive ``User-Agent`` header value (NFR Security).
        """
        self._page_url = page_url
        self._timeout = httpx.Timeout(timeout_seconds)
        self._user_agent = user_agent

    async def fetch_raw_records(self) -> list[dict]:
        """Fetch the catalog page and extract embedded raw records.

        Returns:
            A flat list of raw record dicts recovered from the page markup.

        Raises:
            CatalogUnreachableError: The source could not be reached.
            CatalogTimeoutError: The request exceeded the 30 second limit.
            CatalogUnparseableError: No candidate records could be extracted from
                the page (Requirement 11.3).
        """
        headers = {'User-Agent': self._user_agent, 'Accept': 'text/html'}
        async with httpx.AsyncClient(timeout=self._timeout, headers=headers) as client:
            response = await _request(client, self._page_url)
            html = response.text
        records = _extract_embedded_records(html)
        if not records:
            raise CatalogUnparseableError(
                'The AWS Events catalog page contained no recognizable records.'
            )
        return records


def _strip_html(value: Any) -> str:
    """Reduce an HTML fragment to unescaped plain text.

    Builder Loft event descriptions are HTML fragments; the parser expects a
    plain-text description. Tags are removed, HTML entities are unescaped, and
    surrounding whitespace is collapsed. Non-string input yields an empty string.

    Args:
        value: The raw description value (expected to be an HTML string).

    Returns:
        The plain-text description, or an empty string when ``value`` is not a
        string.
    """
    if not isinstance(value, str):
        return ''
    without_tags = _HTML_TAG_RE.sub(' ', value)
    unescaped = html.unescape(without_tags)
    return ' '.join(unescaped.split())


def _map_builder_loft_event(base_url: str, calendar_id: str, event: Any) -> Optional[dict]:
    """Map a single Builder Loft event object into a flat parser-ready record.

    Produces a flat dict whose keys the existing parser (:func:`_merged_fields`,
    ``_FIELD_KEYS``, ``_derive_location_mode``) already understands, so no parser
    change is needed. The Cvent ``startDate`` is an unzoned ``YYYY-MM-DDTHH:MM``
    string; its date portion maps to the required ``start_date`` and the time
    portion to ``start_time``. The Cvent ``timeZone`` is a resource placeholder
    (not an IANA/label zone) and is deliberately omitted. Delivery mode is set
    explicitly to ``physical`` (Builder Loft is an in-person venue).

    Args:
        base_url: The Builder Loft base URL, used to build the calendar deep link.
        calendar_id: The Cvent calendar identifier, used in the deep link.
        event: A single Builder Loft event object (expected to be a mapping).

    Returns:
        A flat record dict for the parser, or ``None`` when ``event`` is not a
        mapping or carries no usable identifier.
    """
    if not isinstance(event, dict):
        return None
    event_id = event.get('id')
    if not isinstance(event_id, str) or not event_id.strip():
        return None

    record: dict[str, Any] = {'id': f'builder-loft#{event_id}'}

    title = event.get('title')
    if isinstance(title, str):
        record['title'] = title

    description = _strip_html(event.get('description'))
    if description:
        record['description'] = description

    start_date = event.get('startDate')
    if isinstance(start_date, str) and start_date.strip():
        date_part, _, time_part = start_date.partition('T')
        record['date'] = date_part.strip()
        if time_part.strip():
            record['time'] = time_part.strip()

    location = event.get('location')
    if isinstance(location, str):
        record['location'] = location

    # Builder Loft is an in-person venue; honor an explicit delivery mode so the
    # parser does not have to infer it from free text.
    record['location_mode'] = 'physical'

    # No per-event public URL exists; deep-link to the calendar itself.
    record['learn_more_url'] = f'{base_url}/c/calendar/{calendar_id}'

    return record


#: Matches the leading ``HH:MM`` of a Connected Community localized timestamp
#: such as ``2026-07-07T12:00-04:00`` (the offset/seconds are discarded).
_CC_TIME_RE = re.compile(r'^(\d{2}:\d{2})')

#: Connected Community ``settingDetails[].setting`` values denoting in-person.
_CC_PHYSICAL_SETTINGS = frozenset({'in-person', 'in person', 'inperson', 'physical', 'venue'})

#: Human-readable names for the Connected Community ``type`` facet. Unlisted
#: values (e.g. the generic ``otherawsevent``) leave the event type unset.
_CC_EVENT_TYPES: dict[str, str] = {
    'handsonworkshop': 'Hands-on Workshop',
    'technicaltalk': 'Tech Talk',
    'businesstalk': 'Business Talk',
    'meetup': 'Meetup',
}


def _cc_location_from_settings(settings: Any) -> Optional[str]:
    """Extract a free-text location from Connected Community setting details.

    Physical session records carry venue information under
    ``settingDetails[].details`` (an ``address`` string and/or a
    ``location: {"id": <city>}`` mapping). The street address is preferred; the
    city identifier is the fallback.

    Args:
        settings: The record's ``settingDetails`` list.

    Returns:
        The extracted location text, or ``None`` when none is present.
    """
    if not isinstance(settings, list):
        return None
    for setting in settings:
        if not isinstance(setting, dict):
            continue
        details = setting.get('details')
        if not isinstance(details, dict):
            continue
        address = details.get('address')
        if isinstance(address, str) and address.strip():
            return address.strip()
        location = details.get('location')
        if isinstance(location, dict):
            city = location.get('id')
            if isinstance(city, str) and city.strip():
                return city.strip().replace('-', ' ').title()
    return None


def _map_connected_community_event(
    event: Any, *, page_base: Optional[str] = None
) -> Optional[dict]:
    """Map a single Connected Community event into a flat parser-ready record.

    Produces a flat dict whose keys the existing parser (:func:`_merged_fields`,
    ``_FIELD_KEYS``) already understands, so no parser change is needed. Both
    Connected Community feeds (``externalevent`` and ``session``) return flat
    event objects with an unzoned ``startDate`` (``YYYY-MM-DD``), a localized
    ``startWithTimeZone`` (``YYYY-MM-DDTHH:MM±HH:MM``), an IANA ``timeZone``, a
    ``settingDetails`` list carrying the delivery mode (and, for physical
    sessions, the venue), and a numeric ``levels`` list. Delivery mode is set
    explicitly from ``settingDetails`` (defaulting to virtual, which the hub is
    overwhelmingly composed of), the lowest numeric level is passed through as
    the learning level, and the ``type`` facet (e.g. ``handsonworkshop``) maps
    to a readable event type. Links prefer an explicit ``registrationUrl``
    (``externalevent`` records); ``session`` records carry a ``urlSlug``
    instead, which is resolved against ``page_base`` into the event page URL.

    Args:
        event: A single Connected Community event object (expected to be a
            mapping).
        page_base: The segment page base (e.g.
            ``https://aws-experience.com/amer/smb``) used to resolve ``urlSlug``
            links; when ``None``, slug-only records yield no link.

    Returns:
        A flat record dict for the parser, or ``None`` when ``event`` is not a
        mapping or carries no usable identifier.
    """
    if not isinstance(event, dict):
        return None
    event_id = event.get('id')
    if not isinstance(event_id, str) or not event_id.strip():
        return None

    record: dict[str, Any] = {'id': f'connected-community#{event_id.strip()}'}

    title = event.get('title')
    if isinstance(title, str):
        record['title'] = title

    # Combine the plain-text ``summary`` (leading) with the stripped HTML
    # ``description`` so keyword search covers both, matching the site's own
    # search behavior (which scans the full description).
    summary = event.get('summary')
    summary_text = summary.strip() if isinstance(summary, str) else ''
    description_text = _strip_html(event.get('description'))
    if summary_text and description_text and description_text != summary_text:
        record['description'] = f'{summary_text} {description_text}'
    elif summary_text:
        record['description'] = summary_text
    elif description_text:
        record['description'] = description_text

    start_date = event.get('startDate')
    if isinstance(start_date, str) and start_date.strip():
        record['date'] = start_date.strip()

    # Recover a local start time from the localized timestamp (drop the offset).
    start_with_tz = event.get('startWithTimeZone')
    if isinstance(start_with_tz, str) and 'T' in start_with_tz:
        match = _CC_TIME_RE.match(start_with_tz.split('T', 1)[1])
        if match is not None:
            record['time'] = match.group(1)

    time_zone = event.get('timeZone')
    if isinstance(time_zone, str) and time_zone.strip():
        record['timezone'] = time_zone.strip()

    # Delivery mode: honor an explicit in-person setting, else default virtual.
    mode = 'virtual'
    settings = event.get('settingDetails')
    if isinstance(settings, list):
        for setting in settings:
            if isinstance(setting, dict):
                name = setting.get('setting')
                if isinstance(name, str) and name.strip().lower() in _CC_PHYSICAL_SETTINGS:
                    mode = 'physical'
                    break
    record['location_mode'] = mode

    # Physical sessions carry a venue address / city under settingDetails.
    if mode == 'physical':
        location = _cc_location_from_settings(settings)
        if location is not None:
            record['location'] = location

    # The type facet (e.g. handsonworkshop) maps to a readable event type.
    event_type = event.get('type')
    if isinstance(event_type, str):
        readable = _CC_EVENT_TYPES.get(event_type.strip().lower())
        if readable is not None:
            record['event_type'] = readable

    # Learning level: pass through the lowest numeric level (100/200/300/400),
    # which the parser normalizes to the canonical LearningLevel.
    levels = event.get('levels')
    if isinstance(levels, list):
        numeric = [lv for lv in levels if isinstance(lv, str) and lv.strip().isdigit()]
        if numeric:
            record['level'] = min(numeric, key=lambda value: int(value))

    registration_url = event.get('registrationUrl')
    if isinstance(registration_url, str) and registration_url.strip():
        record['registration_url'] = registration_url.strip()
        record['learn_more_url'] = registration_url.strip()
    elif page_base is not None:
        # Session records carry a urlSlug instead of an explicit registration
        # URL; resolve it into the segment's event page deep link.
        url_slug = event.get('urlSlug')
        if isinstance(url_slug, str) and url_slug.strip():
            link = f'{page_base}/e/{url_slug.strip().lstrip("/")}'
            record['registration_url'] = link
            record['learn_more_url'] = link

    return record


class ConnectedCommunityCatalogSource:
    """Source strategy: the AWS Connected Community events hub (aws-experience.com).

    Retrieves records from plain, credential-free JSON APIs under the segment's
    ``api`` prefix. Two feeds are fetched concurrently and concatenated:

    - ``externalevent``: cross-posted events with explicit registration URLs;
    - ``session``: the segment's own workshops/talks/meetups (the much larger
      feed, and the one the site's search page draws from), whose ``urlSlug``
      resolves to the event page under ``<base>/<segment>/e/<slug>``.

    Each feed returns ``{"future": [...], "past": [...]}`` where every entry is
    a flat event object. Both buckets of both feeds are mapped into flat records
    the existing parser already understands (see
    :func:`_map_connected_community_event`), so no parser change is needed.
    Sends a descriptive ``User-Agent`` and a 30-second total timeout, and uses
    no credentials.

    Per-feed degradation mirrors the composite philosophy: if one feed fails but
    the other succeeds, a warning is logged and the successful feed's records
    are returned; only when every feed fails is the first error re-raised.
    Transport failures follow the shared typed-error contract: a connection
    failure raises ``CatalogUnreachableError`` (Requirement 11.1), a timeout
    raises ``CatalogTimeoutError`` (Requirement 11.2), and a body that is not
    valid JSON or lacks both the ``future`` and ``past`` buckets raises
    ``CatalogUnparseableError`` (Requirement 11.3).
    """

    #: The segment API feeds fetched and unioned, in concatenation order.
    _FEEDS: tuple[str, ...] = ('externalevent', 'session')

    def __init__(
        self,
        *,
        base_url: str = consts.CONNECTED_COMMUNITY_BASE_URL,
        segment_path: str = consts.CONNECTED_COMMUNITY_SEGMENT_PATH,
        timeout_seconds: float = consts.REQUEST_TIMEOUT_SECONDS,
        user_agent: str = consts.USER_AGENT,
    ) -> None:
        """Initialize the Connected Community catalog source.

        Args:
            base_url: Connected Community base URL (e.g.
                ``https://aws-experience.com``).
            segment_path: Region/segment path (e.g. ``amer/smb``).
            timeout_seconds: Total request timeout in seconds (Requirement 11.2).
            user_agent: Descriptive ``User-Agent`` header value (NFR Security).
        """
        self._base_url = base_url.rstrip('/')
        self._segment_path = segment_path.strip('/')
        self._timeout = httpx.Timeout(timeout_seconds)
        self._user_agent = user_agent

    @property
    def _page_base(self) -> str:
        """The segment page base used to resolve ``urlSlug`` event links."""
        return f'{self._base_url}/{self._segment_path}'

    def _feed_url(self, feed: str) -> str:
        """Build the JSON endpoint URL for a segment API feed.

        Args:
            feed: The feed name (e.g. ``externalevent`` or ``session``).

        Returns:
            The absolute feed URL.
        """
        return f'{self._base_url}/{self._segment_path}/api/{feed}'

    async def _fetch_feed(self, client: httpx.AsyncClient, feed: str) -> list[dict]:
        """Fetch one segment feed and map its buckets into flat records.

        Args:
            client: The async HTTP client to use.
            feed: The feed name to fetch.

        Returns:
            The mapped records from the feed's ``future`` and ``past`` buckets.

        Raises:
            CatalogUnreachableError: The feed could not be reached (Req 11.1).
            CatalogTimeoutError: The request exceeded the limit (Req 11.2).
            CatalogUnparseableError: The body was not decodable JSON, or carried
                neither a ``future`` nor a ``past`` bucket (Requirement 11.3).
        """
        response = await _request(client, self._feed_url(feed))
        try:
            payload = response.json()
        except ValueError as exc:
            raise CatalogUnparseableError(
                f'The AWS Connected Community {feed} response could not be interpreted as JSON.'
            ) from exc

        if not isinstance(payload, dict) or ('future' not in payload and 'past' not in payload):
            raise CatalogUnparseableError(
                f'The AWS Connected Community {feed} response contained no '
                'recognizable event buckets.'
            )

        records: list[dict] = []
        for bucket in ('future', 'past'):
            items = payload.get(bucket)
            if isinstance(items, list):
                for event in items:
                    mapped = _map_connected_community_event(event, page_base=self._page_base)
                    if mapped is not None:
                        records.append(mapped)
        return records

    async def fetch_raw_records(self) -> list[dict]:
        """Fetch the Connected Community events as flat parser-ready records.

        Fetches the ``externalevent`` and ``session`` feeds concurrently and
        concatenates their mapped records (each feed's ``future`` bucket before
        its ``past`` bucket). If some feeds fail while others succeed, a warning
        is logged and the successful records are returned.

        Returns:
            A flat list of mapped record dicts across all feeds. An empty list
            denotes a reachable hub with no events.

        Raises:
            CatalogUnreachableError: Every feed was unreachable (Req 11.1).
            CatalogTimeoutError: Every feed timed out first (Req 11.2).
            CatalogUnparseableError: Every feed failed and the first failure was
                an undecodable body (Requirement 11.3).
        """
        headers = {'User-Agent': self._user_agent, 'Accept': 'application/json'}
        async with httpx.AsyncClient(timeout=self._timeout, headers=headers) as client:
            results = await asyncio.gather(
                *(self._fetch_feed(client, feed) for feed in self._FEEDS),
                return_exceptions=True,
            )

        records: list[dict] = []
        failures: list[tuple[str, BaseException]] = []
        for feed, result in zip(self._FEEDS, results):
            if isinstance(result, BaseException):
                failures.append((feed, result))
            else:
                records.extend(result)

        if failures and len(failures) == len(self._FEEDS):
            raise failures[0][1]

        if failures:
            failed = ', '.join(feed for feed, _ in failures)
            logger.warning(
                f'AWS Connected Community: {len(failures)} of {len(self._FEEDS)} feed(s) '
                f'failed ({failed}); returning records from the remaining feed(s).'
            )

        return records


class BuilderLoftCatalogSource:
    """Source strategy: the AWS Builder Loft calendar (Cvent-backed).

    Retrieves records via a two-step guest-token flow that requires no
    credentials:

    1. GET the calendar HTML shell with a browser-like ``User-Agent`` and scrape
       the short-lived bearer token from its ``applicationSettings`` block.
    2. GET the calendar ``props`` JSON endpoint with an ``Authorization: BEARER
       <token>`` header and read the events out of ``calendar.events``.

    Each Builder Loft event is mapped into a flat record the existing parser
    already understands (see :func:`_map_builder_loft_event`). The props endpoint
    returns only the default upcoming set (~20 events); further pagination is a
    documented limitation and is intentionally not attempted.

    Transport failures follow the shared typed-error contract: a connection
    failure raises ``CatalogUnreachableError`` (Requirement 11.1), a timeout
    raises ``CatalogTimeoutError`` (Requirement 11.2), and a missing token or an
    undecodable / shape-invalid props body raises ``CatalogUnparseableError``
    (Requirement 11.3).
    """

    def __init__(
        self,
        *,
        base_url: str = consts.BUILDER_LOFT_BASE_URL,
        calendar_id: str = consts.BUILDER_LOFT_CALENDAR_ID,
        timeout_seconds: float = consts.REQUEST_TIMEOUT_SECONDS,
        user_agent: str = consts.USER_AGENT,
        browser_user_agent: str = consts.DEFAULT_BUILDER_LOFT_BROWSER_USER_AGENT,
    ) -> None:
        """Initialize the Builder Loft catalog source.

        Args:
            base_url: Builder Loft events base URL.
            calendar_id: Cvent calendar identifier.
            timeout_seconds: Total request timeout in seconds (Requirement 11.2).
            user_agent: Descriptive ``User-Agent`` for the JSON props call
                (NFR Security).
            browser_user_agent: Browser-like ``User-Agent`` for the HTML shell
                fetch so the guest token page is not blocked.
        """
        self._base_url = base_url.rstrip('/')
        self._calendar_id = calendar_id
        self._timeout = httpx.Timeout(timeout_seconds)
        self._user_agent = user_agent
        self._browser_user_agent = browser_user_agent

    @property
    def _calendar_url(self) -> str:
        """The calendar HTML shell URL carrying the embedded guest token."""
        return f'{self._base_url}/c/calendar/{self._calendar_id}'

    @property
    def _props_url(self) -> str:
        """The calendar props JSON endpoint URL."""
        return (
            f'{self._base_url}/api/calendar_site_editor/v1/{self._calendar_id}/props?latest=false'
        )

    async def _fetch_token(self, client: httpx.AsyncClient) -> str:
        """Fetch the HTML shell and extract the guest bearer token.

        Args:
            client: The async HTTP client to use.

        Returns:
            The extracted guest token string.

        Raises:
            CatalogUnparseableError: The shell contained no extractable token.
            CatalogTimeoutError: The request timed out.
            CatalogUnreachableError: The source could not be reached.
        """
        response = await _request(client, self._calendar_url)
        match = _BUILDER_LOFT_TOKEN_RE.search(response.text)
        if match is None:
            raise CatalogUnparseableError(
                'The AWS Builder Loft calendar page did not contain a guest access token.'
            )
        return match.group(1)

    async def _fetch_props(self, client: httpx.AsyncClient, token: str) -> Any:
        """Fetch and decode the calendar props JSON with the bearer token.

        Args:
            client: The async HTTP client to use.
            token: The guest bearer token scraped from the HTML shell.

        Returns:
            The decoded JSON value.

        Raises:
            CatalogUnparseableError: The props body is not valid JSON.
            CatalogTimeoutError: The request timed out.
            CatalogUnreachableError: The source could not be reached.
        """
        # The endpoint expects the literal, uppercase word BEARER in the header.
        headers = {'Authorization': f'BEARER {token}'}
        response = await _request(client, self._props_url, headers=headers)
        try:
            return response.json()
        except ValueError as exc:
            raise CatalogUnparseableError(
                'The AWS Builder Loft props response could not be interpreted as JSON.'
            ) from exc

    async def fetch_raw_records(self) -> list[dict]:
        """Fetch the Builder Loft calendar events as flat parser-ready records.

        Returns:
            A flat list of mapped record dicts (the default upcoming set, ~20).
            An empty list denotes a reachable calendar with no events.

        Raises:
            CatalogUnreachableError: The source could not be reached (Req 11.1).
            CatalogTimeoutError: A request exceeded the 30 second limit (Req 11.2).
            CatalogUnparseableError: The token could not be extracted, or the
                props body was not decodable / lacked ``calendar.events``
                (Requirement 11.3).
        """
        html_headers = {'User-Agent': self._browser_user_agent, 'Accept': 'text/html'}
        json_headers = {'User-Agent': self._user_agent, 'Accept': 'application/json'}

        async with httpx.AsyncClient(timeout=self._timeout, headers=html_headers) as client:
            token = await self._fetch_token(client)

        async with httpx.AsyncClient(timeout=self._timeout, headers=json_headers) as client:
            payload = await self._fetch_props(client, token)

        calendar = payload.get('calendar') if isinstance(payload, dict) else None
        events = calendar.get('events') if isinstance(calendar, dict) else None
        if not isinstance(events, dict):
            raise CatalogUnparseableError(
                'The AWS Builder Loft props response contained no calendar events.'
            )

        records: list[dict] = []
        for event in events.values():
            mapped = _map_builder_loft_event(self._base_url, self._calendar_id, event)
            if mapped is not None:
                records.append(mapped)
        return records


def _dedup_records(records: list[dict]) -> list[dict]:
    """Drop records with a duplicate raw ``id``, keeping the first occurrence.

    Union sources can overlap; a stable de-dup on the raw record identifier
    keeps event ids unique across sources while preserving input order. Records
    without a usable ``id`` are passed through unchanged (the parser skips
    genuinely unusable records).

    Args:
        records: The concatenated raw records from all sources.

    Returns:
        The records with duplicate ids removed, in first-seen order.
    """
    seen: set[str] = set()
    deduped: list[dict] = []
    for record in records:
        identifier = record.get('id') if isinstance(record, dict) else None
        if isinstance(identifier, str):
            if identifier in seen:
                continue
            seen.add(identifier)
        deduped.append(record)
    return deduped


class CompositeCatalogSource:
    """Union strategy: concatenate raw records from several sources concurrently.

    Runs each sub-source's ``fetch_raw_records`` concurrently and concatenates
    the raw records of every source that succeeded, de-duplicating overlapping
    record ids (first occurrence wins). Failures are handled for graceful
    degradation, consistent with the lenient partial-parse philosophy:

    - if **all** sources fail, the first error is re-raised so a total outage
      still surfaces as a typed source error;
    - if **some** sources fail, a warning naming the failed source(s) is logged
      and the successful records are returned.

    Each sub-source is paired with a short label used only in the warning.
    """

    def __init__(self, sources: list[tuple[str, EventCatalogSource]]) -> None:
        """Initialize the composite source.

        Args:
            sources: An ordered list of ``(label, source)`` pairs. The label is a
                short human-readable name used only for degradation warnings; the
                order determines record concatenation and de-dup precedence.
        """
        self._sources = list(sources)

    async def fetch_raw_records(self) -> list[dict]:
        """Fetch and concatenate raw records from all sub-sources concurrently.

        Returns:
            The de-duplicated concatenation of the raw records from every
            sub-source that succeeded, in source order.

        Raises:
            CatalogSourceError: When every sub-source failed; the first
                encountered error is re-raised.
        """
        results = await asyncio.gather(
            *(source.fetch_raw_records() for _, source in self._sources),
            return_exceptions=True,
        )

        records: list[dict] = []
        failures: list[tuple[str, BaseException]] = []
        for (label, _), result in zip(self._sources, results):
            if isinstance(result, BaseException):
                failures.append((label, result))
            else:
                records.extend(result)

        if failures and len(failures) == len(self._sources):
            # Total outage: re-raise the first error so the tool layer maps it
            # onto the matching source_* response.
            raise failures[0][1]

        if failures:
            failed_labels = ', '.join(label for label, _ in failures)
            logger.warning(
                f'AWS Events catalog: {len(failures)} of {len(self._sources)} source(s) '
                f'failed ({failed_labels}); returning records from the remaining source(s).'
            )

        return _dedup_records(records)
