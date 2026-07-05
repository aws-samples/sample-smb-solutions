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

import httpx
import json
import re
from aws_events_mcp import consts
from aws_events_mcp.errors import (
    CatalogTimeoutError,
    CatalogUnparseableError,
    CatalogUnreachableError,
)
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
    client: httpx.AsyncClient, url: str, params: Optional[dict[str, Any]] = None
) -> httpx.Response:
    """Issue a GET request and translate transport failures into typed errors.

    Args:
        client: The async HTTP client to use.
        url: The absolute URL to request.
        params: Optional query parameters.

    Returns:
        The successful HTTP response.

    Raises:
        CatalogTimeoutError: The request timed out (Requirement 11.2).
        CatalogUnreachableError: A connection failure, any other transport
            error, or a non-success HTTP status (Requirement 11.1).
    """
    try:
        response = await client.get(url, params=params)
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
