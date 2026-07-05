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

"""In-memory catalog cache with TTL and single-flight refresh.

This module orchestrates the retrieval and parsing layers behind a single
``CatalogCache`` that the tool layer depends on. ``CatalogCache.get_events``
fetches raw records from an :class:`~aws_events_mcp.source.EventCatalogSource`,
parses them into validated :class:`~aws_events_mcp.models.Event` instances via
:func:`aws_events_mcp.parser.parse_events`, and caches the parsed list in memory
with a fetch timestamp and a TTL (default 15 minutes). All tool calls within the
TTL window operate on the cached list, keeping default-page responses well under
the 5-second budget (NFR Performance) and making downstream filtering and
pagination pure and deterministic (NFR Reliability).

Concurrency
-----------
A single concurrent refresh is guarded by an ``asyncio.Lock``. On a cache miss,
the first coroutine to acquire the lock performs the upstream fetch; any other
coroutine that was waiting re-checks the cache inside the lock and serves the
freshly cached result instead of issuing a second fetch (single-flight).

Failure handling
----------------
- Retrieval failures raised by the source (``CatalogUnreachableError``,
  ``CatalogTimeoutError``, ``CatalogUnparseableError``) propagate unchanged so
  the tool layer can map them onto the matching ``source_*`` error responses
  (Requirements 2.5, 11.1-11.3).
- When the source returns non-empty content but zero records parse into a valid
  ``Event``, ``get_events`` raises ``CatalogUnparseableError`` (Requirement
  11.3).
- A genuinely empty-but-valid catalog (the source returns zero records) is
  cached and returned as an empty list, never an error (Requirements 2.4, 10.7).

Partial-parse behavior
----------------------
When the source returns content in which at least one but not all records parse
(``parser.is_partial_parse`` is true), ``get_events`` **caches and returns the
successfully parsed events** and logs a warning identifying that some records
were skipped (Requirement 11.4). A partial parse is a successful, degraded
result — never an error — consistent with the lenient per-record skip-and-warn
behavior of Requirement 10. Only a wholly uninterpretable response (non-empty
content, zero valid events) is an error (``CatalogUnparseableError``,
Requirement 11.3).
"""

import asyncio
import time
from aws_events_mcp import consts
from aws_events_mcp.errors import CatalogUnparseableError
from aws_events_mcp.models import Event
from aws_events_mcp.parser import is_partial_parse, parse_events
from aws_events_mcp.source import EventCatalogSource
from loguru import logger
from typing import Optional


class CatalogCache:
    """Caches the parsed AWS Events catalog in memory with a TTL.

    Orchestrates an ``EventCatalogSource`` and the lenient parser, serving the
    parsed ``Event`` list from memory within the TTL window and refreshing it
    on expiry. Concurrent refreshes are collapsed into a single upstream fetch.

    Attributes are private; callers interact only through
    :meth:`get_events`.
    """

    def __init__(
        self, source: EventCatalogSource, ttl_seconds: int = consts.DEFAULT_CACHE_TTL_SECONDS
    ) -> None:
        """Initialize the catalog cache.

        Args:
            source: The catalog source used to retrieve raw records.
            ttl_seconds: Cache time-to-live in seconds; cached events are reused
                without a refetch until this many seconds have elapsed since the
                last successful fetch. Defaults to ``consts.DEFAULT_CACHE_TTL_SECONDS``
                (900 seconds / 15 minutes).
        """
        self._source = source
        self._ttl_seconds = ttl_seconds
        self._lock = asyncio.Lock()
        self._events: Optional[list[Event]] = None
        self._fetched_at: Optional[float] = None

    async def get_events(self, *, force_refresh: bool = False) -> list[Event]:
        """Return the cached parsed events, refreshing if needed.

        Serves the cached events when they are still within the TTL window.
        Otherwise (or when ``force_refresh`` is true) it refreshes under a lock
        so that concurrent cache misses trigger only one upstream fetch
        (single-flight); coroutines that were waiting on the lock re-check the
        cache and reuse the freshly fetched result.

        Args:
            force_refresh: When true, bypass the freshness check and always
                refetch and reparse the catalog.

        Returns:
            The parsed events. An empty list denotes a reachable, decodable, but
            empty catalog and is not an error (Requirements 2.4, 10.7).

        Raises:
            CatalogUnreachableError: The source could not be reached (Req 11.1).
            CatalogTimeoutError: The request exceeded the 30 second limit
                (Requirements 2.5, 11.2).
            CatalogUnparseableError: The source returned content that yielded no
                valid events (Requirement 11.3).

        Note:
            A partial parse (at least one but not all records parsed) is NOT an
            error: the successfully parsed events are returned and a warning is
            logged for the skipped records (Requirement 11.4).
        """
        if not force_refresh and self._is_fresh():
            return list(self._cached_events())

        async with self._lock:
            # Double-checked locking: a coroutine that waited on the lock while
            # another performed the fetch must serve the freshly cached result
            # rather than issuing a redundant upstream fetch (single-flight).
            if not force_refresh and self._is_fresh():
                return list(self._cached_events())
            return list(await self._refresh())

    def _is_fresh(self) -> bool:
        """Report whether cached events exist and are within the TTL window.

        Returns:
            ``True`` when events have been cached and fewer than ``ttl_seconds``
            have elapsed since the last successful fetch; otherwise ``False``.
        """
        if self._events is None or self._fetched_at is None:
            return False
        return (time.monotonic() - self._fetched_at) < self._ttl_seconds

    def _cached_events(self) -> list[Event]:
        """Return the currently cached events, or an empty list if unset.

        Returns:
            The cached event list, or an empty list when nothing is cached.
        """
        return self._events if self._events is not None else []

    async def _refresh(self) -> list[Event]:
        """Fetch, parse, and cache the catalog; surface parse outcomes.

        Returns:
            The freshly parsed events (possibly empty for an empty-but-valid
            catalog).

        Raises:
            CatalogUnreachableError: Propagated from the source (Req 11.1).
            CatalogTimeoutError: Propagated from the source (Req 2.5, 11.2).
            CatalogUnparseableError: Propagated from the source, or raised here
                when non-empty content yielded zero valid events (Req 11.3).
        """
        # Retrieval failures (unreachable/timeout/unparseable body) propagate
        # unchanged for the tool layer to map onto source_* responses.
        records = await self._source.fetch_raw_records()
        events, warnings = parse_events(records)
        record_count = len(records)

        # Non-empty content but nothing parsed: wholly uninterpretable (Req 11.3).
        # is_partial_parse is false here, so this is checked first and before
        # caching, leaving any prior cache untouched.
        if record_count > 0 and not events:
            logger.warning(
                f'AWS Events catalog returned {record_count} record(s) but none could be '
                'parsed into a valid event.'
            )
            raise CatalogUnparseableError(
                'The AWS Events catalog response could not be interpreted: no records '
                'produced a valid event.'
            )

        # Cache the successfully parsed events (including an empty-but-valid
        # catalog) with a fresh timestamp.
        self._events = events
        self._fetched_at = time.monotonic()

        # Partial parse (some but not all records parsed) is a SUCCESSFUL,
        # degraded result, not an error (Requirement 11.4): the successfully
        # parsed events are returned and a warning identifies the skipped
        # records, consistent with the lenient per-record skip-and-warn behavior
        # of Requirement 10. It is never surfaced as a source_partial error.
        if is_partial_parse(record_count, events):
            logger.warning(
                f'AWS Events catalog partially parsed: {len(events)} of {record_count} '
                f'record(s) produced a valid event; {len(warnings)} skipped. Returning the '
                'successfully parsed events.'
            )

        return events
