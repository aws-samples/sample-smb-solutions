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

"""Source failure-mapping integration tests through the tool layer.

These tests exercise the full path a real client takes -- tool -> shared
``_run_listing`` helper -> :class:`~aws_events_mcp.catalog.CatalogCache` ->
:class:`~aws_events_mcp.source.EventCatalogSource` -- to confirm that each
upstream failure mode is mapped onto the correct structured ``source_*`` error
response and that every such response excludes catalog data (empty ``items`` and
a ``total_count`` of zero). Unlike the unit-level Property 17 test, which maps
constructed exceptions directly through the response helper, this suite drives
the failures from a stub source so the cache's own
unparseable/partial-parse detection is validated end-to-end.

A single stub :class:`StubCatalogSource` implements the ``EventCatalogSource``
protocol's async ``fetch_raw_records`` and, per test, either raises a
``CatalogSourceError`` subtype or returns raw records that the real
``CatalogCache`` then classifies:

* raising ``CatalogUnreachableError`` -> ``source_unreachable`` (Requirement 11.1)
* raising ``CatalogTimeoutError`` -> ``source_timeout`` (Requirements 2.5, 11.2)
* returning wholly unparseable records (non-empty, all skipped by the parser) ->
  the cache raises ``CatalogUnparseableError`` -> ``source_unparseable``
  (Requirement 11.3)
* returning mixed valid/invalid records -> the cache raises
  ``CatalogPartialParseError`` -> ``source_partial`` (Requirement 11.4)

In every case the response must carry no items and a zero total count
(Requirements 2.5, 11.5).

The stub is injected with ``set_catalog_cache(CatalogCache(stub, ttl_seconds=...))``
and a fixture restores the global cache with ``set_catalog_cache(None)`` after
each test so the stub never leaks into other tests. The tools' ``Field(...)``
arguments resolve to ``FieldInfo`` objects when the decorated coroutine is
called directly, so every argument is passed explicitly (filters as ``None``, a
valid ``page_size``) to make the tool body run.

Validates: Requirements 2.5, 11.1, 11.2, 11.3, 11.4, 11.5
"""

import pytest
from aws_events_mcp import server
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.errors import (
    CatalogTimeoutError,
    CatalogUnreachableError,
)
from typing import Any, Dict, List, Optional


# A short TTL is irrelevant here because each stub is consulted at most once per
# test, but it keeps the cache construction explicit and self-documenting.
_CACHE_TTL_SECONDS = 60

# A raw record the lenient parser accepts: a flat mapping carrying the three
# required Event fields (event_id, title, start_date). location_mode is always
# derived by the parser, so this minimal shape validates into an Event.
_VALID_RECORD: Dict[str, Any] = {
    'event_id': 'evt-valid-1',
    'title': 'A Valid Event',
    'start_date': '2025-01-15',
}

# A raw record the parser skips: it is missing the required ``title`` field, so
# Event validation raises a "missing" error and the record is dropped.
_INVALID_RECORD: Dict[str, Any] = {
    'event_id': 'evt-invalid-1',
    'start_date': '2025-02-20',
}


class StubCatalogSource:
    """Stub ``EventCatalogSource`` driving a single failure/return behavior.

    Implements the protocol's async ``fetch_raw_records``. When constructed with
    an exception it raises it; otherwise it returns the configured raw records so
    the real :class:`~aws_events_mcp.catalog.CatalogCache` performs its own
    parse-result classification.
    """

    def __init__(
        self,
        *,
        records: Optional[List[Dict[str, Any]]] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        """Initialize the stub source.

        Args:
            records: Raw records to return from ``fetch_raw_records``; ignored
                when ``error`` is set.
            error: An exception instance to raise from ``fetch_raw_records``;
                takes precedence over ``records`` when supplied.
        """
        self._records = records if records is not None else []
        self._error = error

    async def fetch_raw_records(self) -> List[Dict[str, Any]]:
        """Return the configured records or raise the configured error.

        Returns:
            The configured raw record list.

        Raises:
            BaseException: The configured error, when one was supplied.
        """
        if self._error is not None:
            raise self._error
        return list(self._records)


@pytest.fixture
def install_stub_cache():
    """Provide an injector that installs a stub-backed cache and auto-resets it.

    Yields a callable that wraps a :class:`StubCatalogSource` in a real
    :class:`~aws_events_mcp.catalog.CatalogCache` and installs it via
    ``set_catalog_cache``. After the test the global cache is reset with
    ``set_catalog_cache(None)`` so the stub never leaks into other tests.

    Yields:
        A function ``install(source) -> None`` that injects the stub-backed
        cache.
    """

    def install(source: StubCatalogSource) -> None:
        server.set_catalog_cache(CatalogCache(source, ttl_seconds=_CACHE_TTL_SECONDS))

    try:
        yield install
    finally:
        server.set_catalog_cache(None)


async def _call_list_events() -> Dict[str, Any]:
    """Invoke ``list_events`` with explicit arguments through the tool layer.

    Passes every filter as ``None`` and a valid ``page_size`` so the decorated
    coroutine runs its body rather than receiving ``FieldInfo`` defaults.

    Returns:
        The tool's response dictionary.
    """
    return await server.list_events(
        learning_level=None,
        location_mode=None,
        location_text=None,
        event_type=None,
        partner=None,
        start_date=None,
        end_date=None,
        page_size=20,
        page_token=None,
    )


async def _call_search_events() -> Dict[str, Any]:
    """Invoke ``search_events`` with explicit arguments through the tool layer.

    Returns:
        The tool's response dictionary.
    """
    return await server.search_events(
        keyword='aws',
        learning_level=None,
        location_mode=None,
        location_text=None,
        event_type=None,
        partner=None,
        start_date=None,
        end_date=None,
        page_size=20,
        page_token=None,
    )


def _assert_source_error(response: Dict[str, Any], expected_error_type: str) -> None:
    """Assert a response is the expected source error carrying no catalog data.

    Args:
        response: The tool response dictionary to check.
        expected_error_type: The ``error_type`` the response must report.
    """
    assert response['status'] == 'error'
    assert response['error_type'] == expected_error_type
    assert response['items'] == []
    assert response['total_count'] == 0


async def test_list_events_maps_unreachable_source(install_stub_cache) -> None:
    """A source connection failure maps to ``source_unreachable`` with no items.

    Validates: Requirements 11.1, 11.5
    """
    install_stub_cache(StubCatalogSource(error=CatalogUnreachableError('connection failed')))

    response = await _call_list_events()

    _assert_source_error(response, 'source_unreachable')


async def test_list_events_maps_source_timeout(install_stub_cache) -> None:
    """A source timeout maps to ``source_timeout`` with no items.

    Validates: Requirements 2.5, 11.2, 11.5
    """
    install_stub_cache(StubCatalogSource(error=CatalogTimeoutError('exceeded 30 second limit')))

    response = await _call_list_events()

    _assert_source_error(response, 'source_timeout')


async def test_list_events_maps_wholly_unparseable_content(install_stub_cache) -> None:
    """Non-empty content that all fails to parse maps to ``source_unparseable``.

    The stub returns only invalid records, so the real ``CatalogCache`` raises
    ``CatalogUnparseableError`` (non-empty content, zero valid events), which the
    tool maps to ``source_unparseable``.

    Validates: Requirements 11.3, 11.5
    """
    install_stub_cache(StubCatalogSource(records=[dict(_INVALID_RECORD), dict(_INVALID_RECORD)]))

    response = await _call_list_events()

    _assert_source_error(response, 'source_unparseable')


async def test_list_events_maps_partial_content(install_stub_cache) -> None:
    """Mixed valid/invalid content maps to ``source_partial`` with no items.

    The stub returns one valid and one invalid record, so the real
    ``CatalogCache`` raises ``CatalogPartialParseError`` (some but not all
    parsed), which the tool maps to ``source_partial``. Per Requirement 11.5 the
    partially parsed data is excluded from the response.

    Validates: Requirements 11.4, 11.5
    """
    install_stub_cache(StubCatalogSource(records=[dict(_VALID_RECORD), dict(_INVALID_RECORD)]))

    response = await _call_list_events()

    _assert_source_error(response, 'source_partial')


async def test_search_events_maps_source_errors_identically(install_stub_cache) -> None:
    """``search_events`` maps an unreachable source the same way as listing.

    Confirms the shared ``_run_listing`` error mapping is not specific to
    ``list_events`` but applies across the listing/search tools.

    Validates: Requirements 11.1, 11.5
    """
    install_stub_cache(StubCatalogSource(error=CatalogUnreachableError('connection failed')))

    response = await _call_search_events()

    _assert_source_error(response, 'source_unreachable')
