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

"""Integration tests for source-failure mapping through the tool layer.

These tests assert end-to-end that each upstream failure mode surfaces as the
correct structured ``source_*`` error response when a tool is invoked, and that
every such response carries no event records (Requirements 2.5, 11.1-11.5).

Unlike the unit-level mapping test (which constructs each ``CatalogSourceError``
and runs it through :func:`~aws_events_mcp.server.build_source_error_response`),
this module drives the *real* :class:`~aws_events_mcp.catalog.CatalogCache` over
a fake :class:`~aws_events_mcp.source.EventCatalogSource`, installs it via
:func:`~aws_events_mcp.server.set_catalog_cache`, and invokes the real
``list_events`` tool. The error therefore propagates through the genuine
cache -> tool path:

* connection failure -- the fake source raises ``CatalogUnreachableError``,
  which the cache re-raises and the tool maps to ``source_unreachable``
  (Requirement 11.1);
* timeout -- the fake source raises ``CatalogTimeoutError`` -> ``source_timeout``
  (Requirement 11.2);
* wholly unparseable -- the fake returns non-empty raw records that all fail to
  parse (each missing the required ``title``), so the *real* parser skips them
  all and the *real* cache raises ``CatalogUnparseableError`` ->
  ``source_unparseable`` (Requirement 11.3);
* partially parseable -- the fake returns a mix of valid records
  (``event_id`` / ``title`` / ``start_date``) and invalid records (missing
  ``title``), so the real parser produces some-but-not-all events and the real
  cache returns the successfully parsed event(s). A partial parse is a
  successful, degraded result (not an error), so the tool returns a success
  ``list_events`` response containing the valid event(s), never ``source_partial``
  (Requirement 11.4).

For every failure case the response is asserted to be an error carrying
``items == []`` and ``total_count == 0`` (Requirement 11.5).

The async tool is driven via ``asyncio.run`` from synchronous tests, and the
injected global cache is always cleared with ``set_catalog_cache(None)`` in a
``finally`` block so no state leaks between tests.

Validates: Requirements 2.5, 11.1, 11.2, 11.3, 11.4, 11.5
"""

import asyncio
import pytest
from aws_events_mcp import server
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.errors import (
    CatalogSourceError,
    CatalogTimeoutError,
    CatalogUnreachableError,
)
from typing import Any, Dict, List


def _valid_record(event_id: str, title: str = 'Sample Event') -> dict:
    """Build a raw record that the real parser maps to a valid ``Event``.

    The parser flattens flat fixtures unchanged and always derives
    ``location_mode`` (defaulting to virtual), so only an identifier, a
    non-empty title, and a parseable ``start_date`` are required for a record to
    parse successfully (see ``parser._FIELD_KEYS`` required fields).

    Args:
        event_id: Value supplied under ``event_id`` (the unique identifier).
        title: Non-empty event title.

    Returns:
        A raw record dict that parses into a valid ``Event``.
    """
    return {'event_id': event_id, 'title': title, 'start_date': '2025-06-01'}


def _invalid_record(event_id: str) -> dict:
    """Build a raw record the real parser skips (missing the required ``title``).

    Args:
        event_id: Value supplied under ``event_id`` so the skip warning can name
            the record.

    Returns:
        A raw record dict that fails ``Event`` validation and is skipped.
    """
    return {'event_id': event_id, 'start_date': '2025-06-01'}


class _RaisingSource:
    """Fake ``EventCatalogSource`` whose fetch raises a fixed source error.

    Used for the connection-failure and timeout cases, where the source itself
    surfaces the typed error before any parsing occurs.
    """

    def __init__(self, error: CatalogSourceError) -> None:
        """Initialize the raising source.

        Args:
            error: The ``CatalogSourceError`` subtype instance to raise on fetch.
        """
        self._error = error

    async def fetch_raw_records(self) -> List[dict]:
        """Raise the configured source error.

        Raises:
            CatalogSourceError: The configured error subtype.
        """
        raise self._error


class _RecordsSource:
    """Fake ``EventCatalogSource`` returning canned raw records.

    Used for the wholly- and partially-unparseable cases: the fake returns raw
    records and lets the *real* parser and cache decide the outcome, so the
    ``source_unparseable`` / ``source_partial`` mapping is exercised genuinely
    rather than simulated.
    """

    def __init__(self, records: List[dict]) -> None:
        """Initialize the records source.

        Args:
            records: The canned raw records returned by each fetch.
        """
        self._records = records

    async def fetch_raw_records(self) -> List[dict]:
        """Return a copy of the canned records.

        Returns:
            A shallow copy of the configured raw records.
        """
        return list(self._records)


def _invoke_list_events(source: Any) -> Dict[str, Any]:
    """Invoke ``list_events`` over a real cache backed by ``source``.

    Builds a real :class:`~aws_events_mcp.catalog.CatalogCache` over the supplied
    fake source, installs it through ``set_catalog_cache``, and awaits the real
    ``list_events`` tool with explicit ``None`` filter arguments (so the raw tool
    function does not fall back to its ``Field(...)`` schema defaults). The
    injected cache is always cleared afterwards.

    Args:
        source: A fake ``EventCatalogSource`` (raising or records-returning).

    Returns:
        The tool's response dictionary.
    """
    server.set_catalog_cache(CatalogCache(source, ttl_seconds=900))
    try:
        return asyncio.run(
            server.list_events(
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
        )
    finally:
        server.set_catalog_cache(None)


def _assert_source_error(response: Dict[str, Any], expected_error_type: str) -> None:
    """Assert a response is the expected source error carrying no records.

    Args:
        response: The tool response dictionary.
        expected_error_type: The ``error_type`` the response must report.
    """
    assert response['status'] == 'error'
    assert response['error_type'] == expected_error_type
    assert response['items'] == []
    assert response['total_count'] == 0


def test_connection_failure_maps_to_source_unreachable() -> None:
    """A source connection failure surfaces as ``source_unreachable`` (Req 11.1)."""
    response = _invoke_list_events(_RaisingSource(CatalogUnreachableError('connection failed')))
    _assert_source_error(response, 'source_unreachable')


def test_timeout_maps_to_source_timeout() -> None:
    """A source timeout surfaces as ``source_timeout`` (Req 2.5, 11.2)."""
    response = _invoke_list_events(
        _RaisingSource(CatalogTimeoutError('request exceeded the 30 second limit'))
    )
    _assert_source_error(response, 'source_timeout')


def test_wholly_unparseable_maps_to_source_unparseable() -> None:
    """Non-empty content that wholly fails to parse maps to ``source_unparseable``.

    The real parser skips every record (each missing the required ``title``) and
    the real cache raises ``CatalogUnparseableError`` (Requirement 11.3).
    """
    response = _invoke_list_events(
        _RecordsSource([_invalid_record('bad1'), _invalid_record('bad2')])
    )
    _assert_source_error(response, 'source_unparseable')


def test_partially_parseable_returns_parsed_events() -> None:
    """Mixed valid/invalid content returns a success response with the valid event (Req 11.4).

    The real parser produces one event from the valid record and skips the
    invalid one, so the real cache returns the successfully parsed event. A
    partial parse is a successful, degraded result, so the tool returns a
    success response containing the valid event rather than ``source_partial``.
    """
    response = _invoke_list_events(_RecordsSource([_valid_record('good'), _invalid_record('bad')]))
    assert response['status'] == 'success'
    assert response.get('error_type') != 'source_partial'
    assert len(response['items']) == 1
    assert response['total_count'] == 1


@pytest.mark.parametrize(
    ('source', 'expected_error_type'),
    [
        (_RaisingSource(CatalogUnreachableError('connection failed')), 'source_unreachable'),
        (_RaisingSource(CatalogTimeoutError('timed out')), 'source_timeout'),
        (_RecordsSource([_invalid_record('a'), _invalid_record('b')]), 'source_unparseable'),
    ],
)
def test_all_source_failures_carry_no_records(source: Any, expected_error_type: str) -> None:
    """Every genuine source-failure response excludes catalog data (Req 11.5).

    Covers the unreachable, timeout, and wholly-unparseable mappings in one place
    to assert the shared invariant that an error result carries an empty
    ``items`` collection and a ``total_count`` of zero. A partial parse is not a
    failure and is covered separately by
    ``test_partially_parseable_returns_parsed_events``.
    """
    response = _invoke_list_events(source)
    _assert_source_error(response, expected_error_type)
