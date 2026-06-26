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

"""End-to-end smoke test for the AWS Events MCP Server tool layer.

This module wires the *real* server tool layer to a mocked catalog source and
invokes each registered tool through the server, asserting well-formed
responses. It complements the property-based tests (which target individual pure
layers) by verifying that the whole pipeline -- source -> parser -> cache ->
query/pagination -> tool response shaping -- composes correctly behind the
public tools (Requirements 1.1, 2.1, 4.1, 9.1).

Test isolation and no network I/O
---------------------------------
A small :class:`_FixedCatalogSource` implements the
:class:`~aws_events_mcp.source.EventCatalogSource` interface, returning a fixed
handful of flat raw records that the lenient parser accepts. A *real*
:class:`~aws_events_mcp.catalog.CatalogCache` is built over the fake source and
injected through :func:`aws_events_mcp.server.set_catalog_cache`, so the tools
exercise the genuine retrieve -> parse -> filter -> paginate path with zero
network access. The ``catalog`` fixture clears the injected cache in teardown via
``set_catalog_cache(None)`` so each test -- and any later test in the suite --
starts from the lazy default again.

The fixture's records use future start dates (relative to today in UTC) so the
``list_upcoming_events`` assertion has a non-empty, deterministic result set.

Live latency check
------------------
An optional ``@pytest.mark.live`` test exercises the real upstream endpoint via
a default JSON-API-backed cache and asserts a default-size page returns within
the 5-second performance target (NFR Performance). It is deselected by default
(run the suite with ``-m "not live"``) and skips itself when the public catalog
is unreachable, so it never fails CI without network access.

Validates: Requirements 1.1, 2.1, 4.1, 9.1, NFR Performance
"""

import asyncio
import pytest
import time
from aws_events_mcp import server
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.consts import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from aws_events_mcp.errors import CatalogSourceError
from aws_events_mcp.source import JsonApiCatalogSource
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List


#: Number of events in the fixed mocked catalog.
_CATALOG_SIZE = 3

#: A keyword present in exactly one fixture event's title ("Kubernetes Summit").
_UNIQUE_KEYWORD = 'Kubernetes'

#: Identifier of an event known to exist in the fixed catalog.
_KNOWN_EVENT_ID = 'evt-001'

#: Identifier guaranteed to be absent from the fixed catalog.
_ABSENT_EVENT_ID = 'evt-does-not-exist'

#: The twelve presentation fields ``get_event_details`` must always include
#: (Requirements 9.2, 9.3).
_DETAILS_FIELDS = (
    'title',
    'description',
    'start_date',
    'start_time',
    'time_zone',
    'location',
    'location_mode',
    'learning_level',
    'event_type',
    'partner_name',
    'registration_url',
    'learn_more_url',
)


def _fixed_catalog_records() -> List[Dict[str, Any]]:
    """Build the small fixed list of flat raw records for the mocked source.

    The records use the parser's accepted flat keys and carry future start dates
    (relative to today in UTC) so every event is "upcoming", giving
    ``list_upcoming_events`` a non-empty, deterministic result set. Exactly one
    record's title contains :data:`_UNIQUE_KEYWORD`.

    Returns:
        A list of :data:`_CATALOG_SIZE` raw record dicts the real parser accepts.
    """
    today = datetime.now(timezone.utc).date()

    def future(days: int) -> str:
        """Return an ISO ``YYYY-MM-DD`` date ``days`` days after today (UTC)."""
        return (today + timedelta(days=days)).isoformat()

    return [
        {
            'event_id': 'evt-001',
            'title': 'Serverless Tech Talk',
            'description': 'Learn about building serverless apps with AWS Lambda.',
            'start_date': future(7),
            'start_time': '09:00',
            'time_zone': 'PDT',
            'location': 'Online',
            'location_mode': 'virtual',
            'learning_level': 'Foundational',
            'event_type': 'Tech Talk',
            'partner_name': None,
            'registration_url': 'https://aws.amazon.com/events/evt-001/register',
            'learn_more_url': 'https://aws.amazon.com/events/evt-001',
        },
        {
            'event_id': 'evt-002',
            'title': 'Kubernetes Summit',
            'description': 'A deep dive into Amazon EKS and container orchestration.',
            'start_date': future(14),
            'start_time': '10:30',
            'time_zone': 'EDT',
            'location': 'Seattle, WA',
            'location_mode': 'physical',
            'learning_level': 'Advanced',
            'event_type': 'Summit',
            'partner_name': 'ExamplePartner',
            'registration_url': 'https://aws.amazon.com/events/evt-002/register',
            'learn_more_url': 'https://aws.amazon.com/events/evt-002',
        },
        {
            'event_id': 'evt-003',
            'title': 'Data Analytics Webinar',
            'description': 'Modern analytics with Amazon Redshift and QuickSight.',
            'start_date': future(21),
            'start_time': '14:00',
            'time_zone': 'UTC',
            'location': 'Online',
            'location_mode': 'virtual',
            'learning_level': 'Intermediate',
            'event_type': 'Webinar',
            'partner_name': None,
            'registration_url': 'https://aws.amazon.com/events/evt-003/register',
            'learn_more_url': 'https://aws.amazon.com/events/evt-003',
        },
    ]


class _FixedCatalogSource:
    """In-memory ``EventCatalogSource`` returning a fixed list of raw records.

    Implements the single async method the cache depends on, so a real
    :class:`~aws_events_mcp.catalog.CatalogCache` can be built over it and the
    tools exercise their genuine parse/cache/query path with no network access.

    Attributes:
        records: The raw records returned by every ``fetch_raw_records`` call.
    """

    def __init__(self, records: List[Dict[str, Any]]) -> None:
        """Seed the fake source with a fixed record list.

        Args:
            records: The raw records to return on every fetch.
        """
        self.records = records

    async def fetch_raw_records(self) -> List[Dict[str, Any]]:
        """Return a defensive copy of the seeded raw records.

        Returns:
            A shallow copy of the seeded record list.
        """
        return list(self.records)


@pytest.fixture
def catalog() -> Iterator[List[Dict[str, Any]]]:
    """Install a fake-source-backed catalog cache and clear it on teardown.

    Builds a real :class:`~aws_events_mcp.catalog.CatalogCache` over a
    :class:`_FixedCatalogSource` of fixed records and injects it via
    :func:`aws_events_mcp.server.set_catalog_cache`, so the full
    retrieve -> parse -> filter -> paginate path runs end-to-end. After the test
    the injected cache is cleared with ``set_catalog_cache(None)`` so the next
    consumer rebuilds the lazy default and tests stay isolated.

    Yields:
        The fixed raw records backing the installed cache.
    """
    records = _fixed_catalog_records()
    server.set_catalog_cache(CatalogCache(_FixedCatalogSource(records), ttl_seconds=900))
    try:
        yield records
    finally:
        server.set_catalog_cache(None)


def _run(coro: Any) -> Dict[str, Any]:
    """Drive an async tool coroutine to completion from a sync test.

    Args:
        coro: The coroutine returned by invoking an async tool.

    Returns:
        The tool's response dictionary.
    """
    return asyncio.run(coro)


def test_list_events_returns_well_formed_response(catalog: List[Dict[str, Any]]) -> None:
    """``list_events`` returns a well-formed success page over the whole catalog.

    Asserts a success status, a list of ``items``, an integer ``total_count``,
    and that the requested ``page_size`` is echoed back.

    Validates: Requirements 1.1, 2.1
    """
    page_size = DEFAULT_PAGE_SIZE
    response = _run(
        server.list_events(
            learning_level=None,
            location_mode=None,
            location_text=None,
            event_type=None,
            partner=None,
            start_date=None,
            end_date=None,
            page_size=page_size,
            page_token=None,
        )
    )

    assert response['status'] == 'success'
    assert isinstance(response['items'], list)
    assert isinstance(response['total_count'], int)
    # The whole fixed catalog fits within one default page.
    assert response['total_count'] == _CATALOG_SIZE
    assert len(response['items']) == _CATALOG_SIZE
    # The requested page size is echoed back to the caller.
    assert response['page_size'] == page_size
    # No further page remains, so no token is emitted.
    assert 'next_page_token' not in response
    # Results are ordered by start date ascending.
    start_dates = [item['start_date'] for item in response['items']]
    assert start_dates == sorted(start_dates)


def test_search_events_returns_keyword_matches(catalog: List[Dict[str, Any]]) -> None:
    """``search_events`` returns only events whose text matches the keyword.

    Validates: Requirements 4.1
    """
    response = _run(
        server.search_events(
            keyword=_UNIQUE_KEYWORD,
            learning_level=None,
            location_mode=None,
            location_text=None,
            event_type=None,
            partner=None,
            start_date=None,
            end_date=None,
            page_size=DEFAULT_PAGE_SIZE,
            page_token=None,
        )
    )

    assert response['status'] == 'success'
    assert isinstance(response['items'], list)
    assert response['total_count'] == 1
    assert len(response['items']) == 1
    # Every returned event matches the keyword in its title or description.
    needle = _UNIQUE_KEYWORD.casefold()
    for item in response['items']:
        haystack = f'{item.get("title", "")} {item.get("description", "")}'.casefold()
        assert needle in haystack
    assert response['items'][0]['event_id'] == 'evt-002'


def test_list_upcoming_events_returns_well_formed_response(
    catalog: List[Dict[str, Any]],
) -> None:
    """``list_upcoming_events`` returns the fixture's future-dated events.

    Every fixture event is future-dated, so all of them are upcoming.

    Validates: Requirements 1.1, 2.1
    """
    today = datetime.now(timezone.utc).date()
    response = _run(
        server.list_upcoming_events(
            learning_level=None,
            location_mode=None,
            location_text=None,
            event_type=None,
            partner=None,
            start_date=None,
            end_date=None,
            page_size=MAX_PAGE_SIZE,
            page_token=None,
        )
    )

    assert response['status'] == 'success'
    assert isinstance(response['items'], list)
    assert isinstance(response['total_count'], int)
    assert response['total_count'] == _CATALOG_SIZE
    assert len(response['items']) == _CATALOG_SIZE
    for item in response['items']:
        assert item['start_date'] >= today.isoformat()


def test_get_event_details_known_id_returns_all_twelve_fields(
    catalog: List[Dict[str, Any]],
) -> None:
    """``get_event_details`` returns a known event with all twelve fields.

    Validates: Requirements 9.1
    """
    response = _run(server.get_event_details(_KNOWN_EVENT_ID))

    assert response['status'] == 'success'
    event = response['event']
    assert event is not None
    assert event['event_id'] == _KNOWN_EVENT_ID
    for field in _DETAILS_FIELDS:
        assert field in event, f'event details missing field {field!r}'


def test_get_event_details_absent_id_is_not_found(catalog: List[Dict[str, Any]]) -> None:
    """``get_event_details`` returns not-found for an absent identifier.

    Validates: Requirements 9.1
    """
    response = _run(server.get_event_details(_ABSENT_EVENT_ID))

    assert response['status'] == 'not_found'
    assert response['event'] is None


@pytest.mark.live
def test_live_default_page_within_latency_target() -> None:
    """A live default page returns within the 5-second performance target.

    Exercises the real upstream endpoint through a default JSON-API-backed
    cache. Deselected by default (``-m "not live"``) and skipped when the public
    catalog is unreachable, so it never fails CI without network access.

    Validates: NFR Performance
    """
    server.set_catalog_cache(CatalogCache(JsonApiCatalogSource(), ttl_seconds=900))
    try:
        started = time.monotonic()
        response = _run(
            server.list_events(
                learning_level=None,
                location_mode=None,
                location_text=None,
                event_type=None,
                partner=None,
                start_date=None,
                end_date=None,
                page_size=DEFAULT_PAGE_SIZE,
                page_token=None,
            )
        )
        elapsed = time.monotonic() - started
    except CatalogSourceError as exc:
        pytest.skip(f'AWS Events catalog unavailable for live test: {exc}')
    finally:
        server.set_catalog_cache(None)

    if response['status'] != 'success':
        pytest.skip(f'Live catalog returned a non-success response: {response.get("error_type")}')
    assert elapsed < 5.0, f'default page took {elapsed:.2f}s, exceeding the 5s target'
    assert isinstance(response['items'], list)
