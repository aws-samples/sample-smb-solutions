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

"""Property-based test for exact ``get_event_details`` lookup.

This module validates that the ``get_event_details`` tool performs an exact
identifier lookup over the cached catalog:

* For an identifier that exists in the catalog, the tool returns a success
  response whose returned event's ``event_id`` is exactly the requested id (an
  exact, not partial or fuzzy, match).
* For an identifier that is absent from the catalog, the tool returns a
  not-found result (``status`` ``'not_found'`` with ``event`` ``None``) and no
  event record.

The test injects a fake, in-memory catalog cache via
:func:`aws_events_mcp.server.set_catalog_cache` so no network I/O occurs. The
fake exposes the single async ``get_events()`` method the tool depends on,
holding a generated catalog whose ``event_id`` values are unique. The async tool
is driven from a synchronous ``@given`` test via :func:`asyncio.run`, and the
injected cache is always cleared in a ``finally`` block so tests stay isolated.

It lives in its own file so it can run in parallel with the other property
tests.

Feature: aws-events-mcp, Property 15: Event-details lookup is exact
Validates: Requirements 9.1, 9.4
"""

import asyncio
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.models import Event, LearningLevel, LocationMode
from aws_events_mcp.server import get_event_details, set_catalog_cache
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from typing import Any, Dict, List, Tuple, cast


#: Alphabet for generated ``event_id`` values: no surrounding whitespace, so a
#: stored id is unaffected by the tool's ``strip()`` and equals the id passed in.
_ID_ALPHABET = 'abc012XYZ'


class _FakeCatalogCache:
    """Minimal stand-in for ``CatalogCache`` holding a fixed event list.

    Exposes only the async ``get_events`` method that ``get_event_details``
    depends on, returning a defensive copy of the seeded catalog.

    Attributes:
        _events: The catalog the fake serves on every ``get_events`` call.
    """

    def __init__(self, events: List[Event]) -> None:
        """Seed the fake cache with a fixed catalog.

        Args:
            events: The events the fake returns from ``get_events``.
        """
        self._events = events

    async def get_events(self, *, force_refresh: bool = False) -> List[Event]:
        """Return the seeded catalog.

        Args:
            force_refresh: Accepted for interface compatibility; ignored.

        Returns:
            A shallow copy of the seeded event list.
        """
        return list(self._events)


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` with a whitespace-free identifier.

    Optional fields are independently present or absent so the generated
    catalog exercises events with and without each optional value. ``event_id``
    draws from a whitespace-free alphabet so it is left unchanged by the tool's
    identifier trimming and supports ``unique_by`` deduplication.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    small_text = st.text(alphabet='abcXYZ ', max_size=8)
    optional_small_text = st.none() | small_text
    return Event(
        event_id=draw(st.text(alphabet=_ID_ALPHABET, min_size=1, max_size=6)),
        title=draw(st.text(alphabet='abcXYZ ', min_size=1, max_size=8)),
        description=draw(small_text),
        start_date=draw(st.dates()),
        start_time=draw(st.none() | st.text(max_size=8)),
        time_zone=draw(st.none() | st.text(max_size=8)),
        location=draw(optional_small_text),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(optional_small_text),
        partner_name=draw(optional_small_text),
        registration_url=draw(st.none() | st.text(max_size=8)),
        learn_more_url=draw(st.none() | st.text(max_size=8)),
    )


@st.composite
def catalog_and_existing_id(draw: st.DrawFn) -> Tuple[List[Event], str]:
    """Generate a non-empty catalog with unique ids plus one existing id.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A ``(catalog, existing_id)`` tuple where ``catalog`` has unique
        ``event_id`` values and ``existing_id`` is the id of one of its events.
    """
    catalog = draw(
        st.lists(events(), min_size=1, max_size=20, unique_by=lambda event: event.event_id)
    )
    existing_id = draw(st.sampled_from([event.event_id for event in catalog]))
    return catalog, existing_id


@st.composite
def catalog_and_missing_id(draw: st.DrawFn) -> Tuple[List[Event], str]:
    """Generate a catalog with unique ids plus an id absent from it.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A ``(catalog, missing_id)`` tuple where ``catalog`` has unique
        ``event_id`` values and ``missing_id`` matches no event in ``catalog``.
    """
    catalog = draw(st.lists(events(), max_size=20, unique_by=lambda event: event.event_id))
    existing_ids = {event.event_id for event in catalog}
    missing_id = draw(st.text(alphabet=_ID_ALPHABET, min_size=1, max_size=7))
    assume(missing_id not in existing_ids)
    return catalog, missing_id


def _lookup(catalog: List[Event], event_id: str) -> Dict[str, Any]:
    """Run ``get_event_details`` against a fake cache seeded with ``catalog``.

    Installs a :class:`_FakeCatalogCache` over ``catalog`` via
    :func:`set_catalog_cache`, drives the async tool to completion with
    :func:`asyncio.run`, and always clears the injected cache afterward so each
    example runs in isolation.

    Args:
        catalog: The events the fake cache serves.
        event_id: The identifier to look up.

    Returns:
        The tool's response dictionary.
    """
    set_catalog_cache(cast(CatalogCache, _FakeCatalogCache(catalog)))
    try:
        return asyncio.run(get_event_details(event_id))
    finally:
        set_catalog_cache(None)


# Feature: aws-events-mcp, Property 15: Event-details lookup is exact
@settings(max_examples=100)
@given(case=catalog_and_existing_id())
def test_existing_identifier_returns_exact_match(case: Tuple[List[Event], str]) -> None:
    """An id present in the catalog yields a success with that exact event.

    Validates: Requirements 9.1, 9.4
    """
    catalog, existing_id = case
    response = _lookup(catalog, existing_id)

    assert response['status'] == 'success'
    returned_event = response['event']
    assert returned_event is not None
    # Exact match: the returned event's id equals the requested id exactly.
    assert returned_event['event_id'] == existing_id


# Feature: aws-events-mcp, Property 15: Event-details lookup is exact
@settings(max_examples=100)
@given(case=catalog_and_missing_id())
def test_absent_identifier_returns_not_found(case: Tuple[List[Event], str]) -> None:
    """An id absent from the catalog yields a not-found result with no event.

    Validates: Requirements 9.1, 9.4
    """
    catalog, missing_id = case
    response = _lookup(catalog, missing_id)

    assert response['status'] == 'not_found'
    assert response['event'] is None
    # No event record is returned for an unmatched identifier.
    assert 'items' not in response or response.get('items') == []
