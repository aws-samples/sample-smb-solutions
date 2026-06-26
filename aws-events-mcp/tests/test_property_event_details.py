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

"""Property-based test for exact event-details lookup.

This module validates that ``get_event_details`` performs an exact identifier
lookup over the cached catalog: for an identifier that exists in the catalog the
tool returns a ``success`` response whose event's ``event_id`` exactly equals the
requested identifier (and that event is the single match, since the catalog is
generated with unique identifiers); for an identifier that is absent the tool
returns a ``not_found`` result with no event.

The tool is exercised end-to-end through the server layer by installing a fake
catalog cache (exposing ``async def get_events()``) via
:func:`aws_events_mcp.server.set_catalog_cache`, so the property covers the real
matching loop in ``get_event_details`` rather than a reimplementation. The
global cache is reset to its lazy default after every example.

It lives in its own file so it can run in parallel with the other property
tests.

Feature: aws-events-mcp, Property 15: Event-details lookup is exact
Validates: Requirements 9.1, 9.4
"""

import asyncio
import pytest
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.models import Event, LearningLevel, LocationMode
from aws_events_mcp.server import get_event_details, set_catalog_cache
from hypothesis import given, settings
from hypothesis import strategies as st
from typing import List, Tuple, cast


#: Alphabet used for generated identifiers. The sentinel ``#`` is deliberately
#: excluded so a guaranteed-absent identifier can be minted by appending it.
_ID_ALPHABET = 'abc012XYZ'


class FakeCatalogCache:
    """Stand-in catalog cache returning a fixed, in-memory list of events.

    Mirrors the single method ``get_event_details`` depends on
    (``async def get_events()``), so installing an instance via
    :func:`aws_events_mcp.server.set_catalog_cache` makes the tool resolve
    lookups against the generated catalog with no network access.

    Attributes:
        events: The catalog returned by every :meth:`get_events` call.
    """

    def __init__(self, events: List[Event]) -> None:
        """Initialize the fake cache.

        Args:
            events: The catalog of events to serve.
        """
        self.events = events

    async def get_events(self, *, force_refresh: bool = False) -> List[Event]:
        """Return the configured catalog.

        Args:
            force_refresh: Accepted for interface parity with the real cache;
                ignored because the fake catalog is static.

        Returns:
            The configured list of events.
        """
        return self.events


def _build_event(draw: st.DrawFn, event_id: str) -> Event:
    """Build a valid ``Event`` with the given identifier and arbitrary fields.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.
        event_id: The unique identifier to assign to the event.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    optional_text = st.none() | st.text(alphabet='abcXYZ ', max_size=8)
    return Event(
        event_id=event_id,
        title=draw(st.text(alphabet='abcXYZ ', min_size=1, max_size=8)),
        description=draw(st.text(alphabet='abcXYZ ', max_size=8)),
        start_date=draw(st.dates()),
        start_time=draw(st.none() | st.text(max_size=8)),
        time_zone=draw(st.none() | st.text(max_size=8)),
        location=draw(optional_text),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(optional_text),
        partner_name=draw(optional_text),
        registration_url=draw(st.none() | st.text(max_size=8)),
        learn_more_url=draw(st.none() | st.text(max_size=8)),
    )


def _near_miss_ids(lookup_id: str) -> List[str]:
    """Build near-miss variants of ``lookup_id`` that must NOT match it exactly.

    The variants differ from ``lookup_id`` only by case or by a prefix/suffix
    edit, so seeding the catalog with them confirms ``get_event_details`` returns
    the requested event by an *exact* identifier match rather than a fuzzy,
    case-insensitive, or prefix match.

    Args:
        lookup_id: The identifier that will be looked up.

    Returns:
        Distinct near-miss identifiers, each different from ``lookup_id``.
    """
    candidates = [
        lookup_id.swapcase(),  # case-only difference
        lookup_id[:-1],  # one-char-shorter prefix
        lookup_id + lookup_id[0],  # one-char-longer extension
    ]
    seen = set()
    variants: List[str] = []
    for candidate in candidates:
        if candidate and candidate != lookup_id and candidate not in seen:
            seen.add(candidate)
            variants.append(candidate)
    return variants


@st.composite
def catalogs_and_lookups(draw: st.DrawFn) -> Tuple[List[Event], str, bool]:
    """Generate a unique-id catalog plus a lookup id and whether it should match.

    A list of distinct identifiers is drawn and mapped to events so every
    ``event_id`` in the catalog is unique. The lookup identifier is then drawn
    from one of two cases:

    * **match** (only when the catalog is non-empty): an identifier sampled from
      the catalog, which must resolve to exactly one event. Near-miss identifiers
      (differing only by case or by a prefix/suffix edit) are also seeded into
      the catalog so the property confirms only an *exact* match succeeds; or
    * **no match**: an identifier minted by appending the ``#`` sentinel (absent
      from the id alphabet), which is guaranteed not to appear in the catalog.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A tuple ``(catalog, lookup_id, expect_match)``.
    """
    event_ids = draw(
        st.lists(
            st.text(alphabet=_ID_ALPHABET, min_size=1, max_size=6),
            max_size=20,
            unique=True,
        )
    )
    catalog = [_build_event(draw, event_id) for event_id in event_ids]

    want_match = draw(st.booleans())
    if want_match and event_ids:
        lookup_id = draw(st.sampled_from(event_ids))
        # Seed near-miss ids (not already present) so only an EXACT match wins.
        existing = set(event_ids)
        for variant in _near_miss_ids(lookup_id):
            if variant not in existing:
                existing.add(variant)
                catalog.append(_build_event(draw, variant))
        return catalog, lookup_id, True

    # Guaranteed-absent identifier: '#' never appears in any generated id.
    absent_id = draw(st.text(alphabet=_ID_ALPHABET, max_size=6)) + '#'
    return catalog, absent_id, False


@pytest.fixture(autouse=True)
def _reset_catalog_cache():
    """Reset the process-wide catalog cache after each example.

    Ensures the injected fake cache never leaks into other tests by restoring
    the lazy default once the test completes.
    """
    yield
    set_catalog_cache(None)


# Feature: aws-events-mcp, Property 15: Event-details lookup is exact
@settings(max_examples=100)
@given(case=catalogs_and_lookups())
def test_event_details_lookup_is_exact(case: Tuple[List[Event], str, bool]) -> None:
    """``get_event_details`` matches an exact id or reports not-found.

    Validates: Requirements 9.1, 9.4
    """
    catalog, lookup_id, expect_match = case
    set_catalog_cache(cast(CatalogCache, FakeCatalogCache(catalog)))
    try:
        response = asyncio.run(get_event_details(event_id=lookup_id))
    finally:
        set_catalog_cache(None)

    if expect_match:
        # Unique ids guarantee exactly one catalog event carries this id.
        matching = [event for event in catalog if event.event_id == lookup_id]
        assert len(matching) == 1
        assert response['status'] == 'success'
        assert response['event']['event_id'] == lookup_id
    else:
        assert all(event.event_id != lookup_id for event in catalog)
        assert response['status'] == 'not_found'
        assert response['event'] is None
