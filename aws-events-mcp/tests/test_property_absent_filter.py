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

"""Property-based test for absent-filter identity.

This module validates that an omitted or empty filter dimension is the identity
over that dimension: leaving a filter unset (or setting it to ``None``/empty)
never removes events along that dimension. Two complementary facts are checked
against ``query.apply_query``:

* IDENTITY OF THE EMPTY QUERY - applying an entirely empty ``EventQuery`` returns
  exactly the catalog events (as a multiset, ignoring order), so no event is
  excluded when no filter is supplied.
* PER-DIMENSION IDENTITY - for each filter dimension, populating it with an
  empty/``None`` value yields the same matched set as leaving it unset, *and*
  yields the same matched set as the empty query. In other words, an empty value
  for one dimension does not reduce the result along that dimension even when no
  other filter is active.
* MONOTONICITY OF CLEARING A DIMENSION - starting from an arbitrary populated
  query, clearing exactly one dimension never removes any event: the cleared
  query's matched set is a superset (by ``event_id``) of the populated query's
  matched set. This confirms an absent filter cannot exclude events even when
  other filters remain active.

Results are compared as multisets (sorted by ``(start_date, event_id)``) so the
assertions concern membership rather than ordering. The test lives in its own
file so it can run in parallel with the other property tests.

Feature: aws-events-mcp, Property 8: Absent filters do not exclude events
Validates: Requirements 5.5, 6.4
"""

from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query
from datetime import date
from hypothesis import given, settings
from hypothesis import strategies as st
from typing import Any


# Date window for generated catalog events.
_MIN_DATE = date(2023, 1, 1)
_MAX_DATE = date(2025, 12, 31)

# Empty/absent values for each optional filter dimension. Each entry maps an
# ``EventQuery`` field name to a value that should impose no constraint: ``None``
# for the optional scalar/date filters, the empty string for the bounded-string
# filters, and the empty list for the learning-level set.
_EMPTY_FILTER_VALUES: dict[str, Any] = {
    'keyword': '',
    'learning_levels': [],
    'location_mode': None,
    'location_text': '',
    'event_type': '',
    'partner': '',
    'start_date': None,
    'end_date': None,
}


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` with a mix of present and absent optional fields.

    Optional fields are independently present or absent so the catalog exercises
    the ``None`` branches of every filter dimension. ``event_id`` values come
    from the catalog generator, which guarantees uniqueness.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event``.
    """
    text = st.text(min_size=0, max_size=8)
    return Event(
        event_id='placeholder',
        title=draw(st.text(min_size=1, max_size=8)),
        description=draw(text),
        start_date=draw(st.dates(min_value=_MIN_DATE, max_value=_MAX_DATE)),
        start_time=draw(st.none() | text),
        time_zone=draw(st.none() | text),
        location=draw(st.none() | text),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(st.none() | text),
        partner_name=draw(st.none() | text),
    )


@st.composite
def catalogs(draw: st.DrawFn) -> list[Event]:
    """Generate a catalog of events with unique identifiers.

    Identifiers are assigned positionally (``evt-0``, ``evt-1``, ...) so multiset
    comparison by event identity is unambiguous. Catalogs may be empty.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A list of distinct ``Event`` instances.
    """
    base = draw(st.lists(events(), max_size=15))
    return [
        event.model_copy(update={'event_id': f'evt-{index}'}) for index, event in enumerate(base)
    ]


def _sorted(events_list: list[Event]) -> list[Event]:
    """Return events sorted by ``(start_date, event_id)`` for multiset comparison.

    Args:
        events_list: The events to sort.

    Returns:
        A new list ordered deterministically so two result sets can be compared
        as multisets independent of ``apply_query``'s output ordering.
    """
    return sorted(events_list, key=lambda event: (event.start_date, event.event_id))


@st.composite
def populated_queries(draw: st.DrawFn) -> EventQuery:
    """Generate an ``EventQuery`` with a random subset of dimensions populated.

    Each dimension is independently either populated with a constraining value or
    left at its default empty/``None`` state, so the generated query exercises
    arbitrary combinations of active filters. Free-text and date values are drawn
    to overlap the catalog generator's space so the filters both match and miss.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        An ``EventQuery`` with zero or more dimensions populated.
    """
    text = st.text(min_size=1, max_size=4)
    fields: dict[str, Any] = {}
    if draw(st.booleans()):
        fields['keyword'] = draw(text)
    if draw(st.booleans()):
        fields['learning_levels'] = draw(
            st.lists(st.sampled_from(list(LearningLevel)), min_size=1, max_size=4, unique=True)
        )
    if draw(st.booleans()):
        fields['location_mode'] = draw(st.sampled_from(list(LocationMode)))
    if draw(st.booleans()):
        fields['location_text'] = draw(text)
    if draw(st.booleans()):
        fields['event_type'] = draw(text)
    if draw(st.booleans()):
        fields['partner'] = draw(text)
    if draw(st.booleans()):
        fields['start_date'] = draw(st.dates(min_value=_MIN_DATE, max_value=_MAX_DATE))
    if draw(st.booleans()):
        fields['end_date'] = draw(st.dates(min_value=_MIN_DATE, max_value=_MAX_DATE))
    return EventQuery(**fields)


# Feature: aws-events-mcp, Property 8: Absent filters do not exclude events
@settings(max_examples=100)
@given(catalog=catalogs())
def test_empty_query_returns_entire_catalog(catalog: list[Event]) -> None:
    """An entirely empty EventQuery returns exactly the catalog (same multiset).

    With no filter supplied, no dimension imposes a constraint, so the matched
    set is the whole catalog.

    Validates: Requirements 5.5, 6.4
    """
    result = apply_query(catalog, EventQuery())
    assert _sorted(result) == _sorted(catalog)


# Feature: aws-events-mcp, Property 8: Absent filters do not exclude events
@settings(max_examples=100)
@given(catalog=catalogs(), dimension=st.sampled_from(sorted(_EMPTY_FILTER_VALUES)))
def test_empty_value_is_identity_over_its_dimension(catalog: list[Event], dimension: str) -> None:
    """Setting one dimension to an empty/None value does not reduce the result.

    For the chosen dimension, the matched set produced when that dimension is
    explicitly populated with an empty/``None`` value equals both the unset
    (default ``EventQuery``) result and the whole catalog, confirming the empty
    value is the identity over that dimension.

    Validates: Requirements 5.5, 6.4
    """
    unset_result = apply_query(catalog, EventQuery())
    empty_value_query = EventQuery(**{dimension: _EMPTY_FILTER_VALUES[dimension]})
    empty_value_result = apply_query(catalog, empty_value_query)

    # The empty value yields the same result as leaving the dimension unset...
    assert _sorted(empty_value_result) == _sorted(unset_result)
    # ...and that is the entire catalog (no dimension was reduced).
    assert _sorted(empty_value_result) == _sorted(catalog)


# Feature: aws-events-mcp, Property 8: Absent filters do not exclude events
@settings(max_examples=100)
@given(
    catalog=catalogs(),
    query=populated_queries(),
    dimension=st.sampled_from(sorted(_EMPTY_FILTER_VALUES)),
)
def test_clearing_a_dimension_yields_a_superset(
    catalog: list[Event], query: EventQuery, dimension: str
) -> None:
    """Clearing one dimension of a populated query never removes events.

    Starting from an arbitrary populated query, resetting exactly one dimension
    to its empty/``None`` value relaxes (or leaves unchanged) the constraints, so
    the cleared query's matched set is a superset, by ``event_id``, of the
    populated query's matched set. This shows an absent filter cannot exclude
    events even while other filters remain active.

    Validates: Requirements 5.5, 6.4
    """
    populated_ids = {event.event_id for event in apply_query(catalog, query)}

    cleared_query = query.model_copy(update={dimension: _EMPTY_FILTER_VALUES[dimension]})
    cleared_ids = {event.event_id for event in apply_query(catalog, cleared_query)}

    assert populated_ids <= cleared_ids
