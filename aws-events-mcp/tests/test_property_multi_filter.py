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

"""Property-based test for conjunctive multi-filter combination.

This module validates that when an ``EventQuery`` carries multiple populated
filters, ``query.apply_query`` combines them with logical AND: every event in
the result satisfies *every* supplied filter predicate (soundness) and every
catalog event that satisfies *all* supplied predicates appears in the
unpaginated matched set (completeness, since ``apply_query`` does not paginate).

The per-predicate matching rules are independently re-derived in this test (the
private ``query._matches*`` helpers are deliberately not imported) so the test
is an independent oracle for ``apply_query`` rather than a restatement of it. To
keep filter match rates meaningful, event fields and filter values are drawn
from a shared, constrained vocabulary so conjunctions select non-trivial
subsets. It lives in its own file so it can run in parallel with the other
property tests.

Feature: aws-events-mcp, Property 6: Multiple filters combine conjunctively
Validates: Requirements 6.5, 8.3
"""

from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query
from datetime import date
from hypothesis import given, settings
from hypothesis import strategies as st


# Shared, constrained vocabulary so substring/exact filters select non-trivial
# subsets of the generated catalog (mixed case exercises case-insensitivity).
_WORDS = ['AWS', 'cloud', 'Data', 'security', 'Summit', 'partner', 'serverless', 'ML']
_EVENT_TYPES = ['Tech Talk', 'webinar', 'Summit', 'roadshow', 'networking']
_PARTNERS = ['Acme', 'Globex', 'Initech', 'Umbrella']
_LOCATIONS = ['Seattle', 'New York', 'London', 'Online', 'Tokyo']
# A narrow date window so date-range bounds intersect the catalog meaningfully.
_MIN_DATE = date(2024, 1, 1)
_MAX_DATE = date(2024, 12, 31)


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` drawn from the shared vocabulary.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` whose text fields reuse the shared vocabulary
        so filter predicates select meaningful subsets.
    """
    title = ' '.join(draw(st.lists(st.sampled_from(_WORDS), min_size=1, max_size=4)))
    description = ' '.join(draw(st.lists(st.sampled_from(_WORDS), max_size=5)))
    return Event(
        event_id=draw(st.text(min_size=1, max_size=12)),
        title=title or 'AWS',
        description=description,
        start_date=draw(st.dates(min_value=_MIN_DATE, max_value=_MAX_DATE)),
        location=draw(st.none() | st.sampled_from(_LOCATIONS)),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(st.none() | st.sampled_from(_EVENT_TYPES)),
        partner_name=draw(st.none() | st.sampled_from(_PARTNERS)),
    )


# Names of the optional filter dimensions of an EventQuery.
_FILTER_NAMES = [
    'learning_levels',
    'location_mode',
    'location_text',
    'event_type',
    'partner',
    'keyword',
    'date_range',
]


@st.composite
def multi_filter_queries(draw: st.DrawFn) -> EventQuery:
    """Generate an ``EventQuery`` with at least two populated filters.

    A subset of size >= 2 of the filter dimensions is chosen and each chosen
    dimension is populated with a value drawn from the shared vocabulary (or a
    date within the catalog window). Values are intentionally lower/upper-cased
    differently from the event vocabulary to exercise case-insensitive matching.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        An ``EventQuery`` with two or more populated filter dimensions.
    """
    chosen = draw(
        st.lists(st.sampled_from(_FILTER_NAMES), min_size=2, unique=True).filter(
            lambda names: len(names) >= 2
        )
    )
    kwargs: dict = {}
    if 'learning_levels' in chosen:
        kwargs['learning_levels'] = draw(
            st.lists(st.sampled_from(list(LearningLevel)), min_size=1, max_size=4, unique=True)
        )
    if 'location_mode' in chosen:
        kwargs['location_mode'] = draw(st.sampled_from(list(LocationMode)))
    if 'location_text' in chosen:
        kwargs['location_text'] = draw(st.sampled_from(_LOCATIONS)).lower()
    if 'event_type' in chosen:
        kwargs['event_type'] = draw(st.sampled_from(_EVENT_TYPES)).upper()
    if 'partner' in chosen:
        kwargs['partner'] = draw(st.sampled_from(_PARTNERS)).lower()
    if 'keyword' in chosen:
        kwargs['keyword'] = draw(st.sampled_from(_WORDS)).upper()
    if 'date_range' in chosen:
        lo = draw(st.dates(min_value=_MIN_DATE, max_value=_MAX_DATE))
        hi = draw(st.dates(min_value=lo, max_value=_MAX_DATE))
        kwargs['start_date'] = lo
        kwargs['end_date'] = hi
    return EventQuery(**kwargs)


def _satisfies(event: Event, query: EventQuery) -> bool:
    """Independently re-derive whether an event satisfies every supplied filter.

    This reimplements the per-predicate matching rules from the design (it does
    not import the private ``query`` helpers) so it can serve as an independent
    oracle. An absent or empty filter dimension imposes no constraint.

    Args:
        event: The event under test.
        query: The query whose populated dimensions are checked conjunctively.

    Returns:
        ``True`` only when the event satisfies all supplied filter predicates.
    """
    # Learning level: case-insensitive exact match against any supplied value.
    if query.learning_levels:
        if event.learning_level is None:
            return False
        event_level = event.learning_level.value.casefold()
        if not any(level.value.casefold() == event_level for level in query.learning_levels):
            return False
    # Location mode: exact equality.
    if query.location_mode is not None and event.location_mode != query.location_mode:
        return False
    # Location text: case-insensitive substring of the location field.
    if query.location_text:
        if event.location is None:
            return False
        if query.location_text.casefold() not in event.location.casefold():
            return False
    # Event type: case-insensitive exact match.
    if query.event_type:
        if event.event_type is None:
            return False
        if event.event_type.casefold() != query.event_type.casefold():
            return False
    # Partner: case-insensitive substring of the partner name.
    if query.partner:
        if event.partner_name is None:
            return False
        if query.partner.casefold() not in event.partner_name.casefold():
            return False
    # Keyword: case-insensitive substring of the title or description.
    if query.keyword:
        needle = query.keyword.casefold()
        if needle not in event.title.casefold() and needle not in event.description.casefold():
            return False
    # Date range: inclusive bounds on the start date.
    if query.start_date is not None and event.start_date < query.start_date:
        return False
    if query.end_date is not None and event.start_date > query.end_date:
        return False
    return True


# Feature: aws-events-mcp, Property 6: Multiple filters combine conjunctively
@settings(max_examples=100)
@given(
    catalog=st.lists(events(), max_size=60),
    query=multi_filter_queries(),
)
def test_multiple_filters_combine_conjunctively(catalog: list[Event], query: EventQuery) -> None:
    """apply_query returns exactly the events satisfying every supplied filter.

    Soundness: every event in the result satisfies every supplied predicate.
    Completeness: every catalog event satisfying all predicates is in the result
    (no pagination is applied by ``apply_query``).

    Validates: Requirements 6.5, 8.3
    """
    result = apply_query(catalog, query)

    # Soundness: each returned event satisfies the conjunction of all filters.
    for event in result:
        assert _satisfies(event, query)

    # Completeness: the matched set equals every catalog event that satisfies
    # all predicates (compared as multisets, independent of ordering).
    expected = [event for event in catalog if _satisfies(event, query)]
    assert sorted(result, key=lambda e: (e.start_date, e.event_id)) == sorted(
        expected, key=lambda e: (e.start_date, e.event_id)
    )
