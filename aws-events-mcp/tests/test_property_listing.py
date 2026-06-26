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

"""Property-based test for capped, ordered listing with an accurate total count.

This module validates that paginating the matched set produced by
``apply_query`` over any catalog (including the empty catalog and catalogs with
more than 100 events), any valid query, and any applied page size:

* returns at most ``min(page_size, 100)`` events on the first page;
* returns those events ordered by start date ascending with an ``event_id``
  tiebreak; and
* reports a ``total_count`` equal to the number of catalog events matching the
  query, independent of the page size.

It lives in its own file so it can run in parallel with the other property
tests.

Feature: aws-events-mcp, Property 2: Listing is capped, ordered, and reports an accurate total count
Validates: Requirements 2.1, 2.2, 2.3, 2.6, 4.2, 7.5
"""

from aws_events_mcp.consts import MAX_PAGE_SIZE
from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query, paginate
from hypothesis import given, settings
from hypothesis import strategies as st


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` instance with a constrained value space.

    Text fields draw from a small alphabet (mixed case, including values that
    overlap the query strategy's filter values) so generated queries match a
    non-trivial fraction of generated catalogs. ``start_date`` and ``event_id``
    vary enough to exercise the ``(start_date, event_id)`` ordering and its
    tiebreak.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    small_text = st.text(alphabet='abcXYZ ', max_size=8)
    optional_small_text = st.none() | small_text
    return Event(
        event_id=draw(st.text(alphabet='abc012', min_size=1, max_size=4)),
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
def event_queries(draw: st.DrawFn) -> EventQuery:
    """Generate a varied ``EventQuery`` instance.

    Each filter dimension is independently present or absent, drawing values
    from the same small alphabet used by the event strategy so the resulting
    queries match a meaningful range of catalogs rather than always producing
    an empty matched set.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid ``EventQuery`` instance.
    """
    optional_small_text = st.none() | st.text(alphabet='abcXYZ ', max_size=4)
    return EventQuery(
        keyword=draw(optional_small_text),
        learning_levels=draw(
            st.lists(st.sampled_from(list(LearningLevel)), max_size=4, unique=True)
        ),
        location_mode=draw(st.none() | st.sampled_from(list(LocationMode))),
        location_text=draw(optional_small_text),
        event_type=draw(optional_small_text),
        partner=draw(optional_small_text),
        start_date=draw(st.none() | st.dates()),
        end_date=draw(st.none() | st.dates()),
    )


def _count_matches(catalog: list[Event], query: EventQuery) -> int:
    """Return the number of catalog events matching the query, independently.

    This recomputes the matched count without pagination so the property's
    ``total_count`` assertion does not merely echo the implementation under
    test; it uses ``apply_query`` (the unpaginated matched set) as the source of
    truth for "events matching the query".

    Args:
        catalog: The full catalog of events.
        query: The query to match against.

    Returns:
        The count of matching events.
    """
    return len(apply_query(catalog, query))


# Feature: aws-events-mcp, Property 2: Listing is capped, ordered, and reports an accurate total count
@settings(max_examples=100)
@given(
    catalog=st.lists(events(), max_size=130),
    query=event_queries(),
    page_size=st.integers(min_value=1, max_value=150),
)
def test_listing_is_capped_ordered_and_counts_total(
    catalog: list[Event], query: EventQuery, page_size: int
) -> None:
    """Paginated listing is capped, ordered ascending, and counts all matches.

    Validates: Requirements 2.1, 2.2, 2.3, 2.6, 4.2, 7.5
    """
    matched = apply_query(catalog, query)
    page = paginate(matched, page_size, 0)

    # Capped at min(page_size, MAX_PAGE_SIZE).
    assert len(page.items) <= min(page_size, MAX_PAGE_SIZE)

    # Ordered by (start_date, event_id) ascending.
    keys = [(event.start_date, event.event_id) for event in page.items]
    assert keys == sorted(keys)

    # total_count equals the number of matching catalog events, independent of
    # the page size, and is non-negative.
    expected_total = _count_matches(catalog, query)
    assert page.total_count == expected_total
    assert page.total_count >= 0


# Feature: aws-events-mcp, Property 2: Listing is capped, ordered, and reports an accurate total count
@settings(max_examples=100)
@given(
    catalog=st.lists(events(), max_size=130),
    query=event_queries(),
    page_size_a=st.integers(min_value=1, max_value=150),
    page_size_b=st.integers(min_value=1, max_value=150),
)
def test_total_count_is_independent_of_page_size(
    catalog: list[Event], query: EventQuery, page_size_a: int, page_size_b: int
) -> None:
    """The reported total_count does not change with the applied page size.

    Validates: Requirements 2.3, 2.6, 4.2
    """
    matched = apply_query(catalog, query)
    page_a = paginate(matched, page_size_a, 0)
    page_b = paginate(matched, page_size_b, 0)
    assert page_a.total_count == page_b.total_count
