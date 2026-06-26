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

"""Property-based test for walking pages reconstructing the full ordered result.

This module validates that paging through a matched set one page at a time
faithfully reconstructs the single unpaginated result. Starting at offset 0 and
repeatedly calling ``paginate(matched, page_size, offset)`` while feeding the
returned ``next_offset`` back in (the offset-level analogue of following a page
token) until ``next_offset`` is ``None`` SHALL yield, when the page items are
concatenated, exactly the full ordered ``apply_query`` result: same order, no
duplicates, no gaps, and no reordering.

Catalogs (including empty and >100 event catalogs), queries, and page sizes
spanning 1 to above the 100 cap are generated. The walk is bounded by a sane
iteration cap and the test asserts the walk terminates within it. It lives in
its own file so it can run in parallel with the other property tests.

Feature: aws-events-mcp, Property 10: Walking pages reconstructs the full ordered result
Validates: Requirements 3.5
"""

from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query, paginate
from hypothesis import given, settings
from hypothesis import strategies as st


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` instance with a constrained value space.

    Text fields draw from a small alphabet (mixed case, overlapping the query
    strategy's filter values) so generated queries match a non-trivial fraction
    of generated catalogs. ``start_date`` and ``event_id`` vary enough to
    exercise the ``(start_date, event_id)`` ordering and its tiebreak, which the
    page walk must preserve.

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
    queries match a meaningful range of catalogs rather than always producing an
    empty matched set.

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


# Feature: aws-events-mcp, Property 10: Walking pages reconstructs the full ordered result
@settings(max_examples=100)
@given(
    catalog=st.lists(events(), max_size=130),
    query=event_queries(),
    page_size=st.integers(min_value=1, max_value=150),
)
def test_walking_pages_reconstructs_full_ordered_result(
    catalog: list[Event], query: EventQuery, page_size: int
) -> None:
    """Following next_offset to the end rebuilds the unpaginated ordered result.

    Validates: Requirements 3.5
    """
    matched = apply_query(catalog, query)

    # Walk every page, following next_offset until none remains. The number of
    # pages can never exceed total_count + 1 (each non-final page yields at least
    # one item); use that as a sane termination cap and assert we stay under it.
    iteration_cap = len(matched) + 2
    walked: list[Event] = []
    offset: int | None = 0
    iterations = 0
    while offset is not None:
        assert iterations < iteration_cap, 'page walk did not terminate'
        page = paginate(matched, page_size, offset)
        walked.extend(page.items)
        offset = page.next_offset
        iterations += 1

    # Same order, no duplicates, no gaps, no reordering: the concatenation of
    # every page equals the single unpaginated matched result exactly.
    assert walked == matched

    # The walk visited each matched event exactly once (no duplicates).
    assert len(walked) == len(matched)
