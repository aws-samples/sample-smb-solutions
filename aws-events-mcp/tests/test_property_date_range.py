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

"""Property-based test for inclusive date-range bounds in the query engine.

This module validates that ``query.apply_query`` honours the inclusive date-range
filter: for any catalog and any valid ``(start, end)`` pair with ``start <=
end``, every returned event has a start date that is greater than or equal to the
start bound and less than or equal to the end bound. Boundary dates (events that
fall exactly on the start or end bound) are exercised explicitly to confirm both
bounds are inclusive rather than exclusive. It lives in its own file so it can run
in parallel with the other property tests.

Feature: aws-events-mcp, Property 4: Inclusive date-range bounds
Validates: Requirements 7.1, 7.2
"""

from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query
from datetime import date, timedelta
from hypothesis import given, settings
from hypothesis import strategies as st


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` instance.

    Required string fields (``event_id``, ``title``) are non-empty; ``start_date``
    is any valid calendar date; optional fields are independently present or
    absent. Only fields relevant to ordering and the date-range filter are
    meaningfully varied, but every field is populated to a valid value.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    optional_text = st.none() | st.text(max_size=40)
    return Event(
        event_id=draw(st.text(min_size=1, max_size=32)),
        title=draw(st.text(min_size=1, max_size=60)),
        description=draw(st.text(max_size=80)),
        start_date=draw(st.dates()),
        start_time=draw(st.none() | st.text(max_size=16)),
        time_zone=draw(st.none() | st.text(max_size=16)),
        location=draw(optional_text),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(optional_text),
        partner_name=draw(optional_text),
        registration_url=draw(optional_text),
        learn_more_url=draw(optional_text),
    )


@st.composite
def date_ranges(draw: st.DrawFn) -> tuple[date, date]:
    """Generate a valid ``(start, end)`` date pair with ``start <= end``.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A two-tuple ``(start, end)`` of calendar dates where ``start <= end``.
    """
    first = draw(st.dates())
    second = draw(st.dates())
    return (min(first, second), max(first, second))


# Feature: aws-events-mcp, Property 4: Inclusive date-range bounds
@settings(max_examples=100)
@given(catalog=st.lists(events(), max_size=40), bounds=date_ranges())
def test_date_range_bounds_are_inclusive(catalog: list[Event], bounds: tuple[date, date]) -> None:
    """Every returned event's start date lies within the inclusive bounds.

    Validates: Requirements 7.1, 7.2
    """
    start, end = bounds
    query = EventQuery(start_date=start, end_date=end)

    result = apply_query(catalog, query)

    for event in result:
        assert event.start_date >= start
        assert event.start_date <= end

    # Completeness: every catalog event inside the inclusive window is returned.
    expected = {e.event_id for e in catalog if start <= e.start_date <= end}
    assert {e.event_id for e in result} >= expected


# Feature: aws-events-mcp, Property 4: Inclusive date-range bounds
@settings(max_examples=100)
@given(start=st.dates(), span=st.integers(min_value=0, max_value=365))
def test_boundary_dates_are_included(start: date, span: int) -> None:
    """Events exactly on the start or end bound are included, not excluded.

    Validates: Requirements 7.1, 7.2
    """
    try:
        end = start + timedelta(days=span)
    except OverflowError:
        end = date.max

    on_start = Event(
        event_id='on-start',
        title='Starts on the lower bound',
        start_date=start,
        location_mode=LocationMode.VIRTUAL,
    )
    on_end = Event(
        event_id='on-end',
        title='Starts on the upper bound',
        start_date=end,
        location_mode=LocationMode.PHYSICAL,
    )

    query = EventQuery(start_date=start, end_date=end)
    result_ids = {e.event_id for e in apply_query([on_start, on_end], query)}

    assert 'on-start' in result_ids
    assert 'on-end' in result_ids
