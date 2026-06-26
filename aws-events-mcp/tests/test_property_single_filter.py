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

"""Property-based tests for single-dimension filter soundness and completeness.

Each test exercises exactly one filter dimension of ``EventQuery`` (with every
other filter left empty) against a generated catalog and asserts two things
about ``query.apply_query``:

* SOUNDNESS - every event in the result satisfies the dimension's matching rule,
  re-implemented here as an independent oracle predicate.
* COMPLETENESS - every catalog event that satisfies the predicate appears in the
  result. ``apply_query`` returns the full ordered matched set (pagination is a
  separate concern), so soundness plus completeness is exactly set equality
  between the result and the oracle-matched events.

Generators draw event fields and filter values from small, overlapping alphabets
and fixed pools so the generated filter values both match and miss across the
100 examples; substring/exact filters also vary case to exercise the
case-insensitive matching rules.

Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
Validates: Requirements 5.1, 5.2, 6.1, 6.2, 6.3, 7.1, 7.2, 8.1, 8.2
"""

from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query
from collections.abc import Callable
from datetime import date
from hypothesis import given, settings
from hypothesis import strategies as st
from typing import Any


# A small pool of event types so a sampled filter value sometimes matches a
# catalog event's type exactly and sometimes does not.
EVENT_TYPES = ['Tech Talk', 'Webinar', 'Summit', 'Roadshow']

# Narrow alphabet shared by matchable free-text fields and the substring filter
# values, so generated filters hit and miss with reasonable frequency.
SUBSTRING_ALPHABET = 'abc '

# Date window for catalog events and date-range bounds.
MIN_DATE = date(2023, 1, 1)
MAX_DATE = date(2025, 12, 31)


@st.composite
def event_fields(draw: st.DrawFn) -> dict:
    """Generate the value fields of an event, excluding ``event_id``.

    Free-text fields that participate in substring filters draw from
    ``SUBSTRING_ALPHABET``; ``event_type`` is sampled from a fixed pool so exact
    matching is exercised; enum fields are optionally absent.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A mapping of event field names to generated values.
    """
    text = st.text(alphabet=SUBSTRING_ALPHABET, min_size=0, max_size=6)
    return {
        'title': draw(st.text(min_size=1, max_size=8)),
        'description': draw(st.text(max_size=12)),
        'start_date': draw(st.dates(min_value=MIN_DATE, max_value=MAX_DATE)),
        'location': draw(st.none() | text),
        'location_mode': draw(st.sampled_from(list(LocationMode))),
        'learning_level': draw(st.none() | st.sampled_from(list(LearningLevel))),
        'event_type': draw(st.none() | st.sampled_from(EVENT_TYPES)),
        'partner_name': draw(st.none() | text),
    }


@st.composite
def catalogs(draw: st.DrawFn) -> list[Event]:
    """Generate a catalog of events with unique identifiers.

    Identifiers are assigned positionally (``evt-0``, ``evt-1``, ...) so set
    comparison by ``event_id`` is unambiguous. Catalogs may be empty.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A list of distinct ``Event`` instances.
    """
    field_sets = draw(st.lists(event_fields(), max_size=12))
    return [Event(event_id=f'evt-{index}', **fields) for index, fields in enumerate(field_sets)]


def _random_case(draw: Callable[..., Any], value: str) -> str:
    """Randomly upper/lower/leave a string to exercise case-insensitive rules.

    Args:
        draw: Hypothesis draw callable.
        value: The base string to recase.

    Returns:
        ``value`` unchanged, uppercased, or lowercased.
    """
    transform = draw(st.sampled_from([str, str.upper, str.lower]))
    return transform(value)


def _assert_sound_and_complete(
    catalog: list[Event],
    query: EventQuery,
    predicate: Callable[[Event], bool],
) -> None:
    """Assert ``apply_query`` is sound and complete against an oracle predicate.

    Args:
        catalog: The catalog queried.
        query: The single-dimension query applied.
        predicate: Independent oracle for the dimension's matching rule.
    """
    result = apply_query(catalog, query)
    result_ids = {event.event_id for event in result}
    expected_ids = {event.event_id for event in catalog if predicate(event)}

    # SOUNDNESS: every returned event satisfies the predicate.
    for event in result:
        assert predicate(event), f'unsound: {event.event_id} returned but fails predicate'

    # COMPLETENESS: every matching catalog event is returned.
    assert expected_ids <= result_ids, 'incomplete: a matching event is missing from the result'

    # Together these are exactly set equality (ids are unique per catalog).
    assert result_ids == expected_ids


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(catalog=catalogs(), level=st.sampled_from(list(LearningLevel)))
def test_single_learning_level(catalog: list[Event], level: LearningLevel) -> None:
    """A single learning-level filter is sound and complete (case-insensitive exact).

    Validates: Requirements 5.1
    """
    query = EventQuery(learning_levels=[level])

    def predicate(event: Event) -> bool:
        return (
            event.learning_level is not None
            and event.learning_level.value.casefold() == level.value.casefold()
        )

    _assert_sound_and_complete(catalog, query, predicate)


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(
    catalog=catalogs(),
    levels=st.lists(st.sampled_from(list(LearningLevel)), min_size=2, max_size=4, unique=True),
)
def test_learning_level_set(catalog: list[Event], levels: list[LearningLevel]) -> None:
    """A 2-4 value learning-level set matches any supplied value (case-insensitive).

    Validates: Requirements 5.2
    """
    query = EventQuery(learning_levels=levels)
    wanted = {level.value.casefold() for level in levels}

    def predicate(event: Event) -> bool:
        return event.learning_level is not None and event.learning_level.value.casefold() in wanted

    _assert_sound_and_complete(catalog, query, predicate)


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(catalog=catalogs(), mode=st.sampled_from(list(LocationMode)))
def test_location_mode(catalog: list[Event], mode: LocationMode) -> None:
    """A location-mode filter returns only events of that exact mode.

    Validates: Requirements 6.1, 6.2
    """
    query = EventQuery(location_mode=mode)

    def predicate(event: Event) -> bool:
        return event.location_mode == mode

    _assert_sound_and_complete(catalog, query, predicate)


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(catalog=catalogs(), data=st.data())
def test_location_text(catalog: list[Event], data: st.DataObject) -> None:
    """A location-text filter is a case-insensitive substring match on location.

    Validates: Requirements 6.3
    """
    raw = data.draw(st.text(alphabet=SUBSTRING_ALPHABET, min_size=1, max_size=4))
    location_text = _random_case(data.draw, raw)
    query = EventQuery(location_text=location_text)

    def predicate(event: Event) -> bool:
        return event.location is not None and location_text.casefold() in event.location.casefold()

    _assert_sound_and_complete(catalog, query, predicate)


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(catalog=catalogs(), data=st.data())
def test_event_type(catalog: list[Event], data: st.DataObject) -> None:
    """An event-type filter is a case-insensitive exact match.

    Validates: Requirements 8.1
    """
    base = data.draw(st.sampled_from(EVENT_TYPES))
    event_type = _random_case(data.draw, base)
    query = EventQuery(event_type=event_type)

    def predicate(event: Event) -> bool:
        return (
            event.event_type is not None and event.event_type.casefold() == event_type.casefold()
        )

    _assert_sound_and_complete(catalog, query, predicate)


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(catalog=catalogs(), data=st.data())
def test_partner(catalog: list[Event], data: st.DataObject) -> None:
    """A partner filter is a case-insensitive substring match on the partner name.

    Validates: Requirements 8.2
    """
    raw = data.draw(st.text(alphabet=SUBSTRING_ALPHABET, min_size=1, max_size=4))
    partner = _random_case(data.draw, raw)
    query = EventQuery(partner=partner)

    def predicate(event: Event) -> bool:
        return (
            event.partner_name is not None and partner.casefold() in event.partner_name.casefold()
        )

    _assert_sound_and_complete(catalog, query, predicate)


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(catalog=catalogs(), start_date=st.dates(min_value=MIN_DATE, max_value=MAX_DATE))
def test_date_range_lower_bound(catalog: list[Event], start_date: date) -> None:
    """A start-date bound returns only events on or after it (inclusive).

    Validates: Requirements 7.1
    """
    query = EventQuery(start_date=start_date)

    def predicate(event: Event) -> bool:
        return event.start_date >= start_date

    _assert_sound_and_complete(catalog, query, predicate)


# Feature: aws-events-mcp, Property 3: Single-dimension filter soundness and completeness
@settings(max_examples=100)
@given(catalog=catalogs(), end_date=st.dates(min_value=MIN_DATE, max_value=MAX_DATE))
def test_date_range_upper_bound(catalog: list[Event], end_date: date) -> None:
    """An end-date bound returns only events on or before it (inclusive).

    Validates: Requirements 7.2
    """
    query = EventQuery(end_date=end_date)

    def predicate(event: Event) -> bool:
        return event.start_date <= end_date

    _assert_sound_and_complete(catalog, query, predicate)
