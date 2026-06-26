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

"""Property-based test for the lenient catalog parser retain/skip behavior.

This module validates that :func:`aws_events_mcp.parser.parse_events` retains
exactly the valid records, skips every invalid or malformed record, never aborts
on a bad record, and emits one warning per skipped record. It generates mixed
collections of valid, invalid, and malformed raw records that are each tagged
with their expected validity, so the expected parsed/skipped counts are known
independently of the parser under test.

The generators are constructed so that every "valid" record is guaranteed to
satisfy the ``Event`` model constraints (using only keys the parser accepts and
values it normalizes to valid enums) and every "invalid"/"malformed" record is
guaranteed to be skipped (a missing required field, an empty required string, an
unrecognized enum value, an unparseable date, or a non-mapping entry).

Note (task 8.2 contract pinning): ``location_mode`` is **derived** by the parser
(from an explicit field, the catalog tags, the location text, or a default) and
so is never a cause for skipping a record. The genuinely required *input* fields
the parser can be missing are therefore ``event_id``, ``title``, and
``start_date``; the skip cases below exercise those plus the strict enum
(``learning_level``) and date validators.

Feature: aws-events-mcp, Property 14: Parser retains valid records and skips invalid ones
Validates: Requirements 10.1, 10.4, 10.5, 10.6
"""

from aws_events_mcp.models import Event
from aws_events_mcp.parser import parse_events
from hypothesis import given, settings
from hypothesis import strategies as st
from typing import Any


#: Location-mode strings the parser normalizes to a valid ``LocationMode``.
_VALID_MODE_VALUES = ('virtual', 'physical', 'online', 'in-person', 'venue', 'Virtual', 'PHYSICAL')

#: Learning-level strings the parser normalizes to a valid ``LearningLevel``.
_VALID_LEVEL_VALUES = (
    'Foundational',
    'Intermediate',
    'Advanced',
    'Expert',
    'advanced',
    '100',
    '200',
    '300',
    '400',
)

#: Sentinels guaranteed to be rejected by the model (never accepted/normalized).
_INVALID_LEVEL = 'definitely-not-a-valid-level'
_INVALID_DATE = 'definitely-not-a-date'


@st.composite
def valid_records(draw: st.DrawFn) -> dict:
    """Generate a raw record guaranteed to parse into a valid ``Event``.

    Uses only the canonical keys the parser accepts. Required fields are always
    present with valid values (non-empty identifier/title, an ISO date string,
    and a normalizable location mode); optional fields are independently present
    or absent with values that satisfy the model.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A raw record mapping that ``parse_events`` will retain.
    """
    record: dict = {
        'event_id': draw(st.text(min_size=1, max_size=40)),
        'title': draw(st.text(min_size=1, max_size=80)),
        'start_date': draw(st.dates()).isoformat(),
        'location_mode': draw(st.sampled_from(_VALID_MODE_VALUES)),
    }
    if draw(st.booleans()):
        record['description'] = draw(st.text(max_size=120))
    if draw(st.booleans()):
        record['learning_level'] = draw(st.sampled_from(_VALID_LEVEL_VALUES))
    if draw(st.booleans()):
        record['event_type'] = draw(st.text(min_size=1, max_size=40))
    if draw(st.booleans()):
        record['partner_name'] = draw(st.text(min_size=1, max_size=40))
    return record


@st.composite
def invalid_records(draw: st.DrawFn) -> dict:
    """Generate a raw record guaranteed to be skipped for an invalid value.

    Starts from a valid record and corrupts it in exactly one way: dropping a
    required input field (Requirement 10.4), emptying a required string, or
    supplying an unrecognized learning level or date (Requirement 10.5).
    ``location_mode`` is intentionally not corrupted because the parser derives
    it and never skips a record on its account (task 8.2).

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A raw record mapping that ``parse_events`` will skip with a warning.
    """
    record = draw(valid_records())
    kind = draw(
        st.sampled_from(
            (
                'missing_required',
                'empty_event_id',
                'empty_title',
                'bad_learning_level',
                'bad_start_date',
            )
        )
    )
    if kind == 'missing_required':
        key = draw(st.sampled_from(('event_id', 'title', 'start_date')))
        record.pop(key, None)
    elif kind == 'empty_event_id':
        record['event_id'] = ''
    elif kind == 'empty_title':
        record['title'] = ''
    elif kind == 'bad_learning_level':
        record['learning_level'] = _INVALID_LEVEL
    else:
        record['start_date'] = _INVALID_DATE
    return record


def malformed_records() -> st.SearchStrategy:
    """Build a strategy of non-mapping entries the parser treats as malformed.

    Returns:
        A strategy producing values that are not mappings (``None``, numbers,
        booleans, strings, and lists), each of which ``parse_events`` skips with
        a warning (Requirement 10.6).
    """
    return st.one_of(
        st.none(),
        st.integers(),
        st.floats(allow_nan=True, allow_infinity=True),
        st.booleans(),
        st.text(max_size=20),
        st.lists(st.integers(), max_size=4),
    )


def labeled_records() -> st.SearchStrategy:
    """Build a strategy of ``(record, is_valid)`` pairs across all record kinds.

    Returns:
        A strategy yielding a raw record paired with its expected validity, so a
        list of these pairs carries the known valid/skipped counts the test
        asserts against.
    """
    return st.one_of(
        valid_records().map(lambda record: (record, True)),
        invalid_records().map(lambda record: (record, False)),
        malformed_records().map(lambda record: (record, False)),
    )


# Feature: aws-events-mcp, Property 14: Parser retains valid records and skips invalid ones
@settings(max_examples=100)
@given(labeled=st.lists(labeled_records(), max_size=24))
def test_parser_retains_valid_and_skips_invalid(labeled: list[tuple[Any, bool]]) -> None:
    """Parsing retains every valid record, skips the rest, and warns per skip.

    Asserts one ``Event`` per valid record, every invalid/malformed record
    omitted, that parsing continues past skips without raising, and that exactly
    one warning is recorded per skipped record.

    Validates: Requirements 10.1, 10.4, 10.5, 10.6
    """
    records = [record for record, _ in labeled]
    expected_valid = sum(1 for _, is_valid in labeled if is_valid)
    expected_skipped = len(records) - expected_valid

    events, warnings = parse_events(records)

    # One Event per valid record; all invalid/malformed records omitted.
    assert len(events) == expected_valid
    assert all(isinstance(event, Event) for event in events)
    # A warning is recorded for every skipped record, and parsing continued.
    assert len(warnings) == expected_skipped
