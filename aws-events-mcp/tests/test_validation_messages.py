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

"""Unit tests for corrective validation-message wording.

These tests pin the exact corrective wording the server returns for
representative invalid inputs, so the usability guarantee that every validation
error states how to correct the input (NFR Usability) is verified rather than
merely asserted by the design. They target the real validation helpers in
``aws_events_mcp.server`` (``build_event_query``, ``_require_event_id``) and the
``validation_response`` shaper, asserting against the messages those helpers
actually raise.

Page-size range note
--------------------
``page_size`` is constrained at the tool input-schema level
(``pydantic.Field(ge=1, le=100)``) rather than inside ``build_event_query``, so
there is no ``build_event_query`` code path that raises a page-size message to
assert against. The most faithful target for the page-size corrective wording is
therefore the shared argument description ``_DESC_PAGE_SIZE`` that documents the
accepted 1-100 range to MCP clients; this test asserts that description states
the range. (Validates: Requirements 5.4, 6.6, 7.6, 7.7, 9.5, NFR Usability.)
"""

import pytest
from aws_events_mcp.errors import ERROR_TYPE_VALIDATION
from aws_events_mcp.server import (
    _ACCEPTED_LEARNING_LEVELS,
    _ACCEPTED_LOCATION_MODES,
    _DESC_PAGE_SIZE,
    ToolValidationError,
    _require_event_id,
    build_event_query,
    validation_response,
)
from typing import Any, Dict


class TestLearningLevelMessage:
    """Learning-level enumeration listing (Requirement 5.4)."""

    def test_lists_all_four_accepted_values(self):
        """The message names every accepted learning level."""
        with pytest.raises(ToolValidationError) as exc_info:
            build_event_query(learning_level='Novice')
        message = exc_info.value.message
        for accepted in ('Foundational', 'Intermediate', 'Advanced', 'Expert'):
            assert accepted in message

    def test_field_and_invalid_wording(self):
        """The error targets learning_level and flags the value as invalid."""
        with pytest.raises(ToolValidationError) as exc_info:
            build_event_query(learning_level='Novice')
        assert exc_info.value.field == 'learning_level'
        assert 'invalid' in exc_info.value.message.lower()

    def test_accepted_values_constant_matches_message(self):
        """The accepted-values constant lists exactly the four levels."""
        assert _ACCEPTED_LEARNING_LEVELS == 'Foundational, Intermediate, Advanced, Expert'


class TestLocationModeMessage:
    """Location-mode listing (Requirement 6.6)."""

    def test_lists_virtual_and_physical(self):
        """The message names both accepted location modes."""
        with pytest.raises(ToolValidationError) as exc_info:
            build_event_query(location_mode='hybrid')
        message = exc_info.value.message
        assert 'virtual' in message
        assert 'physical' in message

    def test_field_and_invalid_wording(self):
        """The error targets location_mode and flags the value as invalid."""
        with pytest.raises(ToolValidationError) as exc_info:
            build_event_query(location_mode='hybrid')
        assert exc_info.value.field == 'location_mode'
        assert 'invalid' in exc_info.value.message.lower()

    def test_accepted_values_constant(self):
        """The accepted-values constant lists exactly virtual and physical."""
        assert _ACCEPTED_LOCATION_MODES == 'virtual, physical'


class TestDateFormatMessage:
    """Date format message (Requirement 7.6)."""

    @pytest.mark.parametrize('bad_date', ['2024/01/01', '01-01-2024', 'not-a-date', '2024-13-40'])
    def test_states_expected_format(self, bad_date):
        """A non-ISO start date yields a message naming the YYYY-MM-DD format."""
        with pytest.raises(ToolValidationError) as exc_info:
            build_event_query(start_date=bad_date)
        assert 'YYYY-MM-DD' in exc_info.value.message
        assert exc_info.value.field == 'start_date'

    def test_end_date_format_message(self):
        """A non-ISO end date is reported against the end_date field."""
        with pytest.raises(ToolValidationError) as exc_info:
            build_event_query(end_date='31/12/2024')
        assert 'YYYY-MM-DD' in exc_info.value.message
        assert exc_info.value.field == 'end_date'


class TestStartAfterEndMessage:
    """Start-after-end message (Requirement 7.7)."""

    def test_states_start_not_later_than_end(self):
        """A start later than end yields the ordering corrective message."""
        with pytest.raises(ToolValidationError) as exc_info:
            build_event_query(start_date='2024-12-31', end_date='2024-01-01')
        assert 'not be later than' in exc_info.value.message
        assert exc_info.value.field == 'start_date'


class TestEventIdMessage:
    """Empty/whitespace event identifier (Requirement 9.5)."""

    @pytest.mark.parametrize('event_id', ['', '   ', '\t', '\n  '])
    def test_blank_identifier_requires_valid_identifier(self, event_id):
        """A blank identifier reports that a valid identifier is required."""
        with pytest.raises(ToolValidationError) as exc_info:
            _require_event_id(event_id)
        message = exc_info.value.message.lower()
        assert 'valid event identifier is required' in message
        assert exc_info.value.field == 'event_id'

    def test_valid_identifier_is_trimmed(self):
        """A non-blank identifier is accepted and surrounding whitespace trimmed."""
        assert _require_event_id('  evt-123  ') == 'evt-123'


class TestPageSizeRange:
    """Page-size range guidance (Requirement 3.6, documented via NFR Usability).

    ``page_size`` is enforced at the tool input-schema level
    (``Field(ge=1, le=100)``), so the faithful assertion target is the shared
    argument description that documents the accepted range to clients.
    """

    def test_description_states_one_to_hundred_range(self):
        """The page-size description states the inclusive 1-100 range."""
        assert '1 and 100' in _DESC_PAGE_SIZE
        assert 'inclusive' in _DESC_PAGE_SIZE


class TestValidationResponseShape:
    """``validation_response`` carries the corrective message and no records."""

    def test_wraps_message_field_and_excludes_records(self):
        """The structured response echoes field/message with no event records."""
        response: Dict[str, Any] = {}
        try:
            build_event_query(learning_level='Novice')
        except ToolValidationError as exc:
            response = validation_response(exc)
        assert response['status'] == 'error'
        assert response['error_type'] == ERROR_TYPE_VALIDATION
        assert response['field'] == 'learning_level'
        assert 'invalid' in response['message'].lower()
        assert response['items'] == []
        assert response['total_count'] == 0
