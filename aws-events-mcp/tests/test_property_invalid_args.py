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

"""Property-based tests that invalid arguments are rejected with no records.

Property 12 asserts that every invalid tool argument is rejected with a
structured validation error that carries no event records. Invalid arguments
fall into two enforcement tiers in this server (see the design's "Field
validation conventions"):

* Semantic / cross-field rules the JSON schema cannot express are enforced by
  the in-body validators behind :func:`aws_events_mcp.server.build_event_query`,
  which raise :class:`~aws_events_mcp.server.ToolValidationError`. The tool layer
  converts that into the structured response via
  :func:`~aws_events_mcp.server.validation_response`. These cases (invalid
  learning levels, invalid location mode, and blank / whitespace-only /
  over-length bounded string filters) are exercised directly against
  ``build_event_query`` and ``validation_response`` so the property holds without
  the MCP transport layer.

* Pure schema constraints (``page_size`` must be an integer in 1-100) are
  enforced by ``Field(ge=1, le=100)`` on the tool signatures, *before* any tool
  body runs. There is no in-body ``page_size`` validator to call, so these cases
  are exercised through ``mcp.call_tool``: an out-of-range or non-integer
  ``page_size`` raises a ``ToolError`` at the schema boundary, meaning the tool
  returns no records at all.

Every constructed validation-error dict is checked to carry
``error_type == 'validation_error'``, ``items == []``, and ``total_count == 0``.

Feature: aws-events-mcp, Property 12: Invalid arguments are rejected with a validation error and no records
Validates: Requirements 1.6, 3.6, 4.4, 5.4, 6.6, 6.7, 8.5
"""

import asyncio
import pytest
from aws_events_mcp.models import LearningLevel, LocationMode
from aws_events_mcp.server import (
    ToolValidationError,
    build_event_query,
    mcp,
    validation_response,
)
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from mcp.server.fastmcp.exceptions import ToolError
from typing import Any, Dict


# Case-insensitive accepted enum values; generated invalid values avoid these.
_ACCEPTED_LEARNING_LEVELS = {level.value.casefold() for level in LearningLevel}
_ACCEPTED_LOCATION_MODES = {mode.value.casefold() for mode in LocationMode}

# Per-field maximum lengths for the bounded free-text filters (design: Field
# validation conventions). Over-length values exceed these by at least one char.
_BOUNDED_STRING_LIMITS = {
    'keyword': 256,
    'location_text': 200,
    'partner': 256,
    'event_type': 256,
}

# Whitespace characters used to build blank / whitespace-only filter values.
_WHITESPACE = ' \t\n\r\x0b\x0c'


def _assert_no_record_validation_error(response: Dict[str, Any], expected_field: str) -> None:
    """Assert a response is a validation error naming ``expected_field`` with no records.

    Args:
        response: The structured response produced by ``validation_response``.
        expected_field: The argument name expected in the ``field`` slot.
    """
    assert response['status'] == 'error'
    assert response['error_type'] == 'validation_error'
    assert response['field'] == expected_field
    assert response['items'] == []
    assert response['total_count'] == 0


# --- Invalid learning levels -------------------------------------------------


# Feature: aws-events-mcp, Property 12: Invalid arguments are rejected with a validation error and no records
@settings(max_examples=100)
@given(value=st.text(min_size=1, max_size=24))
def test_invalid_learning_level_scalar(value: str) -> None:
    """A learning level not matching the four accepted values is rejected, no records.

    Validates: Requirements 5.4
    """
    assume(value.strip().casefold() not in _ACCEPTED_LEARNING_LEVELS)
    with pytest.raises(ToolValidationError) as exc_info:
        build_event_query(learning_level=value)
    assert exc_info.value.field == 'learning_level'
    _assert_no_record_validation_error(validation_response(exc_info.value), 'learning_level')


# Feature: aws-events-mcp, Property 12: Invalid arguments are rejected with a validation error and no records
@settings(max_examples=100)
@given(
    bad=st.text(min_size=1, max_size=24),
    valid=st.sampled_from([level.value for level in LearningLevel]),
)
def test_invalid_learning_level_in_list(bad: str, valid: str) -> None:
    """A list containing one invalid learning level is rejected, no records.

    Validates: Requirements 5.4
    """
    assume(bad.strip().casefold() not in _ACCEPTED_LEARNING_LEVELS)
    with pytest.raises(ToolValidationError) as exc_info:
        build_event_query(learning_level=[valid, bad])
    assert exc_info.value.field == 'learning_level'
    _assert_no_record_validation_error(validation_response(exc_info.value), 'learning_level')


# --- Invalid location mode ---------------------------------------------------


# Feature: aws-events-mcp, Property 12: Invalid arguments are rejected with a validation error and no records
@settings(max_examples=100)
@given(value=st.text(min_size=1, max_size=24))
def test_invalid_location_mode(value: str) -> None:
    """A location mode other than virtual/physical is rejected, no records.

    Validates: Requirements 6.6
    """
    assume(value.strip().casefold() not in _ACCEPTED_LOCATION_MODES)
    with pytest.raises(ToolValidationError) as exc_info:
        build_event_query(location_mode=value)
    assert exc_info.value.field == 'location_mode'
    _assert_no_record_validation_error(validation_response(exc_info.value), 'location_mode')


# --- Blank / whitespace-only bounded string filters --------------------------


# Feature: aws-events-mcp, Property 12: Invalid arguments are rejected with a validation error and no records
@settings(max_examples=100)
@given(
    field=st.sampled_from(sorted(_BOUNDED_STRING_LIMITS)),
    value=st.text(alphabet=_WHITESPACE, min_size=0, max_size=8),
)
def test_blank_bounded_filter_rejected(field: str, value: str) -> None:
    """An empty or whitespace-only bounded string filter is rejected, no records.

    Validates: Requirements 4.4, 6.7, 8.5
    """
    with pytest.raises(ToolValidationError) as exc_info:
        build_event_query(**{field: value})
    assert exc_info.value.field == field
    _assert_no_record_validation_error(validation_response(exc_info.value), field)


# --- Over-length bounded string filters --------------------------------------


# Feature: aws-events-mcp, Property 12: Invalid arguments are rejected with a validation error and no records
@settings(max_examples=100)
@given(field=st.sampled_from(sorted(_BOUNDED_STRING_LIMITS)), data=st.data())
def test_overlength_bounded_filter_rejected(field: str, data: st.DataObject) -> None:
    """A bounded string filter exceeding its length limit is rejected, no records.

    The limit is 256 characters for keyword/partner/event_type and 200 for
    location_text; generated values exceed the field's limit by at least one
    character.

    Validates: Requirements 4.4, 6.7, 8.5
    """
    limit = _BOUNDED_STRING_LIMITS[field]
    value = data.draw(st.text(min_size=limit + 1, max_size=limit + 16))
    with pytest.raises(ToolValidationError) as exc_info:
        build_event_query(**{field: value})
    assert exc_info.value.field == field
    _assert_no_record_validation_error(validation_response(exc_info.value), field)


# --- Invalid page_size (schema-level boundary) -------------------------------
# page_size carries no in-body validator; the Field(ge=1, le=100) constraint on
# the tool signatures rejects out-of-range / non-integer values before the tool
# body runs. Exercising the tool through mcp.call_tool confirms such inputs are
# rejected at the schema boundary and therefore return no records at all.


# Feature: aws-events-mcp, Property 12: Invalid arguments are rejected with a validation error and no records
@settings(max_examples=100)
@given(
    page_size=st.one_of(
        st.integers(max_value=0),
        st.integers(min_value=101),
        st.floats(min_value=1.0, max_value=100.0).filter(lambda f: f != int(f)),
        st.text(alphabet='abcXYZ', min_size=1, max_size=6),
    )
)
def test_invalid_page_size_rejected_at_schema(page_size: object) -> None:
    """A non-integer or out-of-range page_size is rejected, returning no records.

    Validates: Requirements 3.6
    """
    with pytest.raises(ToolError):
        asyncio.run(mcp.call_tool('list_events', {'page_size': page_size}))
