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

"""Property-based test for date-argument validation in the server tool layer.

This module validates that the shared query builder ``build_event_query``
rejects malformed and inconsistent date arguments and that the resulting
``ToolValidationError`` shapes a structured validation-error response carrying
no event records:

* a ``start_date`` or ``end_date`` that does not conform to the ISO 8601
  calendar format ``YYYY-MM-DD`` (wrong separators, non-numeric components,
  out-of-range month/day, empty, or partial) raises a validation error
  (Requirement 7.6); and
* a valid ``(start_date, end_date)`` pair where the start date is later than the
  end date raises a validation error stating the start must not be later than the
  end (Requirement 7.7).

In both cases ``validation_response`` produces a ``validation_error`` dict with an
empty ``items`` list and a ``total_count`` of ``0`` (no records). As a sanity
check, a valid pair with ``start <= end`` raises no error. It lives in its own
file so it can run in parallel with the other property tests.

Feature: aws-events-mcp, Property 13: Date arguments are validated
Validates: Requirements 7.6, 7.7
"""

import re
from aws_events_mcp.errors import ERROR_TYPE_VALIDATION
from aws_events_mcp.server import (
    ToolValidationError,
    build_event_query,
    validation_response,
)
from datetime import date
from hypothesis import assume, given, settings
from hypothesis import strategies as st


_ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _is_valid_iso_date(value: str) -> bool:
    """Return whether ``value`` is a valid ``YYYY-MM-DD`` calendar date.

    Mirrors the acceptance check in ``server._parse_date_argument`` (regex shape
    plus ``date.fromisoformat``), so a generated candidate can be confirmed
    genuinely invalid before it is asserted to be rejected.

    Args:
        value: The candidate date string.

    Returns:
        ``True`` when ``value`` matches ``YYYY-MM-DD`` and is a real calendar
        date; ``False`` otherwise.
    """
    if not _ISO_DATE_PATTERN.match(value):
        return False
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _assert_validation_no_records(exc: ToolValidationError, expected_field: str) -> None:
    """Assert the error shapes a validation response with no records.

    Args:
        exc: The raised ``ToolValidationError``.
        expected_field: The argument name expected on the error and response.
    """
    assert exc.field == expected_field
    assert exc.message

    response = validation_response(exc)
    assert response['status'] == 'error'
    assert response['error_type'] == ERROR_TYPE_VALIDATION
    assert response['field'] == expected_field
    assert response['message'] == exc.message
    # No catalog data is ever returned in a validation error (Requirement 11.5).
    assert response['items'] == []
    assert response['total_count'] == 0


# Candidate malformed date strings: wrong separators, non-numeric components,
# out-of-range month/day, empty, and partial forms, plus arbitrary free text.
# Every candidate is guarded by ``assume(not _is_valid_iso_date(...))`` so any
# value that happens to be a real YYYY-MM-DD date is skipped.
_malformed_dates = st.one_of(
    st.sampled_from(
        [
            '',
            ' ',
            '2021',
            '2021-01',
            '2021-1-1',
            '21-01-01',
            '2021/01/01',
            '2021.01.01',
            '2021_01_01',
            '01-01-2021',
            '2021-13-01',
            '2021-00-10',
            '2021-02-30',
            '2021-12-32',
            '2021-12-00',
            'abcd-ef-gh',
            'not-a-date',
            '2021-Jan-01',
            '2021-01-01T00:00:00',
        ]
    ),
    st.text(max_size=12),
    # Structured-but-invalid numeric triples (often out-of-range or wrong width).
    st.builds(
        '{:04d}-{:02d}-{:02d}'.format,
        st.integers(min_value=0, max_value=9999),
        st.integers(min_value=0, max_value=99),
        st.integers(min_value=0, max_value=99),
    ),
)


# Feature: aws-events-mcp, Property 13: Date arguments are validated
@settings(max_examples=100)
@given(value=_malformed_dates, field=st.sampled_from(['start_date', 'end_date']))
def test_malformed_date_argument_is_rejected(value: str, field: str) -> None:
    """A non-``YYYY-MM-DD`` date argument yields a validation error, no records.

    Validates: Requirements 7.6
    """
    assume(not _is_valid_iso_date(value))

    kwargs = {field: value}
    try:
        build_event_query(**kwargs)
    except ToolValidationError as exc:
        _assert_validation_no_records(exc, field)
    else:
        raise AssertionError(f'Expected ToolValidationError for {field}={value!r}')


# Feature: aws-events-mcp, Property 13: Date arguments are validated
@settings(max_examples=100)
@given(first=st.dates(), second=st.dates())
def test_start_after_end_is_rejected(first: date, second: date) -> None:
    """A start date later than the end date yields a validation error, no records.

    Validates: Requirements 7.7
    """
    start = max(first, second)
    end = min(first, second)
    assume(start > end)

    try:
        build_event_query(start_date=start.isoformat(), end_date=end.isoformat())
    except ToolValidationError as exc:
        _assert_validation_no_records(exc, 'start_date')
        assert 'must not be later than the end date' in exc.message
    else:
        raise AssertionError(f'Expected ToolValidationError for start={start} > end={end}')


# Feature: aws-events-mcp, Property 13: Date arguments are validated
@settings(max_examples=100)
@given(first=st.dates(), second=st.dates())
def test_valid_ordered_dates_are_accepted(first: date, second: date) -> None:
    """A valid YYYY-MM-DD pair with start <= end raises no error (sanity).

    Validates: Requirements 7.6, 7.7
    """
    start = min(first, second)
    end = max(first, second)

    query = build_event_query(start_date=start.isoformat(), end_date=end.isoformat())

    assert query.start_date == start
    assert query.end_date == end
