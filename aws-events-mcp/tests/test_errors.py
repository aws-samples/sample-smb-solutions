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

"""Unit tests for the structured error helpers and exception hierarchy.

These tests verify the response-shaping helpers in ``aws_events_mcp.errors``
produce exactly the keys documented in the design's "Standard response shapes"
and "Error Handling" sections, and that error/validation/not-found shapes carry
an empty ``items`` collection and a ``total_count`` of ``0`` where required so
no partial catalog data leaks into an error result (Requirement 11.5). They
also confirm each ``CatalogSourceError`` subtype maps to the expected
``error_type`` value.
"""

import pytest
from aws_events_mcp.errors import (
    ERROR_TYPE_SOURCE_PARTIAL,
    ERROR_TYPE_SOURCE_TIMEOUT,
    ERROR_TYPE_SOURCE_UNPARSEABLE,
    ERROR_TYPE_SOURCE_UNREACHABLE,
    ERROR_TYPE_VALIDATION,
    CatalogPartialParseError,
    CatalogSourceError,
    CatalogTimeoutError,
    CatalogUnparseableError,
    CatalogUnreachableError,
    not_found,
    source_error,
    validation_error,
)


class TestValidationError:
    """Tests for the ``validation_error`` helper."""

    def test_returns_documented_keys(self):
        """The response contains exactly the documented keys."""
        result = validation_error('page_size', 'must be between 1 and 100')
        assert set(result.keys()) == {
            'status',
            'error_type',
            'field',
            'message',
            'items',
            'total_count',
        }

    def test_status_and_error_type(self):
        """Status is 'error' and error_type is 'validation_error'."""
        result = validation_error('page_size', 'must be between 1 and 100')
        assert result['status'] == 'error'
        assert result['error_type'] == ERROR_TYPE_VALIDATION
        assert result['error_type'] == 'validation_error'

    def test_carries_field_and_message(self):
        """The supplied field and message are passed through unchanged."""
        result = validation_error('keyword', 'keyword must be 1 to 256 characters')
        assert result['field'] == 'keyword'
        assert result['message'] == 'keyword must be 1 to 256 characters'

    def test_carries_empty_items_and_zero_count(self):
        """No event records are returned with a validation error."""
        result = validation_error('page_size', 'must be between 1 and 100')
        assert result['items'] == []
        assert result['total_count'] == 0


class TestSourceError:
    """Tests for the ``source_error`` helper."""

    def test_returns_documented_keys(self):
        """The response contains exactly the documented keys."""
        result = source_error(ERROR_TYPE_SOURCE_UNREACHABLE, 'connection failed')
        assert set(result.keys()) == {
            'status',
            'error_type',
            'message',
            'items',
            'total_count',
        }

    def test_status_is_error(self):
        """Status is 'error' for source failures."""
        result = source_error(ERROR_TYPE_SOURCE_TIMEOUT, 'timed out')
        assert result['status'] == 'error'

    @pytest.mark.parametrize(
        'error_type',
        [
            ERROR_TYPE_SOURCE_UNREACHABLE,
            ERROR_TYPE_SOURCE_TIMEOUT,
            ERROR_TYPE_SOURCE_UNPARSEABLE,
            ERROR_TYPE_SOURCE_PARTIAL,
        ],
    )
    def test_error_type_passed_through(self, error_type):
        """The supplied error_type is returned unchanged."""
        result = source_error(error_type, 'some message')
        assert result['error_type'] == error_type

    def test_carries_message(self):
        """The supplied message is passed through unchanged."""
        result = source_error(ERROR_TYPE_SOURCE_UNPARSEABLE, 'could not interpret response')
        assert result['message'] == 'could not interpret response'

    def test_carries_empty_items_and_zero_count(self):
        """No partial catalog data is returned with a source error (Req 11.5)."""
        result = source_error(ERROR_TYPE_SOURCE_PARTIAL, 'partial parse')
        assert result['items'] == []
        assert result['total_count'] == 0


class TestNotFound:
    """Tests for the ``not_found`` helper."""

    def test_returns_documented_keys(self):
        """The response contains exactly the documented keys."""
        result = not_found("No event matched identifier 'abc123'.")
        assert set(result.keys()) == {'status', 'message', 'event'}

    def test_status_is_not_found(self):
        """Status is 'not_found'."""
        result = not_found("No event matched identifier 'abc123'.")
        assert result['status'] == 'not_found'

    def test_carries_message(self):
        """The supplied message is passed through unchanged."""
        message = "No event matched identifier 'abc123'."
        result = not_found(message)
        assert result['message'] == message

    def test_event_is_none(self):
        """No event record is returned with a not-found result."""
        result = not_found("No event matched identifier 'abc123'.")
        assert result['event'] is None


class TestCatalogSourceErrorTypes:
    """Tests for the ``CatalogSourceError`` hierarchy ``error_type`` mapping."""

    @pytest.mark.parametrize(
        ('exc_type', 'expected_error_type'),
        [
            (CatalogSourceError, ERROR_TYPE_SOURCE_UNPARSEABLE),
            (CatalogUnreachableError, ERROR_TYPE_SOURCE_UNREACHABLE),
            (CatalogTimeoutError, ERROR_TYPE_SOURCE_TIMEOUT),
            (CatalogUnparseableError, ERROR_TYPE_SOURCE_UNPARSEABLE),
            (CatalogPartialParseError, ERROR_TYPE_SOURCE_PARTIAL),
        ],
    )
    def test_error_type_attribute(self, exc_type, expected_error_type):
        """Each exception subtype carries the expected error_type attribute."""
        assert exc_type.error_type == expected_error_type
        assert exc_type().error_type == expected_error_type

    @pytest.mark.parametrize(
        'exc_type',
        [
            CatalogUnreachableError,
            CatalogTimeoutError,
            CatalogUnparseableError,
            CatalogPartialParseError,
        ],
    )
    def test_subtypes_inherit_from_base(self, exc_type):
        """Every catalog error subtype derives from CatalogSourceError."""
        assert issubclass(exc_type, CatalogSourceError)

    def test_raisable_and_catchable_as_base(self):
        """Subtypes can be caught as the common base class."""
        with pytest.raises(CatalogSourceError):
            raise CatalogTimeoutError('timed out')
