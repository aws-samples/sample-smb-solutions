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

"""Property-based test that error responses carry no catalog data.

Property 17 asserts that every error result the server can return -- a source
that is unreachable, timed out, or wholly unparseable, as well as an
input-validation failure -- carries an empty item collection and a total count
of zero, so no partial or unparsed catalog data ever leaks into an error result
(Requirements 2.5, 11.5).

The property is exercised against the real response-shaping helpers rather than a
reimplementation:

* Source errors are produced by constructing each ``CatalogSourceError`` subtype
  (:class:`~aws_events_mcp.errors.CatalogUnreachableError`,
  :class:`~aws_events_mcp.errors.CatalogTimeoutError`, and
  :class:`~aws_events_mcp.errors.CatalogUnparseableError`) with a generated
  message and mapping it through
  :func:`~aws_events_mcp.server.build_source_error_response`, which routes on the
  exception's ``error_type`` exactly as the tool layer does. A partial parse is
  no longer an error path (the successfully parsed events are returned), so it is
  not exercised here.
* Validation errors are produced both directly via
  :func:`~aws_events_mcp.errors.validation_error` and through the tool-layer path
  :func:`~aws_events_mcp.server.validation_response` over a
  :class:`~aws_events_mcp.server.ToolValidationError`, using a generated field
  name and message.

For any generated message/field, each resulting response is asserted to have
``status == 'error'``, ``items == []``, and ``total_count == 0``.

It lives in its own file so it can run in parallel with the other property tests.

Feature: aws-events-mcp, Property 17: Error responses carry no catalog data
Validates: Requirements 2.5, 11.5
"""

from aws_events_mcp.errors import (
    CatalogSourceError,
    CatalogTimeoutError,
    CatalogUnparseableError,
    CatalogUnreachableError,
    validation_error,
)
from aws_events_mcp.server import (
    ToolValidationError,
    build_source_error_response,
    validation_response,
)
from hypothesis import given, settings
from hypothesis import strategies as st
from typing import Any, Dict, Type


#: The ``CatalogSourceError`` subtypes whose mapped responses must be empty. A
#: partial parse is no longer an error path (the successfully parsed events are
#: returned), so ``CatalogPartialParseError`` is not included here.
_SOURCE_ERROR_TYPES = [
    CatalogUnreachableError,
    CatalogTimeoutError,
    CatalogUnparseableError,
]


def _assert_error_carries_no_data(response: Dict[str, Any]) -> None:
    """Assert an error response excludes all catalog data.

    Args:
        response: A structured error response produced by a response-shaping
            helper.
    """
    assert response['status'] == 'error'
    assert response['items'] == []
    assert response['total_count'] == 0


# --- Source errors -----------------------------------------------------------


# Feature: aws-events-mcp, Property 17: Error responses carry no catalog data
@settings(max_examples=100)
@given(
    error_cls=st.sampled_from(_SOURCE_ERROR_TYPES),
    message=st.text(max_size=128),
)
def test_source_error_response_carries_no_data(
    error_cls: Type[CatalogSourceError], message: str
) -> None:
    """Every source-error subtype maps to an empty error response.

    Validates: Requirements 2.5, 11.5
    """
    response = build_source_error_response(error_cls(message))
    _assert_error_carries_no_data(response)
    # The response routes on the subtype's declared error_type.
    assert response['error_type'] == error_cls.error_type


# --- Validation errors -------------------------------------------------------


# Feature: aws-events-mcp, Property 17: Error responses carry no catalog data
@settings(max_examples=100)
@given(
    field=st.text(min_size=1, max_size=32),
    message=st.text(max_size=128),
)
def test_validation_error_response_carries_no_data(field: str, message: str) -> None:
    """A validation error built directly carries no records.

    Validates: Requirements 2.5, 11.5
    """
    response = validation_error(field, message)
    _assert_error_carries_no_data(response)
    assert response['error_type'] == 'validation_error'
    assert response['field'] == field


# Feature: aws-events-mcp, Property 17: Error responses carry no catalog data
@settings(max_examples=100)
@given(
    field=st.text(min_size=1, max_size=32),
    message=st.text(max_size=128),
)
def test_validation_response_carries_no_data(field: str, message: str) -> None:
    """A validation error routed through the tool-layer path carries no records.

    Validates: Requirements 2.5, 11.5
    """
    response = validation_response(ToolValidationError(field, message))
    _assert_error_carries_no_data(response)
    assert response['error_type'] == 'validation_error'
    assert response['field'] == field
