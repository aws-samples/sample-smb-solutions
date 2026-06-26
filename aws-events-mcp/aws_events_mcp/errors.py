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

"""Structured error types and response-shaping helpers.

This module defines the typed ``CatalogSourceError`` hierarchy raised by the
catalog source and cache layers, and the helper functions that build the
structured response dictionaries returned by the MCP tools. Tools never raise
out to the MCP runtime; instead they return one of these dictionaries so the
server process is never terminated by a catalog failure (reliability NFR).

The response shapes produced here match the design's "Standard response shapes"
and "Error Handling" sections exactly: every error, validation, and not-found
response carries an empty ``items`` collection and a ``total_count`` of ``0``
where applicable, so no partial or unparsed catalog data ever leaks into an
error result.
"""

from typing import Any, Dict


# error_type values used in source-error responses (design: Error Handling table).
ERROR_TYPE_VALIDATION = 'validation_error'
ERROR_TYPE_SOURCE_UNREACHABLE = 'source_unreachable'
ERROR_TYPE_SOURCE_TIMEOUT = 'source_timeout'
ERROR_TYPE_SOURCE_UNPARSEABLE = 'source_unparseable'
ERROR_TYPE_SOURCE_PARTIAL = 'source_partial'


class CatalogSourceError(Exception):
    """Base class for all catalog-retrieval and parsing failures.

    Each subtype carries an ``error_type`` string that the tool layer maps onto
    the ``error_type`` field of a structured source-error response.

    Attributes:
        error_type: The response ``error_type`` string associated with this
            failure (one of the ``ERROR_TYPE_SOURCE_*`` constants).
    """

    error_type: str = ERROR_TYPE_SOURCE_UNPARSEABLE


class CatalogUnreachableError(CatalogSourceError):
    """Raised when the catalog source cannot be reached (connection failure).

    Validates Requirement 11.1.
    """

    error_type: str = ERROR_TYPE_SOURCE_UNREACHABLE


class CatalogTimeoutError(CatalogSourceError):
    """Raised when a catalog request does not complete within the 30s limit.

    Validates Requirements 2.5 and 11.2.
    """

    error_type: str = ERROR_TYPE_SOURCE_TIMEOUT


class CatalogUnparseableError(CatalogSourceError):
    """Raised when catalog content cannot be parsed into any ``Event``.

    This covers both an undecodable response body and content that yielded zero
    valid records despite being non-empty. Validates Requirement 11.3.
    """

    error_type: str = ERROR_TYPE_SOURCE_UNPARSEABLE


class CatalogPartialParseError(CatalogSourceError):
    """Signals that at least one but not all catalog records could be parsed.

    The parser/cache layers surface this distinguishable signal so the tool
    layer can return a ``source_partial`` error response that excludes the
    partial data. Validates Requirement 11.4.
    """

    error_type: str = ERROR_TYPE_SOURCE_PARTIAL


def validation_error(field: str, message: str) -> Dict[str, Any]:
    """Build a structured validation-error response.

    Used whenever a tool argument fails a schema or semantic validator. The
    response identifies the offending field and how to correct it, and returns
    no event records.

    Args:
        field: Name of the input field that failed validation.
        message: Human-readable message stating how to correct the input.

    Returns:
        A response dict with ``status`` ``'error'``, ``error_type``
        ``'validation_error'``, the offending ``field``, the ``message``, an
        empty ``items`` list, and a ``total_count`` of ``0``.
    """
    return {
        'status': 'error',
        'error_type': ERROR_TYPE_VALIDATION,
        'field': field,
        'message': message,
        'items': [],
        'total_count': 0,
    }


def source_error(error_type: str, message: str) -> Dict[str, Any]:
    """Build a structured catalog source-error response.

    Used when retrieving or parsing the catalog fails. The response excludes all
    partial or unparsed catalog data (Requirement 11.5).

    Args:
        error_type: One of ``source_unreachable``, ``source_timeout``,
            ``source_unparseable``, or ``source_partial``.
        message: Human-readable description of the failure.

    Returns:
        A response dict with ``status`` ``'error'``, the supplied
        ``error_type``, the ``message``, an empty ``items`` list, and a
        ``total_count`` of ``0``.
    """
    return {
        'status': 'error',
        'error_type': error_type,
        'message': message,
        'items': [],
        'total_count': 0,
    }


def not_found(message: str) -> Dict[str, Any]:
    """Build a structured not-found response for event-detail lookups.

    Used by ``get_event_details`` when no event matches the supplied identifier
    (Requirement 9.4). No event record is returned.

    Args:
        message: Human-readable message stating that the identifier was not
            matched.

    Returns:
        A response dict with ``status`` ``'not_found'``, the ``message``, and an
        ``event`` of ``None``.
    """
    return {
        'status': 'not_found',
        'message': message,
        'event': None,
    }
