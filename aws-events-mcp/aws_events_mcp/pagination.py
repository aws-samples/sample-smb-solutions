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

"""Opaque page-token encoding and decoding for the AWS Events MCP Server.

A page token is an opaque, base64url-encoded JSON payload of the shape
``{"offset": int, "fp": str}`` where ``offset`` is the start index of the next
page within the matched result set and ``fp`` is a stable fingerprint of the
``EventQuery`` that produced the page. Carrying the fingerprint lets the tool
layer detect when a token is replayed against a different query: decoding
validates the fingerprint against the current request's query and rejects a
mismatch, so a client cannot accidentally page through one result set using a
token minted for another (Requirements 3.3, 3.4, 3.5, 3.7).

The fingerprint is the SHA-256 of the query's canonical JSON serialization
(``model_dump(mode='json')`` with sorted keys), making it deterministic: equal
queries always produce an equal fingerprint. Decoding raises ``PageTokenError``
on any malformed input or fingerprint mismatch; the tool layer maps that to a
structured validation error with no event records.
"""

import base64
import binascii
import hashlib
import json
from aws_events_mcp.models import EventQuery
from dataclasses import dataclass
from typing import Any


#: JSON payload key holding the next-page offset.
_OFFSET_KEY = 'offset'
#: JSON payload key holding the query fingerprint.
_FINGERPRINT_KEY = 'fp'


class PageTokenError(Exception):
    """Raised when a page token is malformed or does not match the active query.

    The tool layer catches this and returns a structured validation error
    indicating that the page token is invalid, with no event records
    (Requirement 3.7).
    """


def compute_query_fingerprint(query: EventQuery) -> str:
    """Compute a stable fingerprint of an ``EventQuery``.

    The fingerprint is the hex SHA-256 digest of the query's canonical JSON
    serialization (``model_dump(mode='json')`` rendered with sorted keys and
    compact separators). It is deterministic: two equal queries always yield the
    same fingerprint, and a change to any filter value changes the fingerprint.

    Args:
        query: The active query whose filters should be fingerprinted.

    Returns:
        A 64-character lowercase hex string uniquely fingerprinting the query.
    """
    canonical = json.dumps(
        query.model_dump(mode='json'),
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class PageToken:
    """The decoded contents of a page token.

    Attributes:
        offset: Zero-based start index of the next page within the matched set.
        fingerprint: Fingerprint of the query that produced the page.
    """

    offset: int
    fingerprint: str

    @classmethod
    def create(cls, offset: int, query: EventQuery) -> 'PageToken':
        """Build a token for the given offset bound to a query's fingerprint.

        Args:
            offset: Zero-based start index of the next page. Must be a
                non-negative integer.
            query: The active query the token is bound to.

        Returns:
            A ``PageToken`` carrying the offset and the query fingerprint.

        Raises:
            ValueError: If ``offset`` is not a non-negative integer.
        """
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError('offset must be a non-negative integer.')
        return cls(offset=offset, fingerprint=compute_query_fingerprint(query))

    def encode(self) -> str:
        """Encode this token to an opaque base64url string.

        Returns:
            A base64url-encoded string (without ``=`` padding) of the JSON
            payload ``{"offset": <offset>, "fp": <fingerprint>}``.
        """
        payload = {_OFFSET_KEY: self.offset, _FINGERPRINT_KEY: self.fingerprint}
        raw = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')

    @classmethod
    def decode(cls, token: str, query: EventQuery) -> 'PageToken':
        """Decode and validate an opaque page token against the active query.

        Decoding succeeds only when the token is well-formed base64url, decodes
        to a JSON object with a non-negative integer ``offset`` and a string
        ``fp``, and that ``fp`` matches the fingerprint of ``query``. Any other
        condition raises ``PageTokenError``.

        Args:
            token: The opaque page token supplied by the client.
            query: The active query the token must match.

        Returns:
            The validated ``PageToken`` with the recovered offset.

        Raises:
            PageTokenError: If the token is malformed, structurally invalid, or
                its fingerprint does not match ``query``.
        """
        payload = _decode_payload(token)

        if _OFFSET_KEY not in payload or _FINGERPRINT_KEY not in payload:
            raise PageTokenError('Page token is missing required fields.')

        offset = payload[_OFFSET_KEY]
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise PageTokenError('Page token offset is invalid.')

        fingerprint = payload[_FINGERPRINT_KEY]
        if not isinstance(fingerprint, str):
            raise PageTokenError('Page token fingerprint is invalid.')

        if fingerprint != compute_query_fingerprint(query):
            raise PageTokenError('Page token does not match the current query.')

        return cls(offset=offset, fingerprint=fingerprint)


def _decode_payload(token: str) -> dict[str, Any]:
    """Decode an opaque token string into its JSON payload dictionary.

    Args:
        token: The opaque page token supplied by the client.

    Returns:
        The decoded JSON payload as a dictionary.

    Raises:
        PageTokenError: If the token is not a non-empty base64url string that
            decodes to a JSON object.
    """
    if not isinstance(token, str) or not token:
        raise PageTokenError('Page token must be a non-empty string.')

    padded = token + '=' * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode('ascii'))
    except (binascii.Error, ValueError, UnicodeEncodeError) as exc:
        raise PageTokenError('Page token is not valid base64url.') from exc

    try:
        payload = json.loads(raw.decode('utf-8'))
    except (ValueError, UnicodeDecodeError) as exc:
        raise PageTokenError('Page token payload is not valid JSON.') from exc

    if not isinstance(payload, dict):
        raise PageTokenError('Page token payload must be a JSON object.')

    return payload
