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

"""Property-based test for page-token round-trip and tamper detection.

This module validates that a page token encoded for a given offset and query
decodes back to the same offset when checked against the same query (the
round-trip), and that decoding fails with ``PageTokenError`` when the token is
malformed (random non-base64url text, truncated, or empty) or when its
fingerprint was minted for a different query than the one it is decoded against
(tamper / replay detection). It lives in its own file so it can run in parallel
with the other property tests.

Feature: aws-events-mcp, Property 11: Page token round-trip and tamper detection
Validates: Requirements 3.7
"""

from aws_events_mcp.models import EventQuery, LearningLevel, LocationMode
from aws_events_mcp.pagination import (
    PageToken,
    PageTokenError,
    compute_query_fingerprint,
)
from hypothesis import assume, given, settings
from hypothesis import strategies as st


@st.composite
def event_queries(draw: st.DrawFn) -> EventQuery:
    """Generate a varied ``EventQuery`` instance.

    Each filter dimension is independently present or absent, drawing text
    across mixed case, learning-level subsets, location modes, and optional
    date bounds, so generated queries produce a wide range of fingerprints.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid ``EventQuery`` instance.
    """
    optional_text = st.none() | st.text(max_size=40)
    return EventQuery(
        keyword=draw(optional_text),
        learning_levels=draw(
            st.lists(st.sampled_from(list(LearningLevel)), max_size=4, unique=True)
        ),
        location_mode=draw(st.none() | st.sampled_from(list(LocationMode))),
        location_text=draw(optional_text),
        event_type=draw(optional_text),
        partner=draw(optional_text),
        start_date=draw(st.none() | st.dates()),
        end_date=draw(st.none() | st.dates()),
    )


# Feature: aws-events-mcp, Property 11: Page token round-trip and tamper detection
@settings(max_examples=100)
@given(offset=st.integers(min_value=0, max_value=10_000_000), query=event_queries())
def test_page_token_round_trip(offset: int, query: EventQuery) -> None:
    """Decoding an encoded token against its query recovers the original offset.

    Validates: Requirements 3.7
    """
    token = PageToken.create(offset, query).encode()
    assert PageToken.decode(token, query).offset == offset


# Feature: aws-events-mcp, Property 11: Page token round-trip and tamper detection
@settings(max_examples=100)
@given(token=st.text(max_size=64), query=event_queries())
def test_malformed_token_fails_decode(token: str, query: EventQuery) -> None:
    """Random/truncated/empty token text fails decode with PageTokenError.

    Only genuine, well-formed base64url tokens carrying a matching fingerprint
    decode successfully; arbitrary text (which is almost never a valid encoded
    token for this query) must be rejected.

    Validates: Requirements 3.7
    """
    valid = PageToken.create(0, query).encode()
    assume(token != valid)
    try:
        decoded = PageToken.decode(token, query)
    except PageTokenError:
        return
    # If decoding did not raise, the text happened to be a structurally valid
    # token whose fingerprint matches this query — only possible if it equals
    # the canonical encoding, which we excluded above.
    raise AssertionError(f'Expected PageTokenError for malformed token, got {decoded!r}.')


# Feature: aws-events-mcp, Property 11: Page token round-trip and tamper detection
@settings(max_examples=100)
@given(
    offset=st.integers(min_value=0, max_value=10_000_000),
    query_a=event_queries(),
    query_b=event_queries(),
)
def test_fingerprint_mismatch_fails_decode(
    offset: int, query_a: EventQuery, query_b: EventQuery
) -> None:
    """A token minted for query A is rejected when decoded against query B.

    Validates: Requirements 3.7
    """
    assume(compute_query_fingerprint(query_a) != compute_query_fingerprint(query_b))
    token = PageToken.create(offset, query_a).encode()
    try:
        PageToken.decode(token, query_b)
    except PageTokenError:
        return
    raise AssertionError('Expected PageTokenError for fingerprint mismatch.')


# Feature: aws-events-mcp, Property 11: Page token round-trip and tamper detection
@settings(max_examples=100)
@given(token=st.sampled_from(['', '!!!not-base64!!!', '====', 'a', 'eyJ']), query=event_queries())
def test_known_malformed_tokens_fail_decode(token: str, query: EventQuery) -> None:
    """Empty, non-base64url, and truncated tokens fail decode with PageTokenError.

    Validates: Requirements 3.7
    """
    try:
        PageToken.decode(token, query)
    except PageTokenError:
        return
    raise AssertionError(f'Expected PageTokenError for token {token!r}.')
