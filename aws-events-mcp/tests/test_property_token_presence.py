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

"""Property-based test for the pagination token presence invariant.

This module validates that, for any matched list of events, any applied page
size, and any offset, ``paginate`` emits a next-page token (a non-``None``
``next_offset``) if and only if more matches remain after the current page,
i.e. the number of matching events exceeds ``offset + applied_page_size`` where
``applied_page_size = min(page_size, MAX_PAGE_SIZE)``. A next page exists exactly
when more matches remain.

Catalog sizes deliberately span below, at, and above the common page sizes and
the 100-item cap; page sizes range 1..150 (so the ``MAX_PAGE_SIZE`` cap is
exercised); and offsets range from 0 to just beyond the catalog length so the
boundary (``offset + applied_page_size == total_count``) is hit from both sides.

It lives in its own file so it can run in parallel with the other property
tests.

Feature: aws-events-mcp, Property 9: Pagination token presence invariant
Validates: Requirements 3.1, 3.2, 3.3, 3.4
"""

from aws_events_mcp.consts import MAX_PAGE_SIZE
from aws_events_mcp.models import Event, LearningLevel, LocationMode
from aws_events_mcp.query import paginate
from hypothesis import given, settings
from hypothesis import strategies as st


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` instance.

    The exact field values are immaterial to this property, which depends only
    on the size of the matched list and the page/offset arithmetic; a lightweight
    generator keeps examples cheap while still producing distinct, valid events.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    return Event(
        event_id=draw(st.text(alphabet='abc012', min_size=1, max_size=4)),
        title=draw(st.text(alphabet='abcXYZ ', min_size=1, max_size=8)),
        description=draw(st.text(alphabet='abcXYZ ', max_size=8)),
        start_date=draw(st.dates()),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
    )


@st.composite
def matched_lists_with_offsets(draw: st.DrawFn) -> tuple[list[Event], int]:
    """Generate a matched list paired with an offset spanning its boundaries.

    Catalog sizes span below, at, and above the 100-item cap. The offset is drawn
    from 0 up to a few items beyond the list length so that the page boundary is
    exercised from both directions (more remain / none remain), including offsets
    at or past the end of the matched set.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A ``(matched, offset)`` tuple where ``matched`` is the matched event list
        and ``offset`` is a non-negative starting index.
    """
    matched = draw(st.lists(events(), max_size=130))
    offset = draw(st.integers(min_value=0, max_value=len(matched) + 5))
    return matched, offset


# Feature: aws-events-mcp, Property 9: Pagination token presence invariant
@settings(max_examples=100)
@given(
    matched_and_offset=matched_lists_with_offsets(),
    page_size=st.integers(min_value=1, max_value=150),
)
def test_next_token_present_iff_more_matches_remain(
    matched_and_offset: tuple[list[Event], int], page_size: int
) -> None:
    """A next page token exists exactly when more matches remain.

    Validates: Requirements 3.1, 3.2, 3.3, 3.4
    """
    matched, offset = matched_and_offset
    applied_page_size = min(page_size, MAX_PAGE_SIZE)

    page = paginate(matched, page_size, offset)

    # total_count reflects the full matched set, independent of page/offset.
    assert page.total_count == len(matched)

    # The defining biconditional: a next-page token is present if and only if
    # the number of matches exceeds offset + applied_page_size.
    more_remain = page.total_count > offset + applied_page_size
    assert (page.next_offset is not None) == more_remain

    # When a token is present it points exactly past this page; when absent,
    # this page reaches (or passes) the end of the matched set.
    if page.next_offset is not None:
        assert page.next_offset == offset + applied_page_size
        assert page.next_offset < page.total_count
    else:
        assert offset + applied_page_size >= page.total_count
