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

"""Property-based test for keyword search soundness and completeness.

This module validates that :func:`aws_events_mcp.query.apply_query`, when given a
keyword-only ``EventQuery``, behaves as a case-insensitive substring search over
each event's title or description:

* **Soundness:** every event in the result contains the keyword as a
  case-insensitive substring of its title or description.
* **Completeness:** every catalog event whose title or description contains the
  keyword (case-insensitively) appears in the (unpaginated) result.

``apply_query`` returns the full matched set with no pagination, so the result is
compared directly against the catalog with no page-size involved. Keywords are
drawn 1-256 characters, mixed case, and include both keywords that are
guaranteed to appear as a substring of some event (derived from a slice of a
generated event's title or description) and keywords that are likely absent
(free-form random text), so both the matching and non-matching paths are
exercised. The matching oracle uses ``str.casefold`` to mirror the
implementation's case-insensitivity exactly.

Feature: aws-events-mcp, Property 7: Keyword search soundness and completeness
Validates: Requirements 4.1
"""

from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query
from hypothesis import given, settings
from hypothesis import strategies as st


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` instance with searchable title/description.

    Title is non-empty and description may be empty; both draw mixed-case text
    across the unicode range so case-insensitive matching is exercised. The
    remaining fields are populated just enough to construct a valid model and do
    not affect keyword matching.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    return Event(
        event_id=draw(st.text(min_size=1, max_size=32)),
        title=draw(st.text(min_size=1, max_size=80)),
        description=draw(st.text(max_size=160)),
        start_date=draw(st.dates()),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
    )


def _substring_of(text: str) -> st.SearchStrategy[str]:
    """Build a strategy producing a non-empty substring (slice) of ``text``.

    Args:
        text: The source string to slice; must be non-empty.

    Returns:
        A strategy yielding a contiguous, non-empty slice of ``text``.
    """
    return st.integers(min_value=0, max_value=len(text) - 1).flatmap(
        lambda start: st.integers(min_value=start + 1, max_value=len(text)).map(
            lambda end: text[start:end]
        )
    )


@st.composite
def catalog_and_keyword(draw: st.DrawFn) -> tuple[list[Event], str]:
    """Generate a catalog paired with a keyword that exercises both paths.

    The keyword is, with even probability, derived from a contiguous slice of a
    generated event's title or description (guaranteeing at least one match,
    optionally re-cased to exercise case-insensitivity) or a free-form random
    string of 1-256 characters (typically absent). Either way the keyword is
    constrained to 1-256 characters per Requirement 4.1.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A ``(catalog, keyword)`` pair.
    """
    catalog = draw(st.lists(events(), max_size=20))

    # Candidate source strings to derive a guaranteed-substring keyword from.
    sources = [event.title for event in catalog if event.title]
    sources += [event.description for event in catalog if event.description]

    derive = bool(sources) and draw(st.booleans())
    if derive:
        source = draw(st.sampled_from(sources))
        keyword = draw(_substring_of(source))
        # Optionally re-case the derived keyword to exercise case-insensitivity.
        recasing = draw(st.sampled_from(('none', 'upper', 'lower', 'swap')))
        if recasing == 'upper':
            keyword = keyword.upper()
        elif recasing == 'lower':
            keyword = keyword.lower()
        elif recasing == 'swap':
            keyword = keyword.swapcase()
    else:
        keyword = draw(st.text(min_size=1, max_size=256))

    # Enforce the 1-256 length bound (re-casing/slicing keeps length <= source).
    keyword = keyword[:256]
    if not keyword:
        keyword = draw(st.text(min_size=1, max_size=256))
    return catalog, keyword


def _contains_keyword(event: Event, keyword: str) -> bool:
    """Return whether the keyword is a case-insensitive substring of the event.

    Args:
        event: The event under test.
        keyword: The search keyword.

    Returns:
        ``True`` when ``keyword`` (case-folded) is a substring of the event's
        case-folded title or description; otherwise ``False``.
    """
    needle = keyword.casefold()
    return needle in event.title.casefold() or needle in event.description.casefold()


# Feature: aws-events-mcp, Property 7: Keyword search soundness and completeness
@settings(max_examples=100)
@given(catalog_keyword=catalog_and_keyword())
def test_keyword_search_sound_and_complete(catalog_keyword: tuple[list[Event], str]) -> None:
    """Keyword search returns exactly the events containing the keyword.

    Soundness: every returned event contains the keyword case-insensitively in
    its title or description. Completeness: every catalog event that contains the
    keyword appears in the unpaginated result.

    Validates: Requirements 4.1
    """
    catalog, keyword = catalog_keyword
    result = apply_query(catalog, EventQuery(keyword=keyword))
    result_ids = {event.event_id for event in result}

    # Soundness: nothing in the result fails the case-insensitive substring test.
    for event in result:
        assert _contains_keyword(event, keyword)

    # Completeness: every matching catalog event is present in the result.
    for event in catalog:
        if _contains_keyword(event, keyword):
            assert event.event_id in result_ids
