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

"""Property-based test for the ``search_upcoming_events`` tool.

This module validates that invoking the ``search_upcoming_events`` tool against
a catalog whose events span dates in the past and future relative to an injected
"current date" (UTC) returns exactly the events that are both (a) keyword matches
and (b) not in the past:

* **Soundness:** every returned event contains the keyword as a case-insensitive
  substring of its title or description AND has a start date greater than or
  equal to the effective lower bound — the later of the injected current date
  and any supplied ``start_date``.
* **Completeness:** every catalog event satisfying both predicates appears in the
  unpaginated matched set.

The property is exercised end-to-end through the registered tool, exactly as a
client would experience it, reusing the harness established by the Property 5
test (``list_upcoming_events``):

* a fixed UTC "today" is injected by replacing ``server.datetime`` with a stub
  whose ``now`` returns that date, which drives ``_constrain_to_upcoming``;
* the catalog is supplied through ``set_catalog_cache`` backed by an in-memory
  fake cache whose async ``get_events`` returns the generated events, so no
  network access occurs; and
* the async tool is awaited and the global cache plus the patched ``datetime``
  are always restored afterwards.

Catalogs are kept small (<= 50 events) and the page size is the catalog-wide
maximum (100), so the single returned page covers the whole upcoming match set
and the completeness assertion is over the full unpaginated result. Generated
catalogs use unique positional ``event_id`` values to avoid duplicate-id
generator defects. Keywords are 1-256 characters, mixed case, and split between
keywords guaranteed to appear in some event and free-form (typically absent)
text so both the matching and non-matching paths are exercised.

It lives in its own file so it can run in parallel with the other property tests.

Feature: aws-events-mcp, Property 19: Upcoming keyword search is not in the past and respects the keyword
Validates: Requirements 12.1, 12.2, 12.3, 12.4
"""

import asyncio
from aws_events_mcp import server
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.consts import MAX_PAGE_SIZE
from aws_events_mcp.models import Event, LearningLevel, LocationMode
from datetime import date, datetime, tzinfo
from hypothesis import given, settings
from hypothesis import strategies as st
from typing import Optional, cast


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` with searchable text spanning past and future.

    ``start_date`` draws from the full ``hypothesis`` date range so generated
    catalogs straddle any injected "today", ensuring the property is exercised
    against events both before and after the current date. Title is non-empty
    and description may be empty; both draw mixed-case text so case-insensitive
    keyword matching is exercised. ``event_id`` is a placeholder here and is
    reassigned to a unique positional value by :func:`catalog_keyword_today`.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    small_text = st.text(alphabet='abcXYZ ', max_size=8)
    optional_small_text = st.none() | small_text
    return Event(
        event_id='placeholder',
        title=draw(st.text(alphabet='abcXYZ ', min_size=1, max_size=8)),
        description=draw(small_text),
        start_date=draw(st.dates()),
        start_time=draw(st.none() | st.text(max_size=8)),
        time_zone=draw(st.none() | st.text(max_size=8)),
        location=draw(optional_small_text),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(optional_small_text),
        partner_name=draw(optional_small_text),
        registration_url=draw(st.none() | st.text(max_size=8)),
        learn_more_url=draw(st.none() | st.text(max_size=8)),
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
def catalog_keyword_today(draw: st.DrawFn) -> tuple[list[Event], str, date, Optional[date]]:
    """Generate a catalog, keyword, injected today, and optional start date.

    The catalog is given unique positional ``event_id`` values so the
    catalog-level uniqueness constraint cannot be tripped by duplicate ids. The
    keyword is, with even probability, derived from a contiguous slice of a
    generated event's title or description (guaranteeing at least one match,
    optionally re-cased to exercise case-insensitivity) or free-form random text
    of 1-256 characters (typically absent); either way it is bounded to 1-256
    characters per Requirement 12.1. ``today`` and the optional ``start_date``
    both draw from the full date range so the effective lower bound is the later
    of the two and spans past and future relative to the catalog.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A ``(catalog, keyword, today, supplied_start)`` tuple where
        ``supplied_start`` may be ``None``.
    """
    raw_catalog = draw(st.lists(events(), max_size=50))
    catalog = [
        event.model_copy(update={'event_id': f'evt-{index}'})
        for index, event in enumerate(raw_catalog)
    ]

    # Candidate source strings to derive a guaranteed-substring keyword from.
    sources = [event.title for event in catalog if event.title]
    sources += [event.description for event in catalog if event.description]

    derive = bool(sources) and draw(st.booleans())
    if derive:
        source = draw(st.sampled_from(sources))
        keyword = draw(_substring_of(source))
        recasing = draw(st.sampled_from(('none', 'upper', 'lower', 'swap')))
        if recasing == 'upper':
            keyword = keyword.upper()
        elif recasing == 'lower':
            keyword = keyword.lower()
        elif recasing == 'swap':
            keyword = keyword.swapcase()
    else:
        keyword = draw(st.text(min_size=1, max_size=256))

    # The tool trims surrounding whitespace and rejects blank keywords
    # (Requirement 12.8), so a valid keyword must be non-blank after stripping.
    # Trimming a contiguous slice yields another contiguous slice, so the
    # derived-substring match guarantee is preserved. Match against the trimmed
    # form to mirror the implementation exactly.
    keyword = keyword.strip()[:256]
    if not keyword:
        keyword = draw(st.text(alphabet='abcXYZ012', min_size=1, max_size=10))

    today = draw(st.dates())
    supplied_start = draw(st.none() | st.dates())
    return catalog, keyword, today, supplied_start


class _FakeCatalogCache:
    """In-memory stand-in for ``CatalogCache`` returning canned events.

    Mirrors the single method the tool layer depends on (``async get_events``),
    so it can be installed via :func:`aws_events_mcp.server.set_catalog_cache`
    without touching the network.
    """

    def __init__(self, events: list[Event]) -> None:
        """Initialize the fake cache.

        Args:
            events: The events returned by every ``get_events`` call.
        """
        self._events = events

    async def get_events(self, *, force_refresh: bool = False) -> list[Event]:
        """Return the canned events.

        Args:
            force_refresh: Accepted for interface compatibility; ignored.

        Returns:
            The configured events.
        """
        return list(self._events)


class _FixedNow:
    """Stub for the ``datetime`` class exposing a fixed ``now``.

    Installed in place of ``server.datetime`` so ``_constrain_to_upcoming``'s
    ``datetime.now(timezone.utc).date()`` resolves to the injected "today".
    """

    def __init__(self, today: date) -> None:
        """Initialize the stub.

        Args:
            today: The UTC calendar date that ``now(...).date()`` should yield.
        """
        self._today = today

    def now(self, tz: Optional[tzinfo] = None) -> datetime:
        """Return a datetime on the injected date in the given time zone.

        Args:
            tz: The time zone passed by the caller (``timezone.utc``); attached
                to the returned datetime so behavior matches the real class.

        Returns:
            A ``datetime`` whose ``.date()`` equals the injected ``today``.
        """
        return datetime(self._today.year, self._today.month, self._today.day, tzinfo=tz)


def _invoke_search_upcoming(
    catalog: list[Event], keyword: str, today: date, supplied_start: Optional[date]
) -> dict:
    """Invoke ``search_upcoming_events`` with a fixed today and fake catalog.

    Injects ``today`` by patching ``server.datetime`` and supplies ``catalog``
    through ``set_catalog_cache``; both globals are restored before returning.
    Explicit ``None`` arguments are passed for every unused filter so the raw
    tool function does not fall back to its ``Field(...)`` schema defaults. The
    page size is the catalog-wide maximum so the single returned page covers the
    whole upcoming match set for the small generated catalogs.

    Args:
        catalog: The events the fake cache should serve.
        keyword: The required search keyword.
        today: The injected UTC "today".
        supplied_start: An optional ``start_date`` lower bound; passed as an
            ISO ``YYYY-MM-DD`` string when present.

    Returns:
        The tool's response dictionary.
    """
    original_datetime = server.datetime
    server.datetime = _FixedNow(today)
    server.set_catalog_cache(cast(CatalogCache, _FakeCatalogCache(catalog)))
    try:
        return asyncio.run(
            server.search_upcoming_events(
                keyword=keyword,
                learning_level=None,
                location_mode=None,
                location_text=None,
                event_type=None,
                partner=None,
                start_date=None if supplied_start is None else supplied_start.isoformat(),
                end_date=None,
                page_size=MAX_PAGE_SIZE,
                page_token=None,
            )
        )
    finally:
        server.datetime = original_datetime
        server.set_catalog_cache(None)


def _contains_keyword(title: str, description: str, keyword: str) -> bool:
    """Return whether the keyword is a case-insensitive substring of the text.

    Args:
        title: The event title.
        description: The event description.
        keyword: The search keyword.

    Returns:
        ``True`` when ``keyword`` (case-folded) is a substring of the case-folded
        title or description; otherwise ``False``.
    """
    needle = keyword.casefold()
    return needle in title.casefold() or needle in description.casefold()


# Feature: aws-events-mcp, Property 19: Upcoming keyword search is not in the past and respects the keyword
@settings(max_examples=100)
@given(generated=catalog_keyword_today())
def test_search_upcoming_respects_keyword_and_is_not_in_the_past(
    generated: tuple[list[Event], str, date, Optional[date]],
) -> None:
    """``search_upcoming_events`` returns exactly upcoming keyword matches.

    Soundness: every returned event contains the keyword case-insensitively in
    its title or description and starts on or after the effective lower bound
    (the later of the injected current date and any supplied start date).
    Completeness: every catalog event satisfying both predicates appears in the
    unpaginated matched set.

    Validates: Requirements 12.1, 12.2, 12.3, 12.4
    """
    catalog, keyword, today, supplied_start = generated
    effective_lower_bound = today if supplied_start is None else max(supplied_start, today)

    response = _invoke_search_upcoming(catalog, keyword, today, supplied_start)

    assert response['status'] == 'success'
    result_ids = {item['event_id'] for item in response['items']}

    # Soundness: every returned event matches the keyword and is not in the past.
    for item in response['items']:
        assert _contains_keyword(item['title'], item['description'], keyword)
        assert date.fromisoformat(item['start_date']) >= effective_lower_bound

    # Completeness: every catalog event satisfying both predicates is present.
    for event in catalog:
        matches_keyword = _contains_keyword(event.title, event.description, keyword)
        is_upcoming = event.start_date >= effective_lower_bound
        if matches_keyword and is_upcoming:
            assert event.event_id in result_ids
