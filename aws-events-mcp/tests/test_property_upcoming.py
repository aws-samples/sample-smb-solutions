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

"""Property-based test for the ``list_upcoming_events`` lower bound.

This module validates that, for any catalog whose events span dates in the past
and future relative to an injected "current date" (UTC), invoking the
``list_upcoming_events`` tool returns only events whose start date is greater
than or equal to that current date (Requirement 7.3).

The property is exercised end-to-end through the registered tool rather than
against the pure ``_constrain_to_upcoming`` helper, so the requirement is
validated as a client would experience it:

* a fixed UTC "today" is injected by replacing ``server.datetime`` with a stub
  whose ``now`` returns that date, which drives ``_constrain_to_upcoming``;
* the catalog is supplied through ``set_catalog_cache`` backed by an in-memory
  fake cache whose async ``get_events`` returns the generated events, so no
  network access occurs; and
* the async tool is awaited and the global cache plus the patched ``datetime``
  are always restored afterwards.

Catalogs are kept small enough that every upcoming match fits within a single
maximum-size page, so the assertion covers the complete upcoming result set
rather than only its first page.

It lives in its own file so it can run in parallel with the other property
tests.

Feature: aws-events-mcp, Property 5: Upcoming events are not in the past
Validates: Requirements 7.3
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
    """Generate a valid ``Event`` with a start date spanning past and future.

    ``start_date`` draws from the full ``hypothesis`` date range so generated
    catalogs straddle any injected "today", ensuring the property is exercised
    against events that are both before and after the current date. Remaining
    fields draw from a small value space; they do not affect the upcoming bound
    but keep generated events realistic.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    small_text = st.text(alphabet='abcXYZ ', max_size=8)
    optional_small_text = st.none() | small_text
    return Event(
        event_id=draw(st.text(alphabet='abc012', min_size=1, max_size=4)),
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


class _FakeCatalogCache:
    """In-memory stand-in for ``CatalogCache`` returning canned events.

    Mirrors the single method the tool layer depends on
    (``async get_events``), so it can be installed via
    :func:`aws_events_mcp.server.set_catalog_cache` without touching the
    network.
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

    Installed in place of ``server.datetime`` so
    ``_constrain_to_upcoming``'s ``datetime.now(timezone.utc).date()`` resolves
    to the injected "today".
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


def _invoke_upcoming(catalog: list[Event], today: date) -> dict:
    """Invoke ``list_upcoming_events`` with a fixed today and fake catalog.

    Injects ``today`` by patching ``server.datetime`` and supplies ``catalog``
    through ``set_catalog_cache``; both globals are restored before returning.
    Explicit ``None`` arguments are passed for every filter so the raw tool
    function does not fall back to its ``Field(...)`` schema defaults. The page
    size is the catalog-wide maximum so the single returned page covers the
    whole upcoming match set for the small generated catalogs.

    Args:
        catalog: The events the fake cache should serve.
        today: The injected UTC "today".

    Returns:
        The tool's response dictionary.
    """
    original_datetime = server.datetime
    server.datetime = _FixedNow(today)
    server.set_catalog_cache(cast(CatalogCache, _FakeCatalogCache(catalog)))
    try:
        return asyncio.run(
            server.list_upcoming_events(
                learning_level=None,
                location_mode=None,
                location_text=None,
                event_type=None,
                partner=None,
                start_date=None,
                end_date=None,
                page_size=MAX_PAGE_SIZE,
                page_token=None,
            )
        )
    finally:
        server.datetime = original_datetime
        server.set_catalog_cache(None)


# Feature: aws-events-mcp, Property 5: Upcoming events are not in the past
@settings(max_examples=100)
@given(
    catalog=st.lists(events(), max_size=50),
    today=st.dates(),
)
def test_upcoming_events_are_not_in_the_past(catalog: list[Event], today: date) -> None:
    """Every event returned by ``list_upcoming_events`` starts on/after today.

    Validates: Requirements 7.3
    """
    response = _invoke_upcoming(catalog, today)

    assert response['status'] == 'success'
    for item in response['items']:
        assert date.fromisoformat(item['start_date']) >= today
