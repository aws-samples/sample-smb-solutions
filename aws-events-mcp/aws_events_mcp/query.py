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

"""Pure query engine for the AWS Events MCP Server.

This module filters, keyword-searches, and orders a ``list[Event]`` according to
an ``EventQuery``. It is intentionally pure: it performs no I/O and imports no
MCP or network types, so it can be used independently of the transport layer
(NFR Modular Design) and is deterministic for a fixed input list (NFR
Reliability).

Filtering rules (all case-insensitive where the requirements specify):

* learning level: exact match against any supplied level, 1-4 values
  (Requirements 5.1, 5.2).
* event type: exact match (Requirement 8.1).
* location text: substring match on the location field (Requirement 6.3).
* partner: substring match on the partner name (Requirement 8.2).
* keyword: substring match on the title or description (Requirements 2.1, 4.1).
* date range: ``start_date <= event.start_date <= end_date``, inclusive bounds
  (Requirements 7.1, 7.2).
* location mode: ``virtual`` -> online events, ``physical`` -> venue events
  (Requirements 6.1, 6.2).
* Multiple filters combine with AND (Requirements 6.5, 8.3); an absent or empty
  filter imposes no constraint along that dimension (Requirements 5.5, 6.4).

Ordering is always by start date ascending, with a stable secondary sort on
``event_id`` to guarantee a total order for deterministic pagination.

``apply_query`` returns the full ordered, matched set; ``paginate`` then slices
that set into a single bounded ``EventPage``, capping the page at
``consts.MAX_PAGE_SIZE`` so a tool never returns the whole catalog in one
response (NFR Performance).
"""

from .consts import MAX_PAGE_SIZE
from .models import Event, EventPage, EventQuery


def _matches_learning_level(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies the learning-level filter.

    Args:
        event: The event under test.
        query: The active query whose ``learning_levels`` drive the filter.

    Returns:
        ``True`` when no learning levels are supplied (no constraint) or when the
        event's learning level is a case-insensitive exact match to any supplied
        value; otherwise ``False``.
    """
    if not query.learning_levels:
        return True
    if event.learning_level is None:
        return False
    event_level = event.learning_level.value.casefold()
    return any(level.value.casefold() == event_level for level in query.learning_levels)


def _matches_location_mode(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies the location-mode filter.

    Args:
        event: The event under test.
        query: The active query whose ``location_mode`` drives the filter.

    Returns:
        ``True`` when no location mode is supplied (no constraint) or when the
        event's location mode equals the requested mode; otherwise ``False``.
    """
    if query.location_mode is None:
        return True
    return event.location_mode == query.location_mode


def _matches_location_text(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies the location-text filter.

    Args:
        event: The event under test.
        query: The active query whose ``location_text`` drives the filter.

    Returns:
        ``True`` when no location text is supplied (no constraint) or when the
        event's location contains the supplied text as a case-insensitive
        substring; otherwise ``False``.
    """
    if not query.location_text:
        return True
    if event.location is None:
        return False
    return query.location_text.casefold() in event.location.casefold()


def _matches_event_type(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies the event-type filter.

    Args:
        event: The event under test.
        query: The active query whose ``event_type`` drives the filter.

    Returns:
        ``True`` when no event type is supplied (no constraint) or when the
        event's type is a case-insensitive exact match to the supplied value;
        otherwise ``False``.
    """
    if not query.event_type:
        return True
    if event.event_type is None:
        return False
    return event.event_type.casefold() == query.event_type.casefold()


def _matches_partner(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies the partner filter.

    Args:
        event: The event under test.
        query: The active query whose ``partner`` drives the filter.

    Returns:
        ``True`` when no partner is supplied (no constraint) or when the event's
        partner name contains the supplied text as a case-insensitive substring;
        otherwise ``False``.
    """
    if not query.partner:
        return True
    if event.partner_name is None:
        return False
    return query.partner.casefold() in event.partner_name.casefold()


def _matches_keyword(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies the keyword filter.

    Args:
        event: The event under test.
        query: The active query whose ``keyword`` drives the filter.

    Returns:
        ``True`` when no keyword is supplied (no constraint) or when the keyword
        is a case-insensitive substring of the event's title or description;
        otherwise ``False``.
    """
    if not query.keyword:
        return True
    needle = query.keyword.casefold()
    return needle in event.title.casefold() or needle in event.description.casefold()


def _matches_date_range(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies the inclusive date-range filter.

    Args:
        event: The event under test.
        query: The active query whose ``start_date``/``end_date`` drive the
            filter.

    Returns:
        ``True`` when the event's start date is greater than or equal to the
        supplied lower bound (if any) and less than or equal to the supplied
        upper bound (if any); otherwise ``False``. Each bound is independent, so
        an absent bound imposes no constraint on that side.
    """
    if query.start_date is not None and event.start_date < query.start_date:
        return False
    if query.end_date is not None and event.start_date > query.end_date:
        return False
    return True


def _matches(event: Event, query: EventQuery) -> bool:
    """Return whether the event satisfies every supplied filter (logical AND).

    Args:
        event: The event under test.
        query: The active query whose populated fields are applied conjunctively.

    Returns:
        ``True`` only when the event satisfies all supplied filter predicates;
        otherwise ``False``. An absent or empty filter imposes no constraint.
    """
    return (
        _matches_learning_level(event, query)
        and _matches_location_mode(event, query)
        and _matches_location_text(event, query)
        and _matches_event_type(event, query)
        and _matches_partner(event, query)
        and _matches_keyword(event, query)
        and _matches_date_range(event, query)
    )


def apply_query(events: list[Event], query: EventQuery) -> list[Event]:
    """Filter then order events by the query.

    Every populated field of ``query`` contributes a filter predicate and the
    predicates combine with logical AND; an absent or empty field imposes no
    constraint along its dimension. The matched events are returned ordered by
    start date ascending with a stable secondary sort on ``event_id`` so the
    ordering is a total order, making the result deterministic for a fixed input
    list (NFR Reliability).

    This function is pure: it does not mutate ``events`` and performs no I/O.

    Args:
        events: The catalog of events to query.
        query: The normalized filters to apply.

    Returns:
        The matched events ordered by ``(start_date, event_id)`` ascending. The
        full matched set is returned; slicing into pages is the responsibility of
        the pagination layer.
    """
    matched = [event for event in events if _matches(event, query)]
    matched.sort(key=lambda event: (event.start_date, event.event_id))
    return matched


def paginate(events: list[Event], page_size: int, offset: int) -> EventPage:
    """Slice an already filtered and ordered matched set into a single page.

    ``events`` is assumed to be the output of ``apply_query``: already filtered
    and ordered by ``(start_date, event_id)`` ascending. This function does not
    re-filter or re-order; it only slices, so the page preserves that ordering.

    The applied page size is capped at ``consts.MAX_PAGE_SIZE`` so a single
    response never exceeds the catalog-wide maximum and never returns the whole
    catalog at once (Requirements 2.6, 3.2, 7.5, NFR Performance). The reported
    ``total_count`` is the size of the full matched set, independent of the page
    slice (Requirements 2.3, 2.6). A ``next_offset`` is returned only when more
    matches remain after this page, i.e. ``offset + applied_page_size <
    total_count`` (Requirements 3.3, 3.4); otherwise it is ``None``.

    This function is pure: it does not mutate ``events`` and performs no I/O.

    Args:
        events: The full matched, ordered set to page over.
        page_size: The requested maximum number of items for this page; capped
            at ``consts.MAX_PAGE_SIZE``.
        offset: The zero-based index into ``events`` at which this page begins.
            Values at or beyond ``total_count`` yield an empty page.

    Returns:
        An ``EventPage`` whose ``items`` hold at most ``min(page_size,
        consts.MAX_PAGE_SIZE)`` events starting at ``offset``, whose
        ``total_count`` is the length of ``events``, and whose ``next_offset``
        points to the next page when more matches remain or is ``None``
        otherwise.
    """
    total_count = len(events)
    applied_page_size = min(page_size, MAX_PAGE_SIZE)
    start = max(offset, 0)
    end = start + applied_page_size
    items = events[start:end]
    next_offset = end if end < total_count else None
    return EventPage(items=items, total_count=total_count, next_offset=next_offset)
