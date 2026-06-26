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

"""Property-based test that filtering is deterministic.

``query.apply_query`` is a pure function over a fixed input list, so it must be
deterministic: the same query applied to the same catalog SHALL produce equal,
identically ordered collections every time. Because ordering uses the total-order
key ``(start_date, event_id)`` over a catalog whose ``event_id`` values are
unique, the matched, ordered result must also be independent of the catalog's
input order, so a shuffled copy of the same catalog yields the same ordered
output.

This file validates both facets:

* REPEATABILITY - ``apply_query(catalog, query)`` equals a second invocation of
  ``apply_query(catalog, query)`` element-by-element, including order.
* ORDER INDEPENDENCE - applying the same query to a shuffled copy of the catalog
  yields the same ordered result, since the ordering key is a total order over a
  catalog with unique ``event_id`` values.

Catalogs are generated with positionally assigned, unique identifiers
(``evt-0``, ``evt-1``, ...) to honor the ``Event`` model's UNIQUE ``event_id``
invariant; this makes ``(start_date, event_id)`` a true total order so order
independence holds.

It lives in its own file so it can run in parallel with the other property
tests.

Feature: aws-events-mcp, Property 18: Filtering is deterministic
Validates: Requirements 2.1, 5.1
"""

from aws_events_mcp.models import Event, EventQuery, LearningLevel, LocationMode
from aws_events_mcp.query import apply_query
from hypothesis import given, settings
from hypothesis import strategies as st


@st.composite
def event_fields(draw: st.DrawFn) -> dict:
    """Generate the value fields of an event, excluding ``event_id``.

    Text fields draw from a small alphabet (mixed case, overlapping the query
    strategy's filter values) so generated queries match a non-trivial fraction
    of generated catalogs. ``start_date`` varies enough to exercise the
    ``(start_date, event_id)`` ordering and its ``event_id`` tiebreak, which is
    what makes order independence observable.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A mapping of event field names (excluding ``event_id``) to values.
    """
    small_text = st.text(alphabet='abcXYZ ', max_size=8)
    optional_small_text = st.none() | small_text
    return {
        'title': draw(st.text(alphabet='abcXYZ ', min_size=1, max_size=8)),
        'description': draw(small_text),
        'start_date': draw(st.dates()),
        'start_time': draw(st.none() | st.text(max_size=8)),
        'time_zone': draw(st.none() | st.text(max_size=8)),
        'location': draw(optional_small_text),
        'location_mode': draw(st.sampled_from(list(LocationMode))),
        'learning_level': draw(st.none() | st.sampled_from(list(LearningLevel))),
        'event_type': draw(optional_small_text),
        'partner_name': draw(optional_small_text),
        'registration_url': draw(st.none() | st.text(max_size=8)),
        'learn_more_url': draw(st.none() | st.text(max_size=8)),
    }


@st.composite
def catalogs(draw: st.DrawFn) -> list[Event]:
    """Generate a catalog of valid events with UNIQUE identifiers.

    Identifiers are assigned positionally (``evt-0``, ``evt-1``, ...) so every
    event's ``event_id`` is unique, honoring the ``Event`` model invariant. This
    makes ``(start_date, event_id)`` a total order, which is what allows order
    independence to hold. Catalogs may be empty.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A list of valid, frozen ``Event`` instances with distinct ``event_id``.
    """
    field_sets = draw(st.lists(event_fields(), max_size=60))
    return [Event(event_id=f'evt-{index}', **fields) for index, fields in enumerate(field_sets)]


@st.composite
def event_queries(draw: st.DrawFn) -> EventQuery:
    """Generate a varied ``EventQuery`` instance.

    Each filter dimension is independently present or absent, drawing values
    from the same small alphabet used by the event strategy so the resulting
    queries match a meaningful range of catalogs rather than always producing an
    empty matched set.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid ``EventQuery`` instance.
    """
    optional_small_text = st.none() | st.text(alphabet='abcXYZ ', max_size=4)
    return EventQuery(
        keyword=draw(optional_small_text),
        learning_levels=draw(
            st.lists(st.sampled_from(list(LearningLevel)), max_size=4, unique=True)
        ),
        location_mode=draw(st.none() | st.sampled_from(list(LocationMode))),
        location_text=draw(optional_small_text),
        event_type=draw(optional_small_text),
        partner=draw(optional_small_text),
        start_date=draw(st.none() | st.dates()),
        end_date=draw(st.none() | st.dates()),
    )


# Feature: aws-events-mcp, Property 18: Filtering is deterministic
@settings(max_examples=100)
@given(catalog=catalogs(), query=event_queries())
def test_repeated_query_yields_identical_ordered_result(
    catalog: list[Event], query: EventQuery
) -> None:
    """Applying the same query to the same catalog twice yields the same result.

    The two results must be equal element-by-element, including order, so the
    matched set is fully deterministic for a fixed input list.

    Validates: Requirements 2.1, 5.1
    """
    first = apply_query(catalog, query)
    second = apply_query(catalog, query)

    # Equal as ordered collections (element-by-element, including order).
    assert first == second
    assert [event.event_id for event in first] == [event.event_id for event in second]


# Feature: aws-events-mcp, Property 18: Filtering is deterministic
@settings(max_examples=100)
@given(catalog=catalogs(), query=event_queries(), data=st.data())
def test_shuffled_catalog_yields_same_ordered_result(
    catalog: list[Event], query: EventQuery, data: st.DataObject
) -> None:
    """A shuffled copy of the catalog produces the same ordered matched set.

    Ordering uses the total-order key ``(start_date, event_id)`` over a catalog
    with unique ``event_id`` values, so the matched, ordered output is
    independent of the catalog's input order.

    Validates: Requirements 2.1, 5.1
    """
    shuffled = data.draw(st.permutations(catalog))

    baseline = apply_query(catalog, query)
    from_shuffled = apply_query(list(shuffled), query)

    # Identical ordered collections despite the different input order.
    assert baseline == from_shuffled
    assert [event.event_id for event in baseline] == [event.event_id for event in from_shuffled]
