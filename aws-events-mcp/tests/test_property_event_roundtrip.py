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

"""Property-based test for the ``Event`` model JSON round-trip.

This module validates that serializing any valid ``Event`` to a JSON-compatible
object with ``model_dump(mode='json')`` and parsing it back with
``Event.model_validate(...)`` reproduces an equal ``Event`` (dates round-trip as
``YYYY-MM-DD`` strings and enums as their string values). It lives in its own
file so it can run in parallel with the other property tests.

Feature: aws-events-mcp, Property 1: Event model JSON round-trip
Validates: Requirements 10.2, 10.3
"""

from aws_events_mcp.models import Event, LearningLevel, LocationMode
from hypothesis import given, settings
from hypothesis import strategies as st


@st.composite
def events(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` instance.

    Required string fields (``event_id``, ``title``) are non-empty; optional
    fields are independently present or absent; text is drawn across the full
    unicode range with mixed case; ``start_date`` is any valid calendar date;
    ``location_mode`` is a valid ``LocationMode`` and ``learning_level`` is an
    optional ``LearningLevel``.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance.
    """
    optional_text = st.none() | st.text(max_size=80)
    return Event(
        event_id=draw(st.text(min_size=1, max_size=64)),
        title=draw(st.text(min_size=1, max_size=120)),
        description=draw(st.text(max_size=256)),
        start_date=draw(st.dates()),
        start_time=draw(st.none() | st.text(max_size=24)),
        time_zone=draw(st.none() | st.text(max_size=24)),
        location=draw(optional_text),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(optional_text),
        partner_name=draw(optional_text),
        registration_url=draw(optional_text),
        learn_more_url=draw(optional_text),
    )


# Feature: aws-events-mcp, Property 1: Event model JSON round-trip
@settings(max_examples=100)
@given(event=events())
def test_event_json_round_trip(event: Event) -> None:
    """Round-tripping any valid Event through JSON yields an equal Event.

    Validates: Requirements 10.2, 10.3
    """
    dumped = event.model_dump(mode='json')
    restored = Event.model_validate(dumped)
    assert restored == event
