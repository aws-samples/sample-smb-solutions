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

"""Property-based test for the twelve-field ``get_event_details`` response.

This module validates that the response produced by
:func:`aws_events_mcp.server.build_event_details_response` always carries the
complete set of twelve presentation fields, regardless of which optional values
are absent on the underlying :class:`~aws_events_mcp.models.Event`. An absent
optional value must be present in the serialized event as an explicit ``null``
(``None``) rather than being omitted, so an MCP client can depend on a stable
response shape (Requirements 9.2, 9.3).

Beyond field presence, the test confirms the serialization contract the design
relies on: the ``start_date`` serializes to a ``YYYY-MM-DD`` string and the
``location_mode``/``learning_level`` enums serialize to their plain string
values (``model_dump(mode='json')`` semantics).

The generator independently draws each optional field as present or absent
(``start_time``, ``time_zone``, ``location``, ``learning_level``, ``event_type``,
``partner_name``, ``registration_url``, ``learn_more_url``) and lets
``description`` be empty, so the property covers every combination of present
and absent optional values. The required fields (``event_id``, ``title``,
``start_date``, ``location_mode``) are always populated. The response is built
through the real server helper rather than a reimplementation, so the actual
serialization path is exercised.

It lives in its own file so it can run in parallel with the other property
tests.

Feature: aws-events-mcp, Property 16: Event-details response always contains all twelve fields
Validates: Requirements 9.2, 9.3
"""

import re
from aws_events_mcp.models import Event, LearningLevel, LocationMode
from aws_events_mcp.server import build_event_details_response
from hypothesis import given, settings
from hypothesis import strategies as st


#: The twelve presentation fields that must always be present in the response
#: event, per Requirements 9.2 and 9.3.
_PRESENTATION_FIELDS = (
    'title',
    'description',
    'start_date',
    'start_time',
    'time_zone',
    'location',
    'location_mode',
    'learning_level',
    'event_type',
    'partner_name',
    'registration_url',
    'learn_more_url',
)

#: Optional fields that, when absent on the model, must serialize to an explicit
#: ``null`` (``None``) in the response rather than being omitted.
_NULLABLE_OPTIONAL_FIELDS = (
    'start_time',
    'time_zone',
    'location',
    'learning_level',
    'event_type',
    'partner_name',
    'registration_url',
    'learn_more_url',
)

#: ISO 8601 calendar-date shape the serialized ``start_date`` must conform to.
_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')


@st.composite
def events_with_arbitrary_optionals(draw: st.DrawFn) -> Event:
    """Generate a valid ``Event`` with each optional value independently absent.

    The required fields (``event_id``, ``title``, ``start_date``,
    ``location_mode``) are always populated. ``description`` may be an empty
    string. Every remaining optional field is independently present or absent
    (``None``), so the generated events cover all combinations of present and
    absent optional values.

    Args:
        draw: Hypothesis draw callable supplied by ``@st.composite``.

    Returns:
        A valid, frozen ``Event`` instance with arbitrary optional values.
    """
    text = st.text(alphabet='abcXYZ ', max_size=8)
    optional_text = st.none() | text
    return Event(
        event_id=draw(st.text(alphabet='abc012XYZ', min_size=1, max_size=6)),
        title=draw(st.text(alphabet='abcXYZ ', min_size=1, max_size=8)),
        description=draw(text),
        start_date=draw(st.dates()),
        start_time=draw(optional_text),
        time_zone=draw(optional_text),
        location=draw(optional_text),
        location_mode=draw(st.sampled_from(list(LocationMode))),
        learning_level=draw(st.none() | st.sampled_from(list(LearningLevel))),
        event_type=draw(optional_text),
        partner_name=draw(optional_text),
        registration_url=draw(optional_text),
        learn_more_url=draw(optional_text),
    )


# Feature: aws-events-mcp, Property 16: Event-details response always contains all twelve fields
@settings(max_examples=100)
@given(event=events_with_arbitrary_optionals())
def test_details_response_always_contains_twelve_fields(event: Event) -> None:
    """The details response includes all twelve fields, absent values as null.

    Confirms field presence (Requirement 9.2), explicit-null serialization of
    absent optionals (Requirement 9.3), and the JSON serialization contract:
    ``start_date`` as a ``YYYY-MM-DD`` string and the enums as their string
    values.

    Validates: Requirements 9.2, 9.3
    """
    response = build_event_details_response(event)

    assert response['status'] == 'success'
    returned_event = response['event']

    # All twelve presentation fields are present as keys, never omitted.
    for field in _PRESENTATION_FIELDS:
        assert field in returned_event, f'missing presentation field: {field}'

    # Absent optional values appear as explicit null rather than being omitted.
    for field in _NULLABLE_OPTIONAL_FIELDS:
        if getattr(event, field) is None:
            assert returned_event[field] is None, (
                f'absent optional field {field} should serialize to explicit null'
            )

    # The date serializes to a YYYY-MM-DD string equal to the model's date.
    serialized_date = returned_event['start_date']
    assert isinstance(serialized_date, str)
    assert _DATE_PATTERN.match(serialized_date), serialized_date
    assert serialized_date == event.start_date.isoformat()

    # The location_mode enum serializes to its plain string value.
    assert returned_event['location_mode'] == event.location_mode.value
    assert isinstance(returned_event['location_mode'], str)

    # When present, the learning_level enum serializes to its plain string value.
    if event.learning_level is not None:
        assert returned_event['learning_level'] == event.learning_level.value
        assert isinstance(returned_event['learning_level'], str)
