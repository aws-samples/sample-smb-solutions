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

"""Unit tests for ``Event`` model validation and serialization.

These example-based tests complement the Property 1 round-trip test by pinning
the model's validation constraints (Requirements 10.1, 10.5): ``event_id`` and
``title`` must be non-empty; ``location_mode`` and ``learning_level`` coerce
their accepted string values and reject anything else; ``start_date`` parses an
ISO 8601 ``YYYY-MM-DD`` string into a ``datetime.date`` and rejects malformed or
out-of-range dates; and absent optional fields serialize to explicit ``null``
under ``model_dump(mode='json')`` rather than being omitted (Requirement 9.3).
"""

import pytest
from aws_events_mcp.models import Event, LearningLevel, LocationMode
from datetime import date
from pydantic import ValidationError
from typing import Any


def _valid_kwargs(**overrides: Any) -> dict[str, Any]:
    """Build keyword arguments for a minimal valid ``Event``.

    Args:
        **overrides: Field values to override or add on top of the defaults.

    Returns:
        A mapping of constructor keyword arguments for ``Event``.
    """
    base: dict[str, Any] = {
        'event_id': 'evt-1',
        'title': 'Intro to AWS',
        'start_date': date(2025, 6, 1),
        'location_mode': LocationMode.VIRTUAL,
    }
    base.update(overrides)
    return base


class TestRequiredNonEmptyFields:
    """Non-empty constraints on ``event_id`` and ``title`` (Requirement 10.5)."""

    def test_minimal_valid_event_constructs(self):
        """A minimal event with the required fields validates successfully."""
        event = Event(**_valid_kwargs())
        assert event.event_id == 'evt-1'
        assert event.title == 'Intro to AWS'

    def test_empty_event_id_raises(self):
        """An empty ``event_id`` violates ``min_length=1``."""
        with pytest.raises(ValidationError) as exc_info:
            Event(**_valid_kwargs(event_id=''))
        assert 'event_id' in str(exc_info.value)

    def test_empty_title_raises(self):
        """An empty ``title`` violates ``min_length=1``."""
        with pytest.raises(ValidationError) as exc_info:
            Event(**_valid_kwargs(title=''))
        assert 'title' in str(exc_info.value)

    def test_missing_event_id_raises(self):
        """Omitting the required ``event_id`` raises a validation error."""
        kwargs = _valid_kwargs()
        del kwargs['event_id']
        with pytest.raises(ValidationError):
            Event(**kwargs)

    def test_missing_title_raises(self):
        """Omitting the required ``title`` raises a validation error."""
        kwargs = _valid_kwargs()
        del kwargs['title']
        with pytest.raises(ValidationError):
            Event(**kwargs)

    def test_description_defaults_to_empty_string(self):
        """``description`` is optional and defaults to an empty string."""
        event = Event(**_valid_kwargs())
        assert event.description == ''

    def test_description_may_be_empty_string(self):
        """An explicit empty ``description`` is accepted (it is not required)."""
        event = Event(**_valid_kwargs(description=''))
        assert event.description == ''


class TestEnumCoercion:
    """Enum coercion and rejection for ``LocationMode`` and ``LearningLevel``."""

    @pytest.mark.parametrize('value', ['virtual', 'physical'])
    def test_location_mode_accepts_string_values(self, value):
        """The accepted ``LocationMode`` string values coerce to the enum."""
        event = Event(**_valid_kwargs(location_mode=value))
        assert event.location_mode == LocationMode(value)
        assert isinstance(event.location_mode, LocationMode)

    def test_location_mode_accepts_enum_instance(self):
        """A ``LocationMode`` enum instance is accepted directly."""
        event = Event(**_valid_kwargs(location_mode=LocationMode.PHYSICAL))
        assert event.location_mode is LocationMode.PHYSICAL

    @pytest.mark.parametrize('value', ['Virtual', 'online', 'in-person', ''])
    def test_location_mode_rejects_invalid_values(self, value):
        """Values outside the accepted set are rejected (case-sensitive enum)."""
        with pytest.raises(ValidationError) as exc_info:
            Event(**_valid_kwargs(location_mode=value))
        assert 'location_mode' in str(exc_info.value)

    @pytest.mark.parametrize('value', ['Foundational', 'Intermediate', 'Advanced', 'Expert'])
    def test_learning_level_accepts_string_values(self, value):
        """The accepted ``LearningLevel`` string values coerce to the enum."""
        event = Event(**_valid_kwargs(learning_level=value))
        assert event.learning_level == LearningLevel(value)
        assert isinstance(event.learning_level, LearningLevel)

    def test_learning_level_defaults_to_none(self):
        """``learning_level`` is optional and defaults to ``None``."""
        event = Event(**_valid_kwargs())
        assert event.learning_level is None

    @pytest.mark.parametrize('value', ['100', 'foundational', 'Beginner', 'Master'])
    def test_learning_level_rejects_invalid_values(self, value):
        """Values outside the four accepted levels are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            Event(**_valid_kwargs(learning_level=value))
        assert 'learning_level' in str(exc_info.value)


class TestDateParsing:
    """``start_date`` parsing, rejection, and serialization edge cases."""

    def test_parses_iso_date_string(self):
        """An ISO ``YYYY-MM-DD`` string parses into a ``datetime.date``."""
        event = Event(**_valid_kwargs(start_date='2025-06-01'))
        assert event.start_date == date(2025, 6, 1)
        assert isinstance(event.start_date, date)

    def test_accepts_date_instance(self):
        """A ``datetime.date`` instance is accepted directly."""
        event = Event(**_valid_kwargs(start_date=date(2030, 12, 31)))
        assert event.start_date == date(2030, 12, 31)

    @pytest.mark.parametrize(
        'value',
        [
            '06/01/2025',  # wrong separator/order
            '2025-6-1',  # not zero-padded ISO
            '2025-13-01',  # month out of range
            '2025-02-30',  # day out of range for February
            'not-a-date',  # not a date at all
            '',  # empty string
        ],
    )
    def test_rejects_invalid_dates(self, value):
        """Malformed or out-of-range date values raise a validation error."""
        with pytest.raises(ValidationError) as exc_info:
            Event(**_valid_kwargs(start_date=value))
        assert 'start_date' in str(exc_info.value)

    def test_missing_start_date_raises(self):
        """``start_date`` is required; omitting it raises a validation error."""
        kwargs = _valid_kwargs()
        del kwargs['start_date']
        with pytest.raises(ValidationError):
            Event(**kwargs)

    def test_serializes_back_to_iso_string(self):
        """``model_dump(mode='json')`` renders the date as ``YYYY-MM-DD``."""
        event = Event(**_valid_kwargs(start_date=date(2025, 6, 1)))
        dumped = event.model_dump(mode='json')
        assert dumped['start_date'] == '2025-06-01'


class TestJsonSerialization:
    """JSON serialization of enums and absent optional fields."""

    def test_enums_serialize_to_string_values(self):
        """Enum fields serialize to their string values, not enum reprs."""
        event = Event(
            **_valid_kwargs(
                location_mode=LocationMode.PHYSICAL,
                learning_level=LearningLevel.ADVANCED,
            )
        )
        dumped = event.model_dump(mode='json')
        assert dumped['location_mode'] == 'physical'
        assert dumped['learning_level'] == 'Advanced'

    def test_absent_optional_fields_serialize_to_explicit_null(self):
        """Absent optional fields are present as explicit ``null`` (Req 9.3)."""
        event = Event(**_valid_kwargs())
        dumped = event.model_dump(mode='json')
        nullable_fields = [
            'start_time',
            'time_zone',
            'location',
            'learning_level',
            'event_type',
            'partner_name',
            'registration_url',
            'learn_more_url',
        ]
        for field in nullable_fields:
            assert field in dumped, f'{field} should be present in the JSON output'
            assert dumped[field] is None, f'{field} should serialize to null'

    def test_all_thirteen_fields_present_in_output(self):
        """Every model field is present in the serialized output."""
        event = Event(**_valid_kwargs())
        dumped = event.model_dump(mode='json')
        expected_fields = {
            'event_id',
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
        }
        assert set(dumped.keys()) == expected_fields


class TestFrozenModel:
    """The ``Event`` model is frozen so equality is value-based."""

    def test_is_immutable(self):
        """Assigning to a field on a frozen model raises a validation error."""
        event = Event(**_valid_kwargs())
        with pytest.raises(ValidationError):
            event.title = 'Changed'  # type: ignore[misc]

    def test_value_based_equality(self):
        """Two events with identical field values compare equal."""
        assert Event(**_valid_kwargs()) == Event(**_valid_kwargs())
