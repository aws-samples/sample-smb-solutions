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

"""Pydantic data models for the AWS Events MCP Server.

This module defines the typed representation of an AWS event and the supporting
query/page models, plus the two enumerations (``LearningLevel`` and
``LocationMode``) used across the server. JSON serialization and deserialization
live here so the rest of the package depends only on these models.

The ``Event`` model is frozen (``ConfigDict(frozen=True)``) so equality is
value-based, which supports the JSON round-trip property and deterministic
comparison used by the query and pagination layers. Serialization uses
``model_dump(mode='json')`` (dates as ``YYYY-MM-DD`` strings and enums as their
string values) and deserialization uses ``Event.model_validate(...)``; these two
operations are exact inverses for valid instances (Requirement 10.3).
"""

from datetime import date
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class LearningLevel(str, Enum):
    """Audience experience level of an event.

    Values mirror the AWS catalog levels: Foundational (100), Intermediate
    (200), Advanced (300), and Expert (400). Inherits from ``str`` so instances
    serialize to their string value under ``model_dump(mode='json')``.
    """

    FOUNDATIONAL = 'Foundational'
    INTERMEDIATE = 'Intermediate'
    ADVANCED = 'Advanced'
    EXPERT = 'Expert'


class LocationMode(str, Enum):
    """Delivery mode of an event.

    ``VIRTUAL`` denotes an online event and ``PHYSICAL`` denotes an in-person
    event at a venue. Inherits from ``str`` so instances serialize to their
    string value under ``model_dump(mode='json')``.
    """

    VIRTUAL = 'virtual'
    PHYSICAL = 'physical'


class Event(BaseModel):
    """A single AWS event record.

    Frozen so equality is value-based and instances are hashable, supporting the
    JSON round-trip property (Requirement 10.3) and deterministic comparison in
    the query/pagination layers. ``event_id`` and ``title`` are required and
    non-empty; ``description`` may be an empty string; all remaining fields are
    optional and serialize to explicit ``null`` when absent (Requirement 9.3).

    Attributes:
        event_id: Unique, non-empty identifier for the event.
        title: Non-empty event title.
        description: Event description; may be an empty string.
        start_date: ISO 8601 calendar date; the primary sort key.
        start_time: Local start time as published, if any.
        time_zone: Time-zone label (e.g. ``PDT``, ``UTC``), if any.
        location: Free-text location or venue, if any.
        location_mode: Whether the event is virtual or physical.
        learning_level: Audience experience level, if any.
        event_type: Category such as Tech Talk, webinar, or summit, if any.
        partner_name: Partner name, if any.
        registration_url: Registration link, if any.
        learn_more_url: Details link, if any.
    """

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=1, description='Unique, non-empty event identifier.')
    title: str = Field(min_length=1, description='Non-empty event title.')
    description: str = Field(default='', description='Event description; may be empty.')
    start_date: date = Field(description='ISO 8601 calendar date; primary sort key.')
    start_time: str | None = Field(default=None, description='Local start time as published.')
    time_zone: str | None = Field(default=None, description='Time-zone label, e.g. PDT or UTC.')
    location: str | None = Field(default=None, description='Free-text location or venue.')
    location_mode: LocationMode = Field(description='Virtual or physical delivery mode.')
    learning_level: LearningLevel | None = Field(
        default=None, description='Audience experience level.'
    )
    event_type: str | None = Field(default=None, description='Event category, e.g. Tech Talk.')
    partner_name: str | None = Field(default=None, description='Partner name, if any.')
    registration_url: str | None = Field(default=None, description='Registration link.')
    learn_more_url: str | None = Field(default=None, description='Details link.')


class EventQuery(BaseModel):
    """Normalized set of filters applied to the catalog by the query engine.

    Every field is optional; an unset field imposes no constraint along that
    dimension (Requirements 5.5, 6.4). Filtering rules are applied by
    ``query.apply_query``.

    Attributes:
        keyword: Case-insensitive substring matched against title or
            description.
        learning_levels: One to four learning levels; an event matches if its
            level equals any supplied value.
        location_mode: Restrict to virtual or physical events.
        location_text: Case-insensitive substring matched against the location.
        event_type: Case-insensitive exact match against the event type.
        partner: Case-insensitive substring matched against the partner name.
        start_date: Inclusive lower bound on the event start date.
        end_date: Inclusive upper bound on the event start date.
    """

    keyword: str | None = None
    learning_levels: list[LearningLevel] = Field(default_factory=list)
    location_mode: LocationMode | None = None
    location_text: str | None = None
    event_type: str | None = None
    partner: str | None = None
    start_date: date | None = None
    end_date: date | None = None


class EventPage(BaseModel):
    """A single page of matched events plus paging metadata.

    Attributes:
        items: The events on this page (at most the applied page size).
        total_count: Non-negative count of all events matching the query,
            independent of the page size (Requirements 2.3, 4.2).
        next_offset: Offset for the next page, or ``None`` when no further page
            remains.
    """

    items: list[Event]
    total_count: int
    next_offset: int | None = None
