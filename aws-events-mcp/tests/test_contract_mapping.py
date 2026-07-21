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

"""Unit tests for the pinned upstream content-directory contract (task 8.2).

These tests exercise the confirmed AWS content-directory contract end to end at
the boundary that depends on it: :func:`aws_events_mcp.source._extract_items`
and :func:`aws_events_mcp.source._extract_total_hits` against a sample payload
shaped exactly like the live response, and :func:`aws_events_mcp.parser.parse_events`
mapping the unwrapped records onto the ``Event`` model (id, title, normalized
date, learning level, resolved URLs, and tag-derived delivery mode).

The fixtures mirror real records captured from
``https://aws.amazon.com/api/dirs/items/search`` for the
``alias#events-webinars-interactive-cards`` directory; no network access occurs.

Validates: Requirements 10.1, 11.3, NFR Security.
"""

from aws_events_mcp.models import Event, LearningLevel, LocationMode
from aws_events_mcp.parser import parse_events
from aws_events_mcp.source import _extract_items, _extract_total_hits


#: A payload shaped exactly like the confirmed live API response.
_SAMPLE_PAYLOAD = {
    'metadata': {'count': 2, 'totalHits': 1369},
    'fieldTypes': {'title': 'string', 'level': 'string'},
    'items': [
        {
            'item': {
                'id': 'training-cards#aws-tc-twitch-from-start-to-certified',
                'locale': 'en_US',
                'directoryId': 'training-cards-interactive-get-certified-vilt-courses',
                'name': 'aws-tc-twitch-from-start-to-certified',
                'dateCreated': '2025-07-23T11:02:50+0000',
                'dateUpdated': '2025-11-06T23:48:35+0000',
                'additionalFields': {
                    'heading': 'Special Event - From Start to Certified',
                    'level': 'foundational',
                    'language': 'English',
                    'location': 'On-Demand Event',
                    'publishedDate': '2025-07-22T10:57:00Z',
                    'title': 'From Start to Certified: AWS AI Practitioner',
                    'body': 'AWS Training on Twitch',
                    'ctaLink': '/training/twitch/from-start-to-certified/',
                    'ctaLabel': 'Learn more',
                },
            },
            'tags': [
                {'tagNamespaceId': 'GLOBAL#aws-event-type', 'name': 'on-demand'},
                {'tagNamespaceId': 'GLOBAL#local-tags-content-type', 'name': 'webinar'},
            ],
        },
        {
            'item': {
                'id': 'events#summit-2026-new-york',
                'locale': 'en_US',
                'directoryId': 'events-interactive-cards',
                'name': 'summit-2026-new-york',
                'additionalFields': {
                    'title': 'AWS Summit New York 2026',
                    'level': 'intermediate',
                    'location': 'New York, NY',
                    'date': '2026-07-10',
                    'body': 'Join us in person.',
                    'primaryCTALink': 'https://example.com/register',
                    'primaryCTALabel': 'Register now',
                    'ctaLink': 'https://aws.amazon.com/summits/new-york/',
                },
            },
            'tags': [
                {'tagNamespaceId': 'GLOBAL#aws-event-type', 'name': 'in-person'},
            ],
        },
    ],
}


def test_extract_items_unwraps_and_attaches_tags() -> None:
    """``_extract_items`` unwraps each ``items[].item`` and attaches its tags."""
    records = _extract_items(_SAMPLE_PAYLOAD)

    assert records is not None
    assert len(records) == 2
    first = records[0]
    # The inner item is unwrapped (top-level id present, no wrapper nesting).
    assert first['id'] == 'training-cards#aws-tc-twitch-from-start-to-certified'
    assert 'additionalFields' in first
    # The wrapper's sibling tags are attached onto the unwrapped record.
    assert {tag['name'] for tag in first['tags']} == {'on-demand', 'webinar'}


def test_extract_items_returns_none_for_unrecognized_payload() -> None:
    """A payload with no items container is undecodable (drives Req 11.3)."""
    assert _extract_items({'unexpected': 'shape'}) is None


def test_extract_total_hits_reads_total_hits() -> None:
    """``_extract_total_hits`` reads ``metadata.totalHits`` (not page count)."""
    assert _extract_total_hits(_SAMPLE_PAYLOAD) == 1369


def test_parse_maps_confirmed_record_shape_onto_event() -> None:
    """An on-demand training record maps onto a fully-populated ``Event``."""
    records = _extract_items(_SAMPLE_PAYLOAD)
    assert records is not None

    events, warnings = parse_events(records)

    assert warnings == []
    assert len(events) == 2

    talk = events[0]
    assert isinstance(talk, Event)
    assert talk.event_id == 'training-cards#aws-tc-twitch-from-start-to-certified'
    assert talk.title == 'From Start to Certified: AWS AI Practitioner'
    assert talk.description == 'AWS Training on Twitch'
    # publishedDate (an ISO datetime) is normalized to a calendar date.
    assert talk.start_date.isoformat() == '2025-07-22'
    assert talk.learning_level is LearningLevel.FOUNDATIONAL
    # on-demand tag -> virtual delivery mode.
    assert talk.location_mode is LocationMode.VIRTUAL
    # content-type tag becomes the event type.
    assert talk.event_type == 'webinar'
    # Site-relative learn-more link resolves to an absolute aws.amazon.com URL.
    assert talk.learn_more_url == 'https://aws.amazon.com/training/twitch/from-start-to-certified/'


def test_parse_maps_in_person_summit_record() -> None:
    """An in-person summit record maps to physical mode with a registration URL."""
    records = _extract_items(_SAMPLE_PAYLOAD)
    assert records is not None

    events, _ = parse_events(records)
    summit = events[1]

    assert summit.event_id == 'events#summit-2026-new-york'
    assert summit.start_date.isoformat() == '2026-07-10'
    assert summit.learning_level is LearningLevel.INTERMEDIATE
    # in-person tag -> physical delivery mode.
    assert summit.location_mode is LocationMode.PHYSICAL
    assert summit.location == 'New York, NY'
    # primaryCTALink (already absolute) is the registration URL.
    assert summit.registration_url == 'https://example.com/register'


def test_parse_applies_fallback_link_when_record_has_none() -> None:
    """A record with no link of its own gets the catalog-page fallback link."""
    records = [
        {
            'id': 'events#no-link-record',
            'title': 'Linkless Event',
            'date': '2026-08-01',
        }
    ]

    events, warnings = parse_events(records)

    assert warnings == []
    assert len(events) == 1
    event = events[0]
    assert event.registration_url is None
    assert event.learn_more_url == 'https://aws.amazon.com/events/explore-aws-events/'


def test_parse_keeps_own_link_over_fallback() -> None:
    """A record with its own link never receives the fallback."""
    records = [
        {
            'id': 'events#linked-record',
            'title': 'Linked Event',
            'date': '2026-08-01',
            'registration_url': 'https://example.com/register',
        }
    ]

    events, _ = parse_events(records)

    assert events[0].registration_url == 'https://example.com/register'
    assert events[0].learn_more_url is None
