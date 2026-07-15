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

"""Tests for :class:`aws_events_mcp.source.ConnectedCommunityCatalogSource`.

These exercise the credential-free JSON calls without any real network access by
patching ``source.httpx.AsyncClient`` to inject an ``httpx.MockTransport`` (the
same technique as :mod:`tests.test_source`). The handler dispatches on the feed
path and serves ``externalevent`` and ``session`` payloads shaped ``{"future":
[...], "past": [...]}`` matching the live ``aws-experience.com`` contract.

The tests confirm:

- a successful call maps events from both feeds into flat parser-ready records
  with the expected keys (namespaced id, split date/time, IANA time zone,
  delivery mode from ``settingDetails``, lowest numeric level);
- both the ``future`` and ``past`` buckets of each feed are consumed;
- an ``in-person``/``physical`` setting yields a physical delivery mode with the
  venue extracted from ``settingDetails[].details``;
- ``session`` records resolve their ``urlSlug`` into the segment event page and
  map the ``type`` facet to a readable event type;
- one failing feed degrades gracefully (remaining feed's records returned);
- a body lacking both buckets on every feed raises ``CatalogUnparseableError``;
- a connection failure on every feed raises ``CatalogUnreachableError``;
- a request timeout on every feed raises ``CatalogTimeoutError``;
- the mapped records parse cleanly through :func:`parse_events`.

Validates: Requirements 11.1, 11.2, 11.3.
"""

import httpx
import pytest
from aws_events_mcp import consts, source
from aws_events_mcp.errors import (
    CatalogTimeoutError,
    CatalogUnparseableError,
    CatalogUnreachableError,
)
from aws_events_mcp.parser import parse_events
from collections.abc import Callable


def _install_mock_transport(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    """Patch ``source.httpx.AsyncClient`` to inject an ``httpx.MockTransport``.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        handler: Callable invoked for each request; returns a canned response or
            raises an ``httpx`` transport exception.
    """
    real_client = httpx.AsyncClient

    def factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs['transport'] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(source.httpx, 'AsyncClient', factory)


def _external_payload() -> dict:
    """Build an ``externalevent`` payload with a future and a past event.

    Returns:
        A dict shaped like the live Connected Community response.
    """
    return {
        'future': [
            {
                'id': '48e49841-27d2-40fd-9949-ab84bb431854',
                'title': 'Platform Engineering on EKS',
                'summary': 'Hands-on workshop building an IDP on Amazon EKS.',
                'description': '<p>Build an <b>IDP</b> on Amazon EKS.</p>',
                'startDate': '2026-07-07',
                'startInUtc': '2026-07-07T16:00',
                'startWithTimeZone': '2026-07-07T12:00-04:00',
                'timeZone': 'America/New_York',
                'settingDetails': [{'setting': 'virtual'}],
                'levels': ['300', '200'],
                'type': 'otherawsevent',
                'registrationUrl': 'https://aws-experience.com/emea/smb/e/6ebce/pe-on-eks',
            }
        ],
        'past': [
            {
                'id': 'past-0001',
                'title': 'Past In-Person Workshop',
                'summary': 'A concluded in-person session.',
                'description': '<p>Concluded.</p>',
                'startDate': '2026-05-01',
                'startWithTimeZone': '2026-05-01T09:00-07:00',
                'timeZone': 'America/Los_Angeles',
                'settingDetails': [{'setting': 'in-person'}],
                'levels': ['100'],
                'registrationUrl': 'https://aws-experience.com/amer/smb/e/abcde/past',
            }
        ],
    }


def _session_payload() -> dict:
    """Build a ``session`` payload with a virtual and a physical session.

    Returns:
        A dict shaped like the live Connected Community session feed.
    """
    return {
        'future': [
            {
                'id': '64028c30-5980-4ef5-8310-bf54efc7f695',
                'title': 'AI Dev Day - Claude Code',
                'summary': 'Build with Claude Code on AWS.',
                'startDate': '2026-07-23',
                'startWithTimeZone': '2026-07-23T14:00-04:00',
                'timeZone': 'America/New_York',
                'settingDetails': [{'setting': 'virtual'}],
                'levels': ['200', '300'],
                'type': 'handsonworkshop',
                'urlSlug': '64028/ai-dev-day-claude-code',
            },
            {
                'id': 'phys-0002',
                'title': 'AI-Assisted Development for Automotive',
                'summary': 'In-person activation day.',
                'startDate': '2026-07-16',
                'startWithTimeZone': '2026-07-16T09:00-04:00',
                'timeZone': 'America/New_York',
                'settingDetails': [
                    {
                        'setting': 'physical',
                        'details': {
                            'venue': 'other',
                            'address': '150 W. Jefferson Avenue, Detroit, MI 48226',
                            'location': {'id': 'detroit', 'type': 'city'},
                        },
                    }
                ],
                'levels': ['200'],
                'type': 'technicaltalk',
                'urlSlug': 'phys2/ai-assisted-development-automotive',
            },
        ],
        'past': [],
    }


def _make_handler(external: object, session: object) -> Callable[[httpx.Request], httpx.Response]:
    """Build a request handler dispatching on the feed path.

    Args:
        external: JSON body served by the ``externalevent`` endpoint.
        session: JSON body served by the ``session`` endpoint.

    Returns:
        A handler suitable for ``httpx.MockTransport``.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith('/api/externalevent'):
            return httpx.Response(200, json=external)
        assert request.url.path.endswith('/api/session')
        return httpx.Response(200, json=session)

    return handler


async def test_successful_call_maps_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful call maps both feeds' buckets into flat, parser-ready records."""
    _install_mock_transport(monkeypatch, _make_handler(_external_payload(), _session_payload()))
    cc = source.ConnectedCommunityCatalogSource(
        base_url='https://aws-experience.com', segment_path='amer/smb'
    )

    records = await cc.fetch_raw_records()

    # 2 externalevent records + 2 session records.
    assert len(records) == 4
    first = records[0]
    assert first['id'] == 'connected-community#48e49841-27d2-40fd-9949-ab84bb431854'
    assert first['title'] == 'Platform Engineering on EKS'
    # Plain-text summary leads, followed by the stripped HTML description, so
    # keyword search covers both (matching the site's own search behavior).
    assert first['description'] == (
        'Hands-on workshop building an IDP on Amazon EKS. Build an IDP on Amazon EKS.'
    )
    assert first['date'] == '2026-07-07'
    # Local time recovered from startWithTimeZone (offset dropped).
    assert first['time'] == '12:00'
    assert first['timezone'] == 'America/New_York'
    assert first['location_mode'] == 'virtual'
    # Lowest numeric level is passed through.
    assert first['level'] == '200'
    assert first['registration_url'] == 'https://aws-experience.com/emea/smb/e/6ebce/pe-on-eks'


async def test_in_person_setting_yields_physical(monkeypatch: pytest.MonkeyPatch) -> None:
    """An ``in-person`` setting maps to a physical delivery mode."""
    _install_mock_transport(monkeypatch, _make_handler(_external_payload(), _session_payload()))
    cc = source.ConnectedCommunityCatalogSource()

    records = await cc.fetch_raw_records()

    by_id = {record['id']: record for record in records}
    past = by_id['connected-community#past-0001']
    assert past['location_mode'] == 'physical'
    assert past['level'] == '100'


async def test_session_records_resolve_slug_type_and_venue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Session records resolve ``urlSlug`` links, the type facet, and the venue."""
    _install_mock_transport(monkeypatch, _make_handler(_external_payload(), _session_payload()))
    cc = source.ConnectedCommunityCatalogSource(
        base_url='https://aws-experience.com', segment_path='amer/smb'
    )

    records = await cc.fetch_raw_records()
    by_id = {record['id']: record for record in records}

    workshop = by_id['connected-community#64028c30-5980-4ef5-8310-bf54efc7f695']
    assert workshop['registration_url'] == (
        'https://aws-experience.com/amer/smb/e/64028/ai-dev-day-claude-code'
    )
    assert workshop['event_type'] == 'Hands-on Workshop'
    assert workshop['location_mode'] == 'virtual'

    physical = by_id['connected-community#phys-0002']
    assert physical['location_mode'] == 'physical'
    assert physical['location'] == '150 W. Jefferson Avenue, Detroit, MI 48226'
    assert physical['event_type'] == 'Tech Talk'


async def test_one_failing_feed_degrades_gracefully(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing session feed still returns the externalevent records."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith('/api/externalevent'):
            return httpx.Response(200, json=_external_payload())
        raise httpx.ConnectError('connection refused', request=request)

    _install_mock_transport(monkeypatch, handler)
    cc = source.ConnectedCommunityCatalogSource()

    records = await cc.fetch_raw_records()

    assert len(records) == 2
    assert all(record['id'].startswith('connected-community#') for record in records)


async def test_missing_buckets_maps_to_unparseable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bodies lacking both buckets on every feed raise ``CatalogUnparseableError``."""
    _install_mock_transport(monkeypatch, _make_handler({'unexpected': []}, {'nope': []}))
    cc = source.ConnectedCommunityCatalogSource()

    with pytest.raises(CatalogUnparseableError):
        await cc.fetch_raw_records()


async def test_empty_buckets_return_no_records(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reachable but empty buckets on both feeds return an empty list, not an error."""
    empty = {'future': [], 'past': []}
    _install_mock_transport(monkeypatch, _make_handler(empty, dict(empty)))
    cc = source.ConnectedCommunityCatalogSource()

    assert await cc.fetch_raw_records() == []


async def test_connect_error_maps_to_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A connection failure surfaces as ``CatalogUnreachableError`` (Req 11.1)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('connection refused', request=request)

    _install_mock_transport(monkeypatch, handler)
    cc = source.ConnectedCommunityCatalogSource()

    with pytest.raises(CatalogUnreachableError):
        await cc.fetch_raw_records()


async def test_timeout_maps_to_timeout_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A request timeout surfaces as ``CatalogTimeoutError`` (Req 11.2)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout('request timed out', request=request)

    _install_mock_transport(monkeypatch, handler)
    cc = source.ConnectedCommunityCatalogSource()

    with pytest.raises(CatalogTimeoutError):
        await cc.fetch_raw_records()


async def test_mapped_records_parse_into_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """The mapped Connected Community records parse cleanly into ``Event`` objects."""
    _install_mock_transport(monkeypatch, _make_handler(_external_payload(), _session_payload()))
    cc = source.ConnectedCommunityCatalogSource()

    records = await cc.fetch_raw_records()
    events, warnings = parse_events(records)

    assert warnings == []
    assert len(events) == 4
    event = events[0]
    assert event.event_id == 'connected-community#48e49841-27d2-40fd-9949-ab84bb431854'
    assert event.start_date.isoformat() == '2026-07-07'
    assert event.location_mode.value == 'virtual'
    assert event.learning_level.value == 'Intermediate'


def test_default_segment_path_matches_consts() -> None:
    """The default Connected Community segment path matches the pinned constant."""
    assert consts.DEFAULT_CONNECTED_COMMUNITY_SEGMENT_PATH == 'amer/smb'
    assert consts.DEFAULT_CONNECTED_COMMUNITY_BASE_URL == 'https://aws-experience.com'
