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

These exercise the single credential-free JSON call without any real network
access by patching ``source.httpx.AsyncClient`` to inject an
``httpx.MockTransport`` (the same technique as :mod:`tests.test_source`). The
handler serves an ``externalevent`` payload shaped ``{"future": [...], "past":
[...]}`` matching the live ``aws-experience.com`` contract.

The tests confirm:

- a successful call maps each event into a flat parser-ready record with the
  expected keys (namespaced id, split date/time, IANA time zone, delivery mode
  from ``settingDetails``, lowest numeric level);
- both the ``future`` and ``past`` buckets are consumed;
- an ``in-person`` setting yields a physical delivery mode;
- a body lacking both buckets raises ``CatalogUnparseableError``;
- a connection failure raises ``CatalogUnreachableError``;
- a request timeout raises ``CatalogTimeoutError``;
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


def _payload() -> dict:
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


def _make_handler(payload: object) -> Callable[[httpx.Request], httpx.Response]:
    """Build a request handler returning a fixed JSON payload.

    Args:
        payload: JSON body served by the external-event endpoint.

    Returns:
        A handler suitable for ``httpx.MockTransport``.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith('/api/externalevent')
        return httpx.Response(200, json=payload)

    return handler


async def test_successful_call_maps_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful call maps both buckets into flat, parser-ready records."""
    _install_mock_transport(monkeypatch, _make_handler(_payload()))
    cc = source.ConnectedCommunityCatalogSource(
        base_url='https://aws-experience.com', segment_path='amer/smb'
    )

    records = await cc.fetch_raw_records()

    assert len(records) == 2
    first = records[0]
    assert first['id'] == 'connected-community#48e49841-27d2-40fd-9949-ab84bb431854'
    assert first['title'] == 'Platform Engineering on EKS'
    # Plain-text summary is preferred over the HTML description.
    assert first['description'] == 'Hands-on workshop building an IDP on Amazon EKS.'
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
    _install_mock_transport(monkeypatch, _make_handler(_payload()))
    cc = source.ConnectedCommunityCatalogSource()

    records = await cc.fetch_raw_records()

    past = records[1]
    assert past['id'] == 'connected-community#past-0001'
    assert past['location_mode'] == 'physical'
    assert past['level'] == '100'


async def test_missing_buckets_maps_to_unparseable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A body lacking both buckets raises ``CatalogUnparseableError`` (Req 11.3)."""
    _install_mock_transport(monkeypatch, _make_handler({'unexpected': []}))
    cc = source.ConnectedCommunityCatalogSource()

    with pytest.raises(CatalogUnparseableError):
        await cc.fetch_raw_records()


async def test_empty_buckets_return_no_records(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reachable but empty buckets return an empty list, not an error."""
    _install_mock_transport(monkeypatch, _make_handler({'future': [], 'past': []}))
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
    _install_mock_transport(monkeypatch, _make_handler(_payload()))
    cc = source.ConnectedCommunityCatalogSource()

    records = await cc.fetch_raw_records()
    events, warnings = parse_events(records)

    assert warnings == []
    assert len(events) == 2
    event = events[0]
    assert event.event_id == 'connected-community#48e49841-27d2-40fd-9949-ab84bb431854'
    assert event.start_date.isoformat() == '2026-07-07'
    assert event.location_mode.value == 'virtual'
    assert event.learning_level.value == 'Intermediate'


def test_default_segment_path_matches_consts() -> None:
    """The default Connected Community segment path matches the pinned constant."""
    assert consts.DEFAULT_CONNECTED_COMMUNITY_SEGMENT_PATH == 'amer/smb'
    assert consts.DEFAULT_CONNECTED_COMMUNITY_BASE_URL == 'https://aws-experience.com'
