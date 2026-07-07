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

"""Tests for :class:`aws_events_mcp.source.BuilderLoftCatalogSource`.

These exercise the two-step guest-token flow without any real network access by
patching ``source.httpx.AsyncClient`` to inject an ``httpx.MockTransport`` (the
same technique as :mod:`tests.test_source`). The handler dispatches on the
request path: the calendar HTML shell serves an ``applicationSettings`` block
carrying the guest token, and the props endpoint serves the calendar events
JSON.

The tests confirm:

- a successful flow maps each event into a flat parser-ready record with the
  expected keys (namespaced id, split date/time, explicit physical mode);
- a shell with no extractable token raises ``CatalogUnparseableError``;
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


#: A valid 128-character lowercase-hex guest token for the extraction regex.
_TOKEN = '0123456789abcdef' * 8


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


def _html_shell(token: str | None) -> str:
    """Build a calendar HTML shell, optionally embedding a guest token.

    Args:
        token: The token to embed, or ``None`` to omit the authorization line.

    Returns:
        A minimal HTML document containing an ``applicationSettings`` block.
    """
    auth_line = f"  authorization: '{token}',\n" if token is not None else ''
    return (
        '<!doctype html><html><head><script>\n'
        'var applicationSettings = {\n'
        "  environment: 'production',\n"
        f'{auth_line}'
        "  locale: 'en_US'\n"
        '};\n'
        '</script></head><body></body></html>'
    )


def _props_body() -> dict:
    """Build a props payload with two Builder Loft events.

    Returns:
        A dict shaped like the calendar props response.
    """
    return {
        'calendar': {
            'events': {
                'uuid-1': {
                    'id': 'evt-1',
                    'status': 'PUBLISHED',
                    'startDate': '2026-08-17T09:30',
                    'endDate': '2026-08-17T11:00',
                    'title': 'Builder Session One',
                    'location': 'AWS Builder Loft',
                    'description': '<p>Hello <b>world</b>&amp; friends</p>',
                    'timeZone': 'resx:TimeZone.America_Los_Angeles',
                    'type': 'EVENT',
                },
                'uuid-2': {
                    'id': 'evt-2',
                    'status': 'PUBLISHED',
                    'startDate': '2026-09-01T14:00',
                    'endDate': '2026-09-01T15:30',
                    'title': 'Builder Session Two',
                    'location': 'AWS Builder Loft',
                    'description': '<p>Second session</p>',
                    'timeZone': 'resx:TimeZone.America_Los_Angeles',
                    'type': 'EVENT',
                },
            },
            'totalCount': 2,
        }
    }


def _make_handler(token: str | None, props: object) -> Callable[[httpx.Request], httpx.Response]:
    """Build a request handler dispatching on the request path.

    Args:
        token: Token to embed in the HTML shell (or ``None`` to omit it).
        props: JSON body served by the props endpoint.

    Returns:
        A handler suitable for ``httpx.MockTransport``.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if 'props' in request.url.path:
            return httpx.Response(200, json=props)
        return httpx.Response(200, text=_html_shell(token))

    return handler


async def test_successful_flow_maps_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful flow returns two flat, parser-ready records (Req 11.3 happy path)."""
    _install_mock_transport(monkeypatch, _make_handler(_TOKEN, _props_body()))
    loft = source.BuilderLoftCatalogSource(
        base_url='https://events.builder.aws.com', calendar_id='cal-123'
    )

    records = await loft.fetch_raw_records()

    assert len(records) == 2
    first = records[0]
    assert first['id'] == 'builder-loft#evt-1'
    assert first['title'] == 'Builder Session One'
    assert first['date'] == '2026-08-17'
    assert first['time'] == '09:30'
    assert first['location'] == 'AWS Builder Loft'
    assert first['location_mode'] == 'physical'
    assert first['learn_more_url'] == 'https://events.builder.aws.com/c/calendar/cal-123'
    # HTML is stripped and entities unescaped.
    assert first['description'] == 'Hello world & friends'
    # The resx placeholder time zone is not carried through.
    assert 'time_zone' not in first and 'timeZone' not in first


async def test_missing_token_maps_to_unparseable(monkeypatch: pytest.MonkeyPatch) -> None:
    """An HTML shell with no token raises ``CatalogUnparseableError`` (Req 11.3)."""
    _install_mock_transport(monkeypatch, _make_handler(None, _props_body()))
    loft = source.BuilderLoftCatalogSource()

    with pytest.raises(CatalogUnparseableError):
        await loft.fetch_raw_records()


async def test_missing_calendar_events_maps_to_unparseable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A props body without ``calendar.events`` raises ``CatalogUnparseableError``."""
    _install_mock_transport(monkeypatch, _make_handler(_TOKEN, {'calendar': {}}))
    loft = source.BuilderLoftCatalogSource()

    with pytest.raises(CatalogUnparseableError):
        await loft.fetch_raw_records()


async def test_connect_error_maps_to_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A connection failure surfaces as ``CatalogUnreachableError`` (Req 11.1)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('connection refused', request=request)

    _install_mock_transport(monkeypatch, handler)
    loft = source.BuilderLoftCatalogSource()

    with pytest.raises(CatalogUnreachableError):
        await loft.fetch_raw_records()


async def test_timeout_maps_to_timeout_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A request timeout surfaces as ``CatalogTimeoutError`` (Req 11.2)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout('request timed out', request=request)

    _install_mock_transport(monkeypatch, handler)
    loft = source.BuilderLoftCatalogSource()

    with pytest.raises(CatalogTimeoutError):
        await loft.fetch_raw_records()


async def test_mapped_records_parse_into_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """The mapped Builder Loft records parse cleanly into valid ``Event`` objects."""
    _install_mock_transport(monkeypatch, _make_handler(_TOKEN, _props_body()))
    loft = source.BuilderLoftCatalogSource(
        base_url='https://events.builder.aws.com', calendar_id='cal-123'
    )

    records = await loft.fetch_raw_records()
    events, warnings = parse_events(records)

    assert warnings == []
    assert len(events) == 2
    event = events[0]
    assert event.event_id == 'builder-loft#evt-1'
    assert event.start_date.isoformat() == '2026-08-17'
    assert event.location_mode.value == 'physical'


def test_default_calendar_id_matches_consts() -> None:
    """The default Builder Loft calendar id matches the pinned constant."""
    assert consts.DEFAULT_BUILDER_LOFT_CALENDAR_ID == 'fc4e2932-9284-4564-982e-8764e037c5a7'
