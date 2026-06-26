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

"""Failure-mapping tests for :class:`aws_events_mcp.source.JsonApiCatalogSource`.

These tests confirm that transport-level and content-level failures from the
upstream content-directory API are translated into the correct typed
``CatalogSourceError`` subtype, without any real network access:

- a connection failure raises ``CatalogUnreachableError`` (Requirement 11.1);
- a request timeout raises ``CatalogTimeoutError`` (Requirement 11.2);
- a decodable response with no recognizable records, or an undecodable body,
  raises ``CatalogUnparseableError`` (Requirement 11.3);
- a non-success HTTP status raises ``CatalogUnreachableError`` (Requirement 11.1).

``JsonApiCatalogSource`` constructs its own ``httpx.AsyncClient`` internally, so
each test installs an ``httpx.MockTransport`` by patching the client class on the
``source`` module to inject the transport. ``MockTransport`` invokes its handler
for every request; the handler either returns a canned ``httpx.Response`` or
raises an ``httpx`` transport exception, which propagates through the client and
into the source's error-translation logic.

Validates: Requirements 11.1, 11.2, 11.3.
"""

import httpx
import pytest
from aws_events_mcp import source
from aws_events_mcp.errors import (
    CatalogTimeoutError,
    CatalogUnparseableError,
    CatalogUnreachableError,
)
from collections.abc import Callable


def _install_mock_transport(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    """Patch ``source.httpx.AsyncClient`` to inject an ``httpx.MockTransport``.

    The real ``AsyncClient`` is wrapped by a factory that forces the mock
    transport onto every constructed client, so the source's own client
    construction (with its timeout and headers) is preserved while no real
    socket is ever opened.

    Args:
        monkeypatch: The pytest monkeypatch fixture (auto-restored after the
            test).
        handler: A callable invoked for each request; it returns a canned
            response or raises an ``httpx`` transport exception.
    """
    real_client = httpx.AsyncClient

    def factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs['transport'] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(source.httpx, 'AsyncClient', factory)


async def test_connect_error_maps_to_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A connection failure surfaces as ``CatalogUnreachableError`` (Req 11.1)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('connection refused', request=request)

    _install_mock_transport(monkeypatch, handler)
    catalog_source = source.JsonApiCatalogSource()

    with pytest.raises(CatalogUnreachableError):
        await catalog_source.fetch_raw_records()


async def test_timeout_maps_to_timeout_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A request timeout surfaces as ``CatalogTimeoutError`` (Req 11.2)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout('request timed out', request=request)

    _install_mock_transport(monkeypatch, handler)
    catalog_source = source.JsonApiCatalogSource()

    with pytest.raises(CatalogTimeoutError):
        await catalog_source.fetch_raw_records()


async def test_connect_timeout_maps_to_timeout_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A connect timeout (subclass of both) maps to ``CatalogTimeoutError`` (Req 11.2)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout('connect timed out', request=request)

    _install_mock_transport(monkeypatch, handler)
    catalog_source = source.JsonApiCatalogSource()

    with pytest.raises(CatalogTimeoutError):
        await catalog_source.fetch_raw_records()


async def test_undecodable_body_maps_to_unparseable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 200 response whose body is not JSON raises ``CatalogUnparseableError`` (Req 11.3)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b'<<< not json at all >>>')

    _install_mock_transport(monkeypatch, handler)
    catalog_source = source.JsonApiCatalogSource()

    with pytest.raises(CatalogUnparseableError):
        await catalog_source.fetch_raw_records()


async def test_no_recognizable_records_maps_to_unparseable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valid JSON with no items container raises ``CatalogUnparseableError`` (Req 11.3)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={'unexpected': 'shape', 'metadata': {}})

    _install_mock_transport(monkeypatch, handler)
    catalog_source = source.JsonApiCatalogSource()

    with pytest.raises(CatalogUnparseableError):
        await catalog_source.fetch_raw_records()


async def test_non_success_status_maps_to_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-2xx HTTP status raises ``CatalogUnreachableError`` (Req 11.1)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text='internal server error')

    _install_mock_transport(monkeypatch, handler)
    catalog_source = source.JsonApiCatalogSource()

    with pytest.raises(CatalogUnreachableError):
        await catalog_source.fetch_raw_records()
