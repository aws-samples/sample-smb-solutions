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

"""Unit tests for :class:`aws_events_mcp.catalog.CatalogCache` behavior.

These tests exercise the cache's freshness, single-flight, and parse-outcome
logic without any real network access by driving it with an in-memory fake
``EventCatalogSource`` that returns canned raw records and counts how many times
``fetch_raw_records`` is invoked. Wall-clock dependence is removed by
monkeypatching ``catalog.time.monotonic`` with a controllable clock, so TTL
expiry is asserted deterministically rather than by sleeping.

The behaviors covered are:

- TTL expiry triggers a refetch while a call within the TTL window reuses the
  cache (Requirement 2.4 — equivalent results from an unchanged source);
- a valid cache is reused without a refetch (``fetch_raw_records`` called once);
- concurrent cold-cache ``get_events`` calls collapse into a single upstream
  fetch (single-flight, design Caching Strategy);
- non-empty content that yields zero valid events raises
  ``CatalogUnparseableError`` (Requirements 10.7, 11.3);
- an empty-but-valid catalog returns an empty list and is not an error
  (Requirements 2.4, 10.7);
- a partial parse raises ``CatalogPartialParseError`` after caching the valid
  events, so a subsequent call within the TTL window returns them without a
  refetch (Requirement 11.4).

Validates: Requirements 2.4, 10.7, 11.3, 11.4.
"""

import asyncio
import pytest
from aws_events_mcp import catalog
from aws_events_mcp.catalog import CatalogCache
from aws_events_mcp.errors import CatalogPartialParseError, CatalogUnparseableError
from typing import Optional


def _valid_record(event_id: str, title: str = 'Sample Event') -> dict:
    """Build a raw record that the parser maps to a valid ``Event``.

    The parser flattens flat fixtures unchanged and always derives
    ``location_mode`` (defaulting to virtual), so only an identifier, a title,
    and a parseable start date are required for a record to parse successfully.

    Args:
        event_id: Value supplied under ``event_id`` (the unique identifier).
        title: Non-empty event title.

    Returns:
        A raw record dict that parses into a valid ``Event``.
    """
    return {'event_id': event_id, 'title': title, 'start_date': '2025-06-01'}


def _invalid_record(event_id: str) -> dict:
    """Build a raw record the parser skips (missing the required ``title``).

    Args:
        event_id: Value supplied under ``event_id`` so the skip warning can name
            the record.

    Returns:
        A raw record dict that fails ``Event`` validation and is skipped.
    """
    return {'event_id': event_id, 'start_date': '2025-06-01'}


class FakeCatalogSource:
    """In-memory ``EventCatalogSource`` returning canned records, counting calls.

    Attributes:
        calls: The number of times :meth:`fetch_raw_records` has been awaited.
    """

    def __init__(self, records: list[dict], *, delay: float = 0.0) -> None:
        """Initialize the fake source.

        Args:
            records: The canned raw records returned by each fetch.
            delay: Optional sleep (seconds) inside ``fetch_raw_records`` used to
                force overlap of concurrent callers for the single-flight test.
        """
        self._records = records
        self._delay = delay
        self.calls = 0

    async def fetch_raw_records(self) -> list[dict]:
        """Return a copy of the canned records, counting the invocation.

        Returns:
            A shallow copy of the configured raw records.
        """
        self.calls += 1
        if self._delay:
            await asyncio.sleep(self._delay)
        return list(self._records)


class FakeClock:
    """Controllable monotonic clock for deterministic TTL assertions.

    Attributes:
        now: The current fake monotonic time in seconds.
    """

    def __init__(self, start: float = 1000.0) -> None:
        """Initialize the clock.

        Args:
            start: The initial monotonic value.
        """
        self.now = start

    def monotonic(self) -> float:
        """Return the current fake monotonic time.

        Returns:
            The current value of ``now``.
        """
        return self.now

    def advance(self, seconds: float) -> None:
        """Advance the clock by ``seconds``.

        Args:
            seconds: The number of seconds to add to ``now``.
        """
        self.now += seconds


def _install_clock(monkeypatch: pytest.MonkeyPatch, clock: Optional[FakeClock]) -> FakeClock:
    """Patch ``catalog.time.monotonic`` with a controllable clock.

    Args:
        monkeypatch: The pytest monkeypatch fixture (auto-restored after the
            test).
        clock: The clock to install, or ``None`` to create a fresh one.

    Returns:
        The installed clock, so the test can advance it.
    """
    clock = clock if clock is not None else FakeClock()
    monkeypatch.setattr(catalog.time, 'monotonic', clock.monotonic)
    return clock


async def test_ttl_expiry_triggers_refetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """A call past the TTL refetches; a call within the TTL reuses the cache."""
    clock = _install_clock(monkeypatch, FakeClock())
    fake = FakeCatalogSource([_valid_record('e1')])
    cache = CatalogCache(fake, ttl_seconds=100)

    await cache.get_events()
    assert fake.calls == 1

    # Within the TTL window: reuse, no refetch.
    clock.advance(50)
    await cache.get_events()
    assert fake.calls == 1

    # Past the TTL window: refetch.
    clock.advance(100)
    await cache.get_events()
    assert fake.calls == 2


async def test_zero_ttl_always_refetches(monkeypatch: pytest.MonkeyPatch) -> None:
    """With ``ttl_seconds=0`` the cache is never fresh, so every call refetches."""
    _install_clock(monkeypatch, FakeClock())
    fake = FakeCatalogSource([_valid_record('e1')])
    cache = CatalogCache(fake, ttl_seconds=0)

    await cache.get_events()
    await cache.get_events()

    assert fake.calls == 2


async def test_valid_cache_reused_without_refetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Multiple calls within a large TTL fetch upstream exactly once."""
    _install_clock(monkeypatch, FakeClock())
    fake = FakeCatalogSource([_valid_record('e1'), _valid_record('e2')])
    cache = CatalogCache(fake, ttl_seconds=900)

    first = await cache.get_events()
    second = await cache.get_events()
    third = await cache.get_events()

    assert fake.calls == 1
    assert len(first) == 2
    assert first == second == third


async def test_force_refresh_bypasses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """``force_refresh=True`` refetches even when the cache is still fresh."""
    _install_clock(monkeypatch, FakeClock())
    fake = FakeCatalogSource([_valid_record('e1')])
    cache = CatalogCache(fake, ttl_seconds=900)

    await cache.get_events()
    await cache.get_events(force_refresh=True)

    assert fake.calls == 2


async def test_single_flight_concurrent_cold_cache() -> None:
    """Many concurrent cold-cache calls trigger only one upstream fetch.

    This test deliberately uses the real monotonic clock rather than the frozen
    ``FakeClock``: the single-flight overlap is forced with a real ``asyncio.sleep``
    inside the fake source, and ``asyncio.sleep`` depends on the event loop's
    monotonic clock, so freezing ``time.monotonic`` here would stall the loop.
    The large TTL keeps the cache fresh for the queued callers without any
    clock manipulation.
    """
    # The delay forces the gathered callers to overlap: the first holds the lock
    # across the await while the rest queue on it, then serve the fresh cache.
    fake = FakeCatalogSource([_valid_record('e1')], delay=0.05)
    cache = CatalogCache(fake, ttl_seconds=900)

    results = await asyncio.gather(*(cache.get_events() for _ in range(10)))

    assert fake.calls == 1
    assert all(result == results[0] for result in results)
    assert len(results[0]) == 1


async def test_zero_valid_content_raises_unparseable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-empty content that yields no valid events raises unparseable (Req 11.3)."""
    _install_clock(monkeypatch, FakeClock())
    fake = FakeCatalogSource([_invalid_record('bad1'), _invalid_record('bad2')])
    cache = CatalogCache(fake, ttl_seconds=900)

    with pytest.raises(CatalogUnparseableError):
        await cache.get_events()


async def test_empty_but_valid_returns_empty_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty-but-valid catalog returns an empty list, not an error (Req 2.4, 10.7)."""
    _install_clock(monkeypatch, FakeClock())
    fake = FakeCatalogSource([])
    cache = CatalogCache(fake, ttl_seconds=900)

    events = await cache.get_events()

    assert events == []
    assert fake.calls == 1


async def test_partial_parse_caches_then_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    """Partial parse raises after caching, so a later call reuses the valid events."""
    _install_clock(monkeypatch, FakeClock())
    fake = FakeCatalogSource([_valid_record('good'), _invalid_record('bad')])
    cache = CatalogCache(fake, ttl_seconds=900)

    # First call: one of two records parses -> partial-parse signal.
    with pytest.raises(CatalogPartialParseError):
        await cache.get_events()
    assert fake.calls == 1

    # The valid event was cached before the signal, so a subsequent call within
    # the TTL window returns it without a new upstream fetch (Req 11.4).
    events = await cache.get_events()
    assert fake.calls == 1
    assert len(events) == 1
    assert events[0].event_id == 'good'
