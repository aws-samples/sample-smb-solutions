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

"""Tests for :class:`aws_events_mcp.source.CompositeCatalogSource`.

The composite unions several sub-sources concurrently. These tests use small
in-memory fake sources (no network) to confirm:

- successful sub-sources are concatenated in order;
- overlapping record ids are de-duplicated (first occurrence wins);
- a mix of success and failure returns the successful records without raising
  (graceful degradation);
- when every sub-source fails, the first error is re-raised.
"""

import pytest
from aws_events_mcp.errors import CatalogTimeoutError, CatalogUnreachableError
from aws_events_mcp.source import CompositeCatalogSource


class _FakeSource:
    """A fake catalog source returning canned records or raising an error."""

    def __init__(
        self, records: list[dict] | None = None, *, error: Exception | None = None
    ) -> None:
        """Initialize the fake source.

        Args:
            records: Records to return from ``fetch_raw_records``.
            error: When set, ``fetch_raw_records`` raises this instead.
        """
        self._records = records or []
        self._error = error

    async def fetch_raw_records(self) -> list[dict]:
        """Return the canned records, or raise the configured error."""
        if self._error is not None:
            raise self._error
        return list(self._records)


async def test_concatenates_successful_sources() -> None:
    """Records from all successful sources are concatenated in source order."""
    composite = CompositeCatalogSource(
        [
            ('a', _FakeSource([{'id': 'a-1'}, {'id': 'a-2'}])),
            ('b', _FakeSource([{'id': 'b-1'}])),
        ]
    )

    records = await composite.fetch_raw_records()

    assert [record['id'] for record in records] == ['a-1', 'a-2', 'b-1']


async def test_deduplicates_overlapping_ids() -> None:
    """Duplicate record ids are dropped, keeping the first occurrence."""
    composite = CompositeCatalogSource(
        [
            ('a', _FakeSource([{'id': 'dup', 'src': 'a'}, {'id': 'a-only'}])),
            ('b', _FakeSource([{'id': 'dup', 'src': 'b'}, {'id': 'b-only'}])),
        ]
    )

    records = await composite.fetch_raw_records()

    assert [record['id'] for record in records] == ['dup', 'a-only', 'b-only']
    # First occurrence (source a) wins.
    assert records[0]['src'] == 'a'


async def test_partial_failure_returns_successful_records() -> None:
    """One failing source does not prevent returning the other's records."""
    composite = CompositeCatalogSource(
        [
            ('good', _FakeSource([{'id': 'g-1'}])),
            ('bad', _FakeSource(error=CatalogUnreachableError('down'))),
        ]
    )

    records = await composite.fetch_raw_records()

    assert [record['id'] for record in records] == ['g-1']


async def test_all_failures_reraise_first_error() -> None:
    """When every source fails, the first error is re-raised."""
    composite = CompositeCatalogSource(
        [
            ('first', _FakeSource(error=CatalogTimeoutError('timed out'))),
            ('second', _FakeSource(error=CatalogUnreachableError('down'))),
        ]
    )

    with pytest.raises(CatalogTimeoutError):
        await composite.fetch_raw_records()


async def test_empty_source_list_returns_empty() -> None:
    """A composite with no sources returns an empty list without raising."""
    composite = CompositeCatalogSource([])

    assert await composite.fetch_raw_records() == []
