# Changelog

All notable changes to the AWS Events MCP Server are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-07-07

### Added

- The catalog now unions a fourth upstream source: the AWS Connected Community events hub
  (`aws-experience.com`). `ConnectedCommunityCatalogSource` issues a single credential-free
  GET against `<base>/<segment>/api/externalevent` (default segment `amer/smb`), reads the
  `future` and `past` buckets, and maps each flat event object into the existing lenient
  parser's record shape (delivery mode from `settingDetails`, lowest numeric `levels` entry as
  the learning level, localized start time and IANA time zone preserved).
- Environment toggle `AWS_EVENTS_ENABLE_CONNECTED_COMMUNITY` (enabled by default) plus
  overrides for the Connected Community base URL (`AWS_EVENTS_CONNECTED_COMMUNITY_BASE_URL`)
  and region/segment path (`AWS_EVENTS_CONNECTED_COMMUNITY_SEGMENT_PATH`).

### Notes

- The Connected Community `externalevent` endpoint is region/segment scoped. The default
  `amer/smb` segment matches the referenced public page
  (`https://aws-experience.com/amer/smb/events`); other regions (e.g. `emea/smb`) return
  distinct, non-overlapping catalogs and can be selected via the segment-path override.

## [0.2.0] - 2026-07-07

### Added

- The catalog is now a union of three upstream sources: the existing AWS Events catalog, the
  AWS Summits interactive-cards hub (same content-directory API, different directory id), and
  the AWS Builder Loft calendar (Cvent-backed, retrieved via a guest-token props endpoint).
- `CompositeCatalogSource` fetches all enabled sources concurrently, concatenates and
  de-duplicates their records, and degrades gracefully: if some sources fail it logs a warning
  and returns the records from the sources that succeeded; only a total outage surfaces as a
  source error.
- `BuilderLoftCatalogSource` implements the two-step Builder Loft flow (scrape the short-lived
  guest bearer token from the calendar HTML shell, then read events from the calendar props
  JSON endpoint) and maps each event into the existing lenient parser's record shape.
- Environment toggles `AWS_EVENTS_ENABLE_SUMMITS` and `AWS_EVENTS_ENABLE_BUILDER_LOFT` (both
  enabled by default) plus overrides for the Summits directory id and Builder Loft calendar
  id / base URL.

### Notes

- The AWS Builder Loft source returns only its default upcoming set (~20 events) exposed by the
  guest-token props endpoint; full history pagination is not yet implemented.

## [0.1.2] - 2026-07-05

### Fixed

- A partial parse of the upstream catalog (some but not all records parse) now returns the
  successfully parsed events as a successful, degraded result instead of an empty
  `source_partial` error that discarded them. This fixes intermittent empty results. Only a
  wholly uninterpretable response (non-empty content, zero valid events) remains an error
  (`source_unparseable`).

### Changed

- The upstream catalog request now matches the live catalog page: it excludes third-party and
  archived items via `tags.id` exclusion filters and adds `sort_by`/`sort_order`, aligning the
  catalog size returned by the server with what the website shows.

## [0.1.1] - 2026-06-25

### Added

- `search_upcoming_events` — keyword substring search over title and description restricted to
  upcoming events (start date today UTC or later); the search counterpart of
  `list_upcoming_events`, combinable with every shared filter.

## [0.1.0] - 2025-01-02

### Added

- Initial release of the AWS Events MCP Server.
- MCP tools for the public AWS Events catalog:
  - `list_events` — list events ordered by start date with optional filters.
  - `search_events` — keyword substring search over title and description, combinable with
    filters.
  - `list_upcoming_events` — list events with a start date on or after today (UTC).
  - `get_event_details` — retrieve a single event with all twelve presentation fields always
    present.
- Filtering by learning level, location mode, location text, date range, event type, and
  partner; all filters combine conjunctively.
- Bounded, paginated responses (default page size 20, maximum 100) with opaque page tokens.
- In-memory catalog cache with a configurable TTL and single-flight refresh.
- Structured error responses for unreachable, timed-out, unparseable, and partially parseable
  upstream responses, and for invalid arguments.
- Operates without any AWS credentials or profile; contacts only the public AWS Events
  catalog endpoint.
- Distribution artifacts: `pyproject.toml` (uv/hatchling), Dockerfile, and standard
  documentation files (`README.md`, `CHANGELOG.md`, `LICENSE`, `NOTICE`).

[Unreleased]: https://github.com/aws-samples/sample-smb-solutions/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/aws-samples/sample-smb-solutions/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/aws-samples/sample-smb-solutions/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/aws-samples/sample-smb-solutions/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/aws-samples/sample-smb-solutions/releases/tag/v0.1.0
