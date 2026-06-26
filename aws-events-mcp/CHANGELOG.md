# Changelog

All notable changes to the AWS Events MCP Server are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/aws-samples/sample-smb-solutions/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/aws-samples/sample-smb-solutions/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/aws-samples/sample-smb-solutions/releases/tag/v0.1.0
