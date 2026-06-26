# AWS Events MCP Server

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Docker Hub](https://img.shields.io/badge/docker-okclip%2Faws--events--mcp-blue.svg)](https://hub.docker.com/r/okclip/aws-events-mcp)

A Model Context Protocol (MCP) server that exposes the public
[AWS Events catalog](https://aws.amazon.com/events/explore-aws-events/) as a set of
programmatic tools for discovering, searching, and filtering AWS events — Tech Talks,
webinars, summits, roadshows, networking events, and partner events.

## Overview

The server lets an MCP client (an AI agent or application) discover, search, filter, and
inspect AWS events without scraping the catalog page itself. It fetches the public catalog,
parses each record into a validated event model, caches the result in memory, and serves all
filtering, searching, and pagination as fast, deterministic operations.

Key characteristics:

- **No AWS credentials required.** The server contacts only the public AWS Events catalog
  endpoint. It does not call AWS service APIs, so no `AWS_REGION`, `AWS_PROFILE`, or
  credentials of any kind are needed. This is a deliberate deviation from the sibling
  `aws-dms-troubleshoot-mcp-server`, which does require `AWS_REGION`/`AWS_PROFILE`.
- **Structured results.** Listings return ordered, paginated event collections with an
  accurate total count; failures return structured error responses rather than crashing.
- **Deterministic filtering.** Identical queries against an unchanged catalog return
  equivalent results.

## Features

- List AWS events ordered by start date, with optional filters
- Keyword search over event title and description
- Keyword search restricted to upcoming events only (start date on or after today, UTC)
- Filter by learning level, location (virtual/physical or free-text), date range, event
  type, and partner
- Convenience listing of upcoming events only (start date on or after today, UTC)
- Retrieve full details for a single event, including registration and learn-more links
- Bounded, paginated responses with opaque page tokens

## Installation

### Using uvx (recommended)

The simplest way to run the server is with [`uvx`](https://docs.astral.sh/uv/), which
downloads and runs the published package without a manual install:

```bash
uvx aws-events-mcp@latest
```

Alternatively, install it into your environment with `uv` or `pip`:

```bash
uv pip install aws-events-mcp
# or
pip install aws-events-mcp
```

All of these expose the `aws-events-mcp` console command.

### From Source

```bash
cd aws-events-mcp
uv sync
uv run aws-events-mcp
```

## Configuration

### Environment Variables

This server needs **no AWS credentials and no AWS configuration**. The only supported
environment variable is the log level:

```bash
# Optional: logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO
export FASTMCP_LOG_LEVEL=INFO
```

> **No AWS credentials or profile needed.** Unlike the sibling
> `aws-dms-troubleshoot-mcp-server` (which requires `AWS_REGION` and optionally
> `AWS_PROFILE`), this server only retrieves the public AWS Events catalog over HTTPS. Do not
> set `AWS_REGION`, `AWS_PROFILE`, or AWS access keys — they are ignored.

## Running the Server

The server can run two ways: directly via `uvx` (recommended), or inside a Docker container.
Both modes are configured below for a typical MCP client (e.g., Kiro, Claude Desktop).

### uvx mode

Run the published package directly:

```bash
uvx aws-events-mcp@latest
```

MCP client configuration snippet:

```json
{
  "mcpServers": {
    "aws-events": {
      "command": "uvx",
      "args": ["aws-events-mcp@latest"],
      "env": {
        "FASTMCP_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

> No AWS credentials or profile are required for this server — only the public AWS Events
> catalog is contacted. The `env` block intentionally omits `AWS_REGION` and `AWS_PROFILE`.

### Docker mode

You can either pull the published image from Docker Hub or build it locally.

**Option 1: Pull from Docker Hub (recommended)**

```bash
docker pull okclip/aws-events-mcp:latest
```

**Option 2: Build the image from the project root**

```bash
docker build -t okclip/aws-events-mcp:latest .
```

Run the container (MCP uses stdio, so run interactively and remove on exit):

```bash
docker run -i --rm okclip/aws-events-mcp:latest
```

MCP client configuration snippet:

```json
{
  "mcpServers": {
    "aws-events": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "okclip/aws-events-mcp:latest"],
      "env": {
        "FASTMCP_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

> No AWS credentials or profile are required for this server — only the public AWS Events
> catalog is contacted. There is no need to mount `~/.aws` or pass AWS environment variables
> into the container.

## Available Tools

All tools are asynchronous and return structured JSON responses. Listing and search tools
share the same optional filter arguments and the same pagination behavior.

### Shared filter and pagination arguments

These arguments are accepted by `list_events`, `list_upcoming_events`, `search_events`, and
`search_upcoming_events`:

- `learning_level` (string or list, optional): One to four learning levels. Accepted values
  (case-insensitive): `Foundational`, `Intermediate`, `Advanced`, `Expert`. Matches events
  whose level equals any supplied value.
- `location_mode` (string, optional): `virtual` (online events) or `physical` (in-person
  venue events).
- `location_text` (string, optional, 1–200 chars): Case-insensitive substring match against
  the event's location/venue field.
- `event_type` (string, optional, 1–256 chars): Case-insensitive exact match on the event
  category (e.g., Tech Talk, webinar, summit).
- `partner` (string, optional, 1–256 chars): Case-insensitive substring match on the partner
  name.
- `start_date` (string, optional, `YYYY-MM-DD`): Return only events whose start date is on or
  after this date.
- `end_date` (string, optional, `YYYY-MM-DD`): Return only events whose start date is on or
  before this date. Must not be earlier than `start_date`.
- `page_size` (integer, optional, 1–100): Maximum number of events in the response. Default
  20.
- `page_token` (string, optional): Opaque token from a previous response used to fetch the
  next page. Must match the query that produced it.

Multiple filters combine with AND. Results are always ordered by start date ascending.

### 1. list_events

List AWS events ordered by start date ascending, with all filters optional.

**Parameters:** all of the shared filter and pagination arguments above.

**Example:**

```python
await list_events(
    learning_level="Advanced",
    location_mode="virtual",
    page_size=20,
)
```

### 2. search_events

Keyword substring search over event title and description, combinable with all the shared
filters.

**Parameters:**
- `keyword` (string, required, 1–256 chars): Case-insensitive substring searched in each
  event's title or description.
- Plus all of the shared filter and pagination arguments above.

**Example:**

```python
await search_events(
    keyword="generative ai",
    learning_level=["Intermediate", "Advanced"],
    page_size=25,
)
```

### 3. list_upcoming_events

Convenience listing constrained to events whose start date is on or after the current date in
UTC. Otherwise identical to `list_events`.

**Parameters:** all of the shared filter and pagination arguments above (any supplied
`start_date` is additionally bounded by today's UTC date).

**Example:**

```python
await list_upcoming_events(
    location_text="Seattle",
    page_size=10,
)
```

### 4. search_upcoming_events

Keyword substring search (the search counterpart of `list_upcoming_events`) constrained to
events whose start date is on or after the current date in UTC. Otherwise identical to
`search_events`.

**Parameters:**
- `keyword` (string, required, 1–256 chars): Case-insensitive substring searched in each
  event's title or description (same as `search_events`).
- Plus all of the shared filter and pagination arguments above (any supplied `start_date` is
  additionally bounded by today's UTC date — the effective lower bound is the later of today
  (UTC) and `start_date`).

**Example:**

```python
await search_upcoming_events(
    keyword="generative ai",
    location_text="Seattle",
    page_size=10,
)
```

### 5. get_event_details

Return the single event whose identifier exactly matches the supplied value. The response
always includes all twelve presentation fields (title, description, start date, start time,
time zone, location, location mode, learning level, event type, partner name, registration
URL, learn-more URL); absent values are returned as explicit `null` rather than omitted. If no
event matches, a not-found result is returned.

**Parameters:**
- `event_id` (string, required): Non-empty unique identifier of the event to retrieve.

**Example:**

```python
await get_event_details(
    event_id="evt-12345",
)
```

## Response Shapes

Listing/search success:

```json
{
  "status": "success",
  "items": [ { "...event JSON..." } ],
  "total_count": 137,
  "page_size": 20,
  "next_page_token": "eyJvZmZzZXQiOjIwLC..."
}
```

`next_page_token` is present only when more matching events remain.

Validation error:

```json
{
  "status": "error",
  "error_type": "validation_error",
  "field": "page_size",
  "message": "page_size must be an integer between 1 and 100 inclusive.",
  "items": [],
  "total_count": 0
}
```

Source error:

```json
{
  "status": "error",
  "error_type": "source_unreachable",
  "message": "The AWS Events catalog could not be retrieved: connection failed.",
  "items": [],
  "total_count": 0
}
```

Not found (`get_event_details`):

```json
{ "status": "not_found", "message": "No event matched identifier 'abc123'.", "event": null }
```

## Development

```bash
cd aws-events-mcp
uv sync

# Run the test suite
uv run pytest

# Lint and type-check
uv run ruff check .
uv run pyright
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for
details.

## Resources

- [AWS Events catalog](https://aws.amazon.com/events/explore-aws-events/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Docker Hub image](https://hub.docker.com/r/okclip/aws-events-mcp)
- [MCP Servers Collection](https://github.com/awslabs/mcp)
- [GitHub Issues](https://github.com/aws-samples/sample-smb-solutions/issues)

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.
