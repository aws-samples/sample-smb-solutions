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

"""Shared constants for the AWS Events MCP Server.

This module centralizes environment-variable names, default values, the
descriptive ``User-Agent`` string, and the upstream content-directory
configuration. It depends on nothing else in the package except the package
``__version__`` (used only to build the ``User-Agent`` template), keeping the
dependency hierarchy pointing inward and avoiding circular imports.

The upstream endpoint, directory identifier, and query parameters were
confirmed and pinned in task 8.2 by inspecting the live network contract behind
``https://aws.amazon.com/events/explore-aws-events/`` (see the "Confirmed
upstream contract" note below). Each upstream value remains overridable via an
environment variable so the concrete contract can be corrected without a code
change should AWS alter it.

Confirmed upstream contract (best-effort, verified against the live endpoint):
    - Endpoint: ``https://aws.amazon.com/api/dirs/items/search`` (the AWS
      content-directory search API; HTTP 200, JSON body).
    - Directory identifier: the alias ``alias#events-webinars-interactive-cards``
      (an alias that aggregates the many per-program event sub-directories),
      sent as the ``item.directoryId`` query parameter. The page's embedded
      directory configuration declares this id under ``dataSourceOptions``.
    - Query parameters: ``item.directoryId`` (the alias), ``item.locale``
      (``en_US``), ``size`` (page size, capped at 100 by the API), and ``page``
      (zero-based page index for upstream pagination).
    - Response shape: a JSON object ``{"items": [...], "metadata": {...},
      "fieldTypes": {...}}``. Each entry of ``items`` is a wrapper
      ``{"item": {...}, "tags": [...]}``. The record lives under ``item`` with a
      top-level ``id`` (the unique event identifier) and the event content under
      ``item.additionalFields`` (``title``, ``heading``, ``body``, ``bodyBack``,
      ``date``, ``publishedDate``, ``level``, ``location``, ``ctaLink``,
      ``primaryCTALink``, ...). Delivery mode and event category are carried by
      ``tags`` under the namespaces ``GLOBAL#aws-event-type`` (values
      ``on-demand``/``virtual``/``in-person``) and
      ``GLOBAL#local-tags-content-type``. ``metadata.totalHits`` reports the full
      match count (~1,369 at the time of pinning).

The record-to-``Event`` field mapping derived from this contract lives in
:mod:`aws_events_mcp.parser` (the single place the mapping is adjusted).
"""

import json
import os
from aws_events_mcp import __version__


# --- Environment-variable names ---------------------------------------------

#: Override for the in-memory catalog cache TTL, in seconds.
ENV_CACHE_TTL_SECONDS = 'AWS_EVENTS_CACHE_TTL_SECONDS'
#: Logging level for loguru, following the FastMCP convention.
ENV_LOG_LEVEL = 'FASTMCP_LOG_LEVEL'
#: Override for the MCP transport the server serves its tools over (Req 1.4/1.5).
ENV_TRANSPORT = 'FASTMCP_TRANSPORT'
#: Override for the upstream content-directory endpoint URL.
ENV_CATALOG_ENDPOINT_URL = 'AWS_EVENTS_CATALOG_ENDPOINT_URL'
#: Override for the upstream content-directory identifier.
ENV_CATALOG_DIRECTORY_ID = 'AWS_EVENTS_CATALOG_DIRECTORY_ID'
#: Override (JSON object string) for the upstream query parameters.
ENV_CATALOG_QUERY_PARAMS = 'AWS_EVENTS_CATALOG_QUERY_PARAMS'


# --- Defaults ----------------------------------------------------------------

#: Default catalog cache TTL window (15 minutes) per the design.
DEFAULT_CACHE_TTL_SECONDS = 900
#: Default page size applied when a tool is called without ``page_size`` (Req 3.1).
DEFAULT_PAGE_SIZE = 20
#: Smallest acceptable page size (Req 3.2/3.6).
MIN_PAGE_SIZE = 1
#: Largest acceptable page size (Req 2.1, 2.6, 3.2, 7.5).
MAX_PAGE_SIZE = 100
#: Total request timeout, in seconds, for upstream catalog calls (Req 11.2).
REQUEST_TIMEOUT_SECONDS = 30.0
#: Default logging level when ``FASTMCP_LOG_LEVEL`` is unset.
DEFAULT_LOG_LEVEL = 'INFO'


# --- Transport ---------------------------------------------------------------

#: Transports FastMCP can serve the registered tools over via
#: ``mcp.run(transport=...)`` (Requirements 1.4, 1.5).
SUPPORTED_TRANSPORTS: tuple[str, ...] = ('stdio', 'sse', 'streamable-http')
#: Transport used when ``FASTMCP_TRANSPORT`` is unset; ``stdio`` matches the
#: sibling project and the MCP stdio JSON-RPC convention (Req 1.4).
DEFAULT_TRANSPORT = 'stdio'


# --- User-Agent --------------------------------------------------------------

#: Template for the descriptive user-agent sent on every upstream request.
USER_AGENT_TEMPLATE = 'aws-events-mcp/{version}'
#: Descriptive user-agent identifying the server and its version (NFR Security).
USER_AGENT = USER_AGENT_TEMPLATE.format(version=__version__)


# --- Upstream content-directory configuration (confirmed/pinned, task 8.2) ---

#: Confirmed content-directory search endpoint backing the public catalog page.
DEFAULT_CATALOG_ENDPOINT_URL = 'https://aws.amazon.com/api/dirs/items/search'
#: Confirmed directory identifier: the alias aggregating the AWS Events
#: sub-directories, sent as the ``item.directoryId`` query parameter.
DEFAULT_CATALOG_DIRECTORY_ID = 'alias#events-webinars-interactive-cards'
#: Confirmed base upstream query parameters. ``item.directoryId`` and ``page``
#: are added per request by ``JsonApiCatalogSource``; ``size`` is capped at 100
#: by the API and ``item.locale`` selects the English catalog.
DEFAULT_CATALOG_QUERY_PARAMS: dict[str, str] = {
    'item.locale': 'en_US',
    'size': str(MAX_PAGE_SIZE),
}


def _resolve_query_params() -> dict[str, str]:
    """Resolve upstream query parameters, honoring the environment override.

    Returns:
        The default query parameters, or the parsed JSON object supplied via
        ``AWS_EVENTS_CATALOG_QUERY_PARAMS`` when that variable holds a valid
        JSON object. Invalid overrides fall back to the defaults.
    """
    raw = os.getenv(ENV_CATALOG_QUERY_PARAMS)
    if not raw:
        return dict(DEFAULT_CATALOG_QUERY_PARAMS)
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return dict(DEFAULT_CATALOG_QUERY_PARAMS)
    if not isinstance(parsed, dict):
        return dict(DEFAULT_CATALOG_QUERY_PARAMS)
    return {str(key): str(value) for key, value in parsed.items()}


#: Effective content-directory endpoint URL (env override applied at import).
CATALOG_ENDPOINT_URL = os.getenv(ENV_CATALOG_ENDPOINT_URL, DEFAULT_CATALOG_ENDPOINT_URL)
#: Effective content-directory identifier (env override applied at import).
CATALOG_DIRECTORY_ID = os.getenv(ENV_CATALOG_DIRECTORY_ID, DEFAULT_CATALOG_DIRECTORY_ID)
#: Effective upstream query parameters (env override applied at import).
CATALOG_QUERY_PARAMS = _resolve_query_params()
