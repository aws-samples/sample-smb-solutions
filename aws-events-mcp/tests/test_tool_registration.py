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

"""Tool-registration smoke test for the FastMCP instance.

This test introspects the live FastMCP application built in
``aws_events_mcp.server`` and asserts the contract Requirement 1 places on tool
registration and tool discovery: exactly the five expected catalog tools are
registered, each with a unique name, a non-empty description, and an input
schema (Requirements 1.1, 1.3).

Introspection uses the public MCP discovery API, ``await mcp.list_tools()``,
which returns the same :class:`mcp.types.Tool` objects an MCP client receives
when it lists tools. Each carries ``name``, ``description``, and ``inputSchema``
(the JSON Schema for the tool's arguments). The async call is driven from sync
tests via ``asyncio.run`` so no event-loop fixtures are required.

Beyond the core smoke assertions, the input schemas are checked against the
declared argument contract: ``search_events`` requires ``keyword`` and
``get_event_details`` requires ``event_id`` (both declared with ``Field(...)``),
and every listing/search tool exposes ``page_size`` with a default of 20 bounded
to the inclusive 1-100 range.

Validates: Requirements 1.1, 1.3
"""

import asyncio
from aws_events_mcp import consts
from aws_events_mcp.server import EXPECTED_TOOL_NAMES, mcp
from mcp.types import Tool
from typing import Dict, List


#: Tools that accept the shared listing/search arguments, including ``page_size``.
_LISTING_TOOL_NAMES = (
    'list_events',
    'list_upcoming_events',
    'search_events',
    'search_upcoming_events',
)


def _list_registered_tools() -> List[Tool]:
    """Return the tools exposed through the public MCP discovery API.

    Returns:
        The :class:`mcp.types.Tool` objects an MCP client would receive from a
        list-tools request, in registration order.
    """
    return asyncio.run(mcp.list_tools())


def _tools_by_name() -> Dict[str, Tool]:
    """Return the registered tools keyed by their unique name.

    Returns:
        A mapping from tool name to its :class:`mcp.types.Tool`.
    """
    return {tool.name: tool for tool in _list_registered_tools()}


def test_exactly_expected_tools_registered() -> None:
    """Exactly the five expected tools are registered: no extras, none missing.

    Validates: Requirements 1.1, 1.3
    """
    registered_names = {tool.name for tool in _list_registered_tools()}
    assert registered_names == set(EXPECTED_TOOL_NAMES)


def test_search_upcoming_events_is_registered() -> None:
    """``search_upcoming_events`` is registered as the fifth catalog tool.

    Asserts the tool is present with a unique name, a non-empty description, and
    a non-empty object input schema, bringing the server to five tools total.

    Validates: Requirements 1.1, 1.3
    """
    tools = _tools_by_name()
    assert 'search_upcoming_events' in tools, 'search_upcoming_events is not registered'
    assert len(tools) == 5, 'the server must expose exactly five tools'

    tool = tools['search_upcoming_events']
    assert tool.description is not None, 'search_upcoming_events has no description'
    assert tool.description.strip(), 'search_upcoming_events has a blank description'

    schema = tool.inputSchema
    assert isinstance(schema, dict), 'search_upcoming_events has no input schema'
    assert schema, 'search_upcoming_events has an empty input schema'
    assert schema.get('type') == 'object', 'search_upcoming_events schema is not an object'
    assert 'properties' in schema, 'search_upcoming_events schema declares no properties'


def test_tool_names_are_unique() -> None:
    """Every registered tool has a unique name (no duplicates).

    Validates: Requirements 1.1
    """
    names = [tool.name for tool in _list_registered_tools()]
    assert len(names) == len(set(names))


def test_every_tool_has_non_empty_description() -> None:
    """Every registered tool has a non-empty, non-whitespace description.

    Validates: Requirements 1.1, 1.3
    """
    for tool in _list_registered_tools():
        assert tool.description is not None, f'{tool.name} has no description'
        assert tool.description.strip(), f'{tool.name} has a blank description'


def test_every_tool_has_input_schema() -> None:
    """Every registered tool exposes a non-empty object input schema.

    Validates: Requirements 1.1, 1.3
    """
    for tool in _list_registered_tools():
        schema = tool.inputSchema
        assert isinstance(schema, dict), f'{tool.name} has no input schema'
        assert schema, f'{tool.name} has an empty input schema'
        assert schema.get('type') == 'object', f'{tool.name} schema is not an object'
        assert 'properties' in schema, f'{tool.name} schema declares no properties'


def test_search_events_requires_keyword() -> None:
    """The search_events input schema marks ``keyword`` as required.

    Validates: Requirements 1.1, 1.3
    """
    schema = _tools_by_name()['search_events'].inputSchema
    assert 'keyword' in schema['properties']
    assert 'keyword' in schema.get('required', [])


def test_get_event_details_requires_event_id() -> None:
    """The get_event_details input schema marks ``event_id`` as required.

    Validates: Requirements 1.1, 1.3
    """
    schema = _tools_by_name()['get_event_details'].inputSchema
    assert 'event_id' in schema['properties']
    assert 'event_id' in schema.get('required', [])


def test_listing_tools_expose_bounded_page_size() -> None:
    """Listing/search tools declare ``page_size`` with a default and 1-100 bounds.

    Validates: Requirements 1.1, 1.3
    """
    tools = _tools_by_name()
    for name in _LISTING_TOOL_NAMES:
        properties = tools[name].inputSchema['properties']
        assert 'page_size' in properties, f'{name} declares no page_size'
        page_size = properties['page_size']
        assert page_size.get('default') == consts.DEFAULT_PAGE_SIZE, (
            f'{name} page_size default is not {consts.DEFAULT_PAGE_SIZE}'
        )
        assert page_size.get('minimum') == 1, f'{name} page_size has no lower bound of 1'
        assert page_size.get('maximum') == 100, f'{name} page_size has no upper bound of 100'
