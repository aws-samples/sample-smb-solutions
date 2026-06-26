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

"""Startup failure-path unit tests for the FastMCP server (task 11.11).

These tests exercise the two fatal startup conditions the design calls out,
driving the small helpers ``main()`` composes rather than ``main()`` itself
(which would block on ``mcp.run``):

* ``_verify_tools_registered`` aborts startup with a :class:`StartupError`
  naming the offending tool when an expected tool is not registered
  (Requirement 1.2), while the default call over the live registry — where all
  four catalog tools are present with descriptions and input schemas — does not
  raise.
* ``_resolve_transport`` defaults to ``stdio`` when nothing is configured
  (Requirement 1.4) and aborts with a :class:`StartupError` listing the
  supported transports when a configured transport is unsupported
  (Requirement 1.5), while every supported transport is returned unchanged.

The transport-selection tests cover both the explicit ``configured`` argument
and the ``FASTMCP_TRANSPORT`` environment-variable path.
"""

import pytest
from aws_events_mcp import consts
from aws_events_mcp.server import (
    EXPECTED_TOOL_NAMES,
    StartupError,
    _resolve_transport,
    _verify_tools_registered,
)


class TestVerifyToolsRegistered:
    """Tests for ``_verify_tools_registered`` (Requirements 1.1, 1.2)."""

    def test_defaults_do_not_raise(self):
        """All four catalog tools are registered with usable schemas (Req 1.1)."""
        # Should not raise: the live FastMCP registry contains every expected
        # tool with a non-empty description and an input schema.
        _verify_tools_registered()

    def test_missing_tool_aborts_naming_the_tool(self):
        """A required-but-unregistered tool aborts startup naming it (Req 1.2)."""
        missing = 'nonexistent_tool'
        with pytest.raises(StartupError) as excinfo:
            _verify_tools_registered(expected_names=('list_events', missing))
        assert missing in str(excinfo.value)

    def test_missing_tool_message_indicates_registration_failure(self):
        """The error states registration failed (Req 1.2)."""
        with pytest.raises(StartupError) as excinfo:
            _verify_tools_registered(expected_names=('totally_made_up_tool',))
        message = str(excinfo.value)
        assert 'totally_made_up_tool' in message
        assert 'registration failed' in message.lower()

    def test_subset_of_real_tools_does_not_raise(self):
        """Verifying a subset of genuinely registered tools succeeds."""
        # A narrower expectation built only from real tool names must pass,
        # confirming the failure path above is driven by the missing name.
        _verify_tools_registered(expected_names=(EXPECTED_TOOL_NAMES[0],))


class TestResolveTransport:
    """Tests for ``_resolve_transport`` (Requirements 1.4, 1.5)."""

    def test_none_defaults_to_stdio(self, monkeypatch):
        """No configured transport defaults to stdio (Req 1.4)."""
        # Ensure the environment does not supply a transport either.
        monkeypatch.delenv(consts.ENV_TRANSPORT, raising=False)
        assert _resolve_transport(None) == 'stdio'
        assert _resolve_transport(None) == consts.DEFAULT_TRANSPORT

    @pytest.mark.parametrize('transport', list(consts.SUPPORTED_TRANSPORTS))
    def test_supported_transport_returned_unchanged(self, transport):
        """Every supported transport is accepted and returned (Req 1.4)."""
        assert _resolve_transport(transport) == transport

    def test_unsupported_transport_aborts(self):
        """An unsupported transport aborts startup (Req 1.5)."""
        with pytest.raises(StartupError) as excinfo:
            _resolve_transport('telepathy')
        message = str(excinfo.value)
        assert 'telepathy' in message
        assert 'supported transport is required' in message.lower()

    def test_unsupported_transport_lists_supported_values(self):
        """The error lists the supported transports (Req 1.5)."""
        with pytest.raises(StartupError) as excinfo:
            _resolve_transport('telepathy')
        message = str(excinfo.value)
        for supported in consts.SUPPORTED_TRANSPORTS:
            assert supported in message

    def test_env_var_unset_defaults_to_stdio(self, monkeypatch):
        """With FASTMCP_TRANSPORT unset, the default stdio is resolved (Req 1.4)."""
        monkeypatch.delenv(consts.ENV_TRANSPORT, raising=False)
        assert _resolve_transport() == 'stdio'

    @pytest.mark.parametrize('transport', list(consts.SUPPORTED_TRANSPORTS))
    def test_env_var_supported_transport(self, monkeypatch, transport):
        """A supported transport from the environment is honored (Req 1.4)."""
        monkeypatch.setenv(consts.ENV_TRANSPORT, transport)
        assert _resolve_transport() == transport

    def test_env_var_unsupported_transport_aborts(self, monkeypatch):
        """An unsupported transport from the environment aborts startup (Req 1.5)."""
        monkeypatch.setenv(consts.ENV_TRANSPORT, 'telepathy')
        with pytest.raises(StartupError) as excinfo:
            _resolve_transport()
        assert 'telepathy' in str(excinfo.value)

    def test_blank_env_var_defaults_to_stdio(self, monkeypatch):
        """A whitespace-only transport is treated as unconfigured (Req 1.4)."""
        monkeypatch.setenv(consts.ENV_TRANSPORT, '   ')
        assert _resolve_transport() == 'stdio'
