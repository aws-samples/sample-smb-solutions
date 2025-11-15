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

"""Enhanced tests for tools_advanced module - targeting missing coverage lines."""

import pytest
from awslabs.aws_dms_mcp_server.config import DMSServerConfig
from awslabs.aws_dms_mcp_server.tools_advanced import (
    register_fleet_advisor_tools,
    register_metadata_model_tools,
    register_recommendation_tools,
)
from unittest.mock import Mock


class TestMetadataModelToolsExceptionCoverage:
    """Test exception handling in metadata model tools."""

    @pytest.fixture
    def mock_mcp(self):
        """Create mock MCP server object."""
        mcp = Mock()
        mcp.registered_tools = {}

        def tool_decorator():
            def wrapper(func):
                mcp.registered_tools[func.__name__] = func
                return func

            return wrapper

        mcp.tool = tool_decorator
        return mcp

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = Mock(spec=DMSServerConfig)
        config.read_only_mode = False
        return config

    @pytest.fixture
    def mock_metadata_model_manager(self):
        """Create mock metadata model manager."""
        return Mock()

    @pytest.fixture
    def setup_tools(self, mock_mcp, mock_config, mock_metadata_model_manager):
        """Setup and register tools."""
        register_metadata_model_tools(mock_mcp, mock_config, mock_metadata_model_manager)
        return mock_mcp, mock_config, mock_metadata_model_manager

    def test_describe_extension_pack_associations_exception(self, setup_tools):
        """Test exception handling in describe_extension_pack_associations."""
        mcp, config, manager = setup_tools

        manager.describe_extension_pack_associations.side_effect = ValueError('Test error')

        func = mcp.registered_tools['describe_extension_pack_associations']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Test error' in result['error']['message']

    def test_start_extension_pack_association_exception(self, setup_tools):
        """Test exception handling in start_extension_pack_association."""
        mcp, config, manager = setup_tools

        manager.start_extension_pack_association.side_effect = RuntimeError('Network error')

        func = mcp.registered_tools['start_extension_pack_association']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Network error' in result['error']['message']

    def test_describe_metadata_model_assessments_exception(self, setup_tools):
        """Test exception handling in describe_metadata_model_assessments."""
        mcp, config, manager = setup_tools

        manager.describe_metadata_model_assessments.side_effect = ConnectionError(
            'API unreachable'
        )

        func = mcp.registered_tools['describe_metadata_model_assessments']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'API unreachable' in result['error']['message']

    def test_start_metadata_model_assessment_exception(self, setup_tools):
        """Test exception handling in start_metadata_model_assessment."""
        mcp, config, manager = setup_tools

        manager.start_metadata_model_assessment.side_effect = IOError('Disk error')

        func = mcp.registered_tools['start_metadata_model_assessment']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False
        assert 'Disk error' in result['error']['message']

    def test_describe_metadata_model_conversions_exception(self, setup_tools):
        """Test exception handling in describe_metadata_model_conversions."""
        mcp, config, manager = setup_tools

        manager.describe_metadata_model_conversions.side_effect = TimeoutError('Request timeout')

        func = mcp.registered_tools['describe_metadata_model_conversions']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Request timeout' in result['error']['message']

    def test_start_metadata_model_conversion_exception(self, setup_tools):
        """Test exception handling in start_metadata_model_conversion."""
        mcp, config, manager = setup_tools

        manager.start_metadata_model_conversion.side_effect = KeyError('Invalid key')

        func = mcp.registered_tools['start_metadata_model_conversion']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False
        assert 'Invalid key' in result['error']['message']

    def test_describe_metadata_model_exports_as_script_exception(self, setup_tools):
        """Test exception handling in describe_metadata_model_exports_as_script."""
        mcp, config, manager = setup_tools

        manager.describe_metadata_model_exports_as_script.side_effect = AttributeError(
            'Missing attribute'
        )

        func = mcp.registered_tools['describe_metadata_model_exports_as_script']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Missing attribute' in result['error']['message']

    def test_start_metadata_model_export_as_script_exception(self, setup_tools):
        """Test exception handling in start_metadata_model_export_as_script."""
        mcp, config, manager = setup_tools

        manager.start_metadata_model_export_as_script.side_effect = PermissionError(
            'Access denied'
        )

        func = mcp.registered_tools['start_metadata_model_export_as_script']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', 'SOURCE')

        assert result['success'] is False
        assert 'Access denied' in result['error']['message']

    def test_describe_metadata_model_exports_to_target_exception(self, setup_tools):
        """Test exception handling in describe_metadata_model_exports_to_target."""
        mcp, config, manager = setup_tools

        manager.describe_metadata_model_exports_to_target.side_effect = TypeError('Type mismatch')

        func = mcp.registered_tools['describe_metadata_model_exports_to_target']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Type mismatch' in result['error']['message']

    def test_start_metadata_model_export_to_target_exception(self, setup_tools):
        """Test exception handling in start_metadata_model_export_to_target."""
        mcp, config, manager = setup_tools

        manager.start_metadata_model_export_to_target.side_effect = OSError('OS error')

        func = mcp.registered_tools['start_metadata_model_export_to_target']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False
        assert 'OS error' in result['error']['message']

    def test_describe_metadata_model_imports_exception(self, setup_tools):
        """Test exception handling in describe_metadata_model_imports."""
        mcp, config, manager = setup_tools

        manager.describe_metadata_model_imports.side_effect = IndexError('Index out of range')

        func = mcp.registered_tools['describe_metadata_model_imports']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Index out of range' in result['error']['message']

    def test_start_metadata_model_import_exception(self, setup_tools):
        """Test exception handling in start_metadata_model_import."""
        mcp, config, manager = setup_tools

        manager.start_metadata_model_import.side_effect = MemoryError('Out of memory')

        func = mcp.registered_tools['start_metadata_model_import']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', 'SOURCE')

        assert result['success'] is False
        assert 'Out of memory' in result['error']['message']

    def test_export_metadata_model_assessment_exception(self, setup_tools):
        """Test exception handling in export_metadata_model_assessment."""
        mcp, config, manager = setup_tools

        manager.export_metadata_model_assessment.side_effect = BufferError('Buffer overflow')

        func = mcp.registered_tools['export_metadata_model_assessment']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False
        assert 'Buffer overflow' in result['error']['message']


class TestFleetAdvisorToolsExceptionCoverage:
    """Test exception handling in Fleet Advisor tools."""

    @pytest.fixture
    def mock_mcp(self):
        """Create mock MCP server object."""
        mcp = Mock()
        mcp.registered_tools = {}

        def tool_decorator():
            def wrapper(func):
                mcp.registered_tools[func.__name__] = func
                return func

            return wrapper

        mcp.tool = tool_decorator
        return mcp

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = Mock(spec=DMSServerConfig)
        config.read_only_mode = False
        return config

    @pytest.fixture
    def mock_fleet_advisor_manager(self):
        """Create mock Fleet Advisor manager."""
        return Mock()

    @pytest.fixture
    def setup_tools(self, mock_mcp, mock_config, mock_fleet_advisor_manager):
        """Setup and register tools."""
        register_fleet_advisor_tools(mock_mcp, mock_config, mock_fleet_advisor_manager)
        return mock_mcp, mock_config, mock_fleet_advisor_manager

    def test_create_fleet_advisor_collector_exception(self, setup_tools):
        """Test exception handling in create_fleet_advisor_collector."""
        mcp, config, manager = setup_tools

        manager.create_collector.side_effect = ValueError('Invalid collector config')

        func = mcp.registered_tools['create_fleet_advisor_collector']
        result = func('test', 'desc', 'arn', 'bucket')

        assert result['success'] is False
        assert 'Invalid collector config' in result['error']['message']

    def test_delete_fleet_advisor_collector_exception(self, setup_tools):
        """Test exception handling in delete_fleet_advisor_collector."""
        mcp, config, manager = setup_tools

        manager.delete_collector.side_effect = RuntimeError('Delete failed')

        func = mcp.registered_tools['delete_fleet_advisor_collector']
        result = func('collector-ref-123')

        assert result['success'] is False
        assert 'Delete failed' in result['error']['message']

    def test_describe_fleet_advisor_databases_exception(self, setup_tools):
        """Test exception handling in describe_fleet_advisor_databases."""
        mcp, config, manager = setup_tools

        manager.list_databases.side_effect = ConnectionError('Connection lost')

        func = mcp.registered_tools['describe_fleet_advisor_databases']
        result = func()

        assert result['success'] is False
        assert 'Connection lost' in result['error']['message']

    def test_describe_fleet_advisor_lsa_analysis_exception(self, setup_tools):
        """Test exception handling in describe_fleet_advisor_lsa_analysis."""
        mcp, config, manager = setup_tools

        manager.describe_lsa_analysis.side_effect = TimeoutError('Analysis timeout')

        func = mcp.registered_tools['describe_fleet_advisor_lsa_analysis']
        result = func()

        assert result['success'] is False
        assert 'Analysis timeout' in result['error']['message']

    def test_run_fleet_advisor_lsa_analysis_exception(self, setup_tools):
        """Test exception handling in run_fleet_advisor_lsa_analysis."""
        mcp, config, manager = setup_tools

        manager.run_lsa_analysis.side_effect = MemoryError('Insufficient memory')

        func = mcp.registered_tools['run_fleet_advisor_lsa_analysis']
        result = func()

        assert result['success'] is False
        assert 'Insufficient memory' in result['error']['message']

    def test_describe_fleet_advisor_schema_object_summary_exception(self, setup_tools):
        """Test exception handling in describe_fleet_advisor_schema_object_summary."""
        mcp, config, manager = setup_tools

        manager.describe_schema_object_summary.side_effect = KeyError('Key not found')

        func = mcp.registered_tools['describe_fleet_advisor_schema_object_summary']
        result = func()

        assert result['success'] is False
        assert 'Key not found' in result['error']['message']

    def test_describe_fleet_advisor_schemas_exception(self, setup_tools):
        """Test exception handling in describe_fleet_advisor_schemas."""
        mcp, config, manager = setup_tools

        manager.list_schemas.side_effect = AttributeError('Attribute missing')

        func = mcp.registered_tools['describe_fleet_advisor_schemas']
        result = func()

        assert result['success'] is False
        assert 'Attribute missing' in result['error']['message']

    def test_delete_fleet_advisor_databases_exception(self, setup_tools):
        """Test exception handling in delete_fleet_advisor_databases."""
        mcp, config, manager = setup_tools

        manager.delete_databases.side_effect = PermissionError('Permission denied')

        func = mcp.registered_tools['delete_fleet_advisor_databases']
        result = func(['db-1', 'db-2'])

        assert result['success'] is False
        assert 'Permission denied' in result['error']['message']


class TestRecommendationToolsExceptionCoverage:
    """Test exception handling in recommendation tools."""

    @pytest.fixture
    def mock_mcp(self):
        """Create mock MCP server object."""
        mcp = Mock()
        mcp.registered_tools = {}

        def tool_decorator():
            def wrapper(func):
                mcp.registered_tools[func.__name__] = func
                return func

            return wrapper

        mcp.tool = tool_decorator
        return mcp

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = Mock(spec=DMSServerConfig)
        config.read_only_mode = False
        return config

    @pytest.fixture
    def mock_recommendation_manager(self):
        """Create mock recommendation manager."""
        return Mock()

    @pytest.fixture
    def setup_tools(self, mock_mcp, mock_config, mock_recommendation_manager):
        """Setup and register tools."""
        register_recommendation_tools(mock_mcp, mock_config, mock_recommendation_manager)
        return mock_mcp, mock_config, mock_recommendation_manager

    def test_describe_recommendation_limitations_exception(self, setup_tools):
        """Test exception handling in describe_recommendation_limitations."""
        mcp, config, manager = setup_tools

        manager.list_recommendation_limitations.side_effect = RuntimeError('API error')

        func = mcp.registered_tools['describe_recommendation_limitations']
        result = func()

        assert result['success'] is False
        assert 'API error' in result['error']['message']

    def test_start_recommendations_exception(self, setup_tools):
        """Test exception handling in start_recommendations."""
        mcp, config, manager = setup_tools

        manager.start_recommendations.side_effect = ValueError('Invalid database ID')

        func = mcp.registered_tools['start_recommendations']
        result = func('db-123', {})

        assert result['success'] is False
        assert 'Invalid database ID' in result['error']['message']

    def test_batch_start_recommendations_exception(self, setup_tools):
        """Test exception handling in batch_start_recommendations."""
        mcp, config, manager = setup_tools

        manager.batch_start_recommendations.side_effect = TypeError('Invalid data type')

        func = mcp.registered_tools['batch_start_recommendations']
        result = func([{'database_id': 'db-1'}])

        assert result['success'] is False
        assert 'Invalid data type' in result['error']['message']
