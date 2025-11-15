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

"""Comprehensive tests for tools_advanced module."""

import pytest
from awslabs.aws_dms_mcp_server.config import DMSServerConfig
from awslabs.aws_dms_mcp_server.exceptions.dms_exceptions import (
    DMSMCPException,
    DMSResourceNotFoundException,
)
from awslabs.aws_dms_mcp_server.tools_advanced import (
    register_fleet_advisor_tools,
    register_metadata_model_tools,
    register_recommendation_tools,
)
from unittest.mock import Mock


class TestMetadataModelTools:
    """Test metadata model operation tools."""

    @pytest.fixture
    def mock_mcp(self):
        """Create mock MCP server object."""
        mcp = Mock()
        # Store registered tools for testing
        mcp.registered_tools = {}

        def tool_decorator():
            def wrapper(func):
                # Store the function for later testing
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

    def test_describe_conversion_configuration_success(self, setup_tools):
        """Test describe_conversion_configuration successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'configuration': 'test_config'}
        manager.describe_conversion_configuration.return_value = expected_result

        func = mcp.registered_tools['describe_conversion_configuration']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result
        manager.describe_conversion_configuration.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST'
        )

    def test_describe_conversion_configuration_read_only_mode(self, setup_tools):
        """Test describe_conversion_configuration in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['describe_conversion_configuration']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'write access' in result['error']['message']

    def test_describe_conversion_configuration_exception(self, setup_tools):
        """Test describe_conversion_configuration with exception."""
        mcp, config, manager = setup_tools

        manager.describe_conversion_configuration.side_effect = Exception('Test error')

        func = mcp.registered_tools['describe_conversion_configuration']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Test error' in result['error']['message']

    def test_modify_conversion_configuration_success(self, setup_tools):
        """Test modify_conversion_configuration successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'modified'}
        manager.modify_conversion_configuration.return_value = expected_result
        conversion_config = {'setting': 'value'}

        func = mcp.registered_tools['modify_conversion_configuration']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', conversion_config)

        assert result == expected_result
        manager.modify_conversion_configuration.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST', configuration=conversion_config
        )

    def test_modify_conversion_configuration_read_only_mode(self, setup_tools):
        """Test modify_conversion_configuration in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['modify_conversion_configuration']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', {})

        assert result['success'] is False
        assert 'read-only mode' in result['error']['message']

    def test_describe_extension_pack_associations_success(self, setup_tools):
        """Test describe_extension_pack_associations successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'associations': []}
        manager.describe_extension_pack_associations.return_value = expected_result

        func = mcp.registered_tools['describe_extension_pack_associations']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result
        manager.describe_extension_pack_associations.assert_called_once()

    def test_describe_extension_pack_associations_with_filters(self, setup_tools):
        """Test describe_extension_pack_associations with filters."""
        mcp, config, manager = setup_tools

        expected_result = {'associations': []}
        manager.describe_extension_pack_associations.return_value = expected_result
        filters = [{'Name': 'test', 'Values': ['value']}]

        func = mcp.registered_tools['describe_extension_pack_associations']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', filters, 50, 'token')

        assert result == expected_result
        manager.describe_extension_pack_associations.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST',
            filters=filters,
            max_results=50,
            marker='token',
        )

    def test_start_extension_pack_association_success(self, setup_tools):
        """Test start_extension_pack_association successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.start_extension_pack_association.return_value = expected_result

        func = mcp.registered_tools['start_extension_pack_association']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result

    def test_start_extension_pack_association_read_only_mode(self, setup_tools):
        """Test start_extension_pack_association in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['start_extension_pack_association']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'read-only mode' in result['error']['message']

    def test_describe_metadata_model_assessments_success(self, setup_tools):
        """Test describe_metadata_model_assessments successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'assessments': []}
        manager.describe_metadata_model_assessments.return_value = expected_result

        func = mcp.registered_tools['describe_metadata_model_assessments']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result

    def test_start_metadata_model_assessment_success(self, setup_tools):
        """Test start_metadata_model_assessment successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.start_metadata_model_assessment.return_value = expected_result

        func = mcp.registered_tools['start_metadata_model_assessment']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{"rules": []}')

        assert result == expected_result
        manager.start_metadata_model_assessment.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST', selection_rules='{"rules": []}'
        )

    def test_start_metadata_model_assessment_read_only_mode(self, setup_tools):
        """Test start_metadata_model_assessment in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['start_metadata_model_assessment']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False

    def test_describe_metadata_model_conversions_success(self, setup_tools):
        """Test describe_metadata_model_conversions successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'conversions': []}
        manager.describe_metadata_model_conversions.return_value = expected_result

        func = mcp.registered_tools['describe_metadata_model_conversions']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result

    def test_start_metadata_model_conversion_success(self, setup_tools):
        """Test start_metadata_model_conversion successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.start_metadata_model_conversion.return_value = expected_result

        func = mcp.registered_tools['start_metadata_model_conversion']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{"rules": []}')

        assert result == expected_result

    def test_start_metadata_model_conversion_read_only_mode(self, setup_tools):
        """Test start_metadata_model_conversion in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['start_metadata_model_conversion']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False

    def test_describe_metadata_model_exports_as_script_success(self, setup_tools):
        """Test describe_metadata_model_exports_as_script successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'exports': []}
        manager.describe_metadata_model_exports_as_script.return_value = expected_result

        func = mcp.registered_tools['describe_metadata_model_exports_as_script']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result

    def test_start_metadata_model_export_as_script_success(self, setup_tools):
        """Test start_metadata_model_export_as_script successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.start_metadata_model_export_as_script.return_value = expected_result

        func = mcp.registered_tools['start_metadata_model_export_as_script']
        result = func(
            'arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', 'SOURCE', 'script.sql'
        )

        assert result == expected_result
        manager.start_metadata_model_export_as_script.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST',
            selection_rules='{}',
            origin='SOURCE',
            file_name='script.sql',
        )

    def test_start_metadata_model_export_as_script_read_only_mode(self, setup_tools):
        """Test start_metadata_model_export_as_script in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['start_metadata_model_export_as_script']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', 'SOURCE')

        assert result['success'] is False

    def test_describe_metadata_model_exports_to_target_success(self, setup_tools):
        """Test describe_metadata_model_exports_to_target successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'exports': []}
        manager.describe_metadata_model_exports_to_target.return_value = expected_result

        func = mcp.registered_tools['describe_metadata_model_exports_to_target']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result

    def test_start_metadata_model_export_to_target_success(self, setup_tools):
        """Test start_metadata_model_export_to_target successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.start_metadata_model_export_to_target.return_value = expected_result

        func = mcp.registered_tools['start_metadata_model_export_to_target']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', True)

        assert result == expected_result
        manager.start_metadata_model_export_to_target.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST',
            selection_rules='{}',
            overwrite_extension_pack=True,
        )

    def test_start_metadata_model_export_to_target_read_only_mode(self, setup_tools):
        """Test start_metadata_model_export_to_target in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['start_metadata_model_export_to_target']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False

    def test_describe_metadata_model_imports_success(self, setup_tools):
        """Test describe_metadata_model_imports successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'imports': []}
        manager.describe_metadata_model_imports.return_value = expected_result

        func = mcp.registered_tools['describe_metadata_model_imports']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result == expected_result

    def test_start_metadata_model_import_success(self, setup_tools):
        """Test start_metadata_model_import successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.start_metadata_model_import.return_value = expected_result

        func = mcp.registered_tools['start_metadata_model_import']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', 'SOURCE')

        assert result == expected_result
        manager.start_metadata_model_import.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST',
            selection_rules='{}',
            origin='SOURCE',
        )

    def test_start_metadata_model_import_read_only_mode(self, setup_tools):
        """Test start_metadata_model_import in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['start_metadata_model_import']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', 'SOURCE')

        assert result['success'] is False

    def test_export_metadata_model_assessment_success(self, setup_tools):
        """Test export_metadata_model_assessment successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'exported'}
        manager.export_metadata_model_assessment.return_value = expected_result

        func = mcp.registered_tools['export_metadata_model_assessment']
        result = func(
            'arn:aws:dms:us-east-1:123:migration-project:TEST', '{}', 'report.pdf', ['pdf']
        )

        assert result == expected_result
        manager.export_metadata_model_assessment.assert_called_once_with(
            arn='arn:aws:dms:us-east-1:123:migration-project:TEST',
            selection_rules='{}',
            file_name='report.pdf',
            assessment_report_types=['pdf'],
        )

    def test_export_metadata_model_assessment_read_only_mode(self, setup_tools):
        """Test export_metadata_model_assessment in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['export_metadata_model_assessment']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST', '{}')

        assert result['success'] is False

    def test_metadata_model_exception_handling(self, setup_tools):
        """Test exception handling across metadata model tools."""
        mcp, config, manager = setup_tools

        manager.describe_metadata_model_assessments.side_effect = DMSResourceNotFoundException(
            'Resource not found'
        )

        func = mcp.registered_tools['describe_metadata_model_assessments']
        result = func('arn:aws:dms:us-east-1:123:migration-project:TEST')

        assert result['success'] is False
        assert 'Resource not found' in result['error']['message']


class TestFleetAdvisorTools:
    """Test Fleet Advisor operation tools."""

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

    def test_create_fleet_advisor_collector_success(self, setup_tools):
        """Test create_fleet_advisor_collector successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'collector_id': '123'}
        manager.create_collector.return_value = expected_result

        func = mcp.registered_tools['create_fleet_advisor_collector']
        result = func(
            'test-collector', 'Test description', 'arn:aws:iam::123:role/test', 'test-bucket'
        )

        assert result == expected_result
        manager.create_collector.assert_called_once_with(
            name='test-collector',
            description='Test description',
            service_access_role_arn='arn:aws:iam::123:role/test',
            s3_bucket_name='test-bucket',
        )

    def test_create_fleet_advisor_collector_read_only_mode(self, setup_tools):
        """Test create_fleet_advisor_collector in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['create_fleet_advisor_collector']
        result = func('test', 'desc', 'arn', 'bucket')

        assert result['success'] is False
        assert 'read-only mode' in result['error']['message']

    def test_delete_fleet_advisor_collector_success(self, setup_tools):
        """Test delete_fleet_advisor_collector successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'deleted'}
        manager.delete_collector.return_value = expected_result

        func = mcp.registered_tools['delete_fleet_advisor_collector']
        result = func('collector-ref-123')

        assert result == expected_result
        manager.delete_collector.assert_called_once_with(ref='collector-ref-123')

    def test_delete_fleet_advisor_collector_read_only_mode(self, setup_tools):
        """Test delete_fleet_advisor_collector in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['delete_fleet_advisor_collector']
        result = func('collector-ref-123')

        assert result['success'] is False

    def test_describe_fleet_advisor_collectors_success(self, setup_tools):
        """Test describe_fleet_advisor_collectors successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'collectors': []}
        manager.list_collectors.return_value = expected_result

        func = mcp.registered_tools['describe_fleet_advisor_collectors']
        result = func()

        assert result == expected_result
        manager.list_collectors.assert_called_once()

    def test_describe_fleet_advisor_collectors_with_filters(self, setup_tools):
        """Test describe_fleet_advisor_collectors with filters."""
        mcp, config, manager = setup_tools

        expected_result = {'collectors': []}
        manager.list_collectors.return_value = expected_result
        filters = [{'Name': 'status', 'Values': ['active']}]

        func = mcp.registered_tools['describe_fleet_advisor_collectors']
        result = func(filters, 50, 'token')

        assert result == expected_result
        manager.list_collectors.assert_called_once_with(
            filters=filters, max_results=50, marker='token'
        )

    def test_delete_fleet_advisor_databases_success(self, setup_tools):
        """Test delete_fleet_advisor_databases successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'deleted_count': 2}
        manager.delete_databases.return_value = expected_result

        func = mcp.registered_tools['delete_fleet_advisor_databases']
        result = func(['db-1', 'db-2'])

        assert result == expected_result
        manager.delete_databases.assert_called_once_with(database_ids=['db-1', 'db-2'])

    def test_delete_fleet_advisor_databases_read_only_mode(self, setup_tools):
        """Test delete_fleet_advisor_databases in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['delete_fleet_advisor_databases']
        result = func(['db-1'])

        assert result['success'] is False

    def test_describe_fleet_advisor_databases_success(self, setup_tools):
        """Test describe_fleet_advisor_databases successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'databases': []}
        manager.list_databases.return_value = expected_result

        func = mcp.registered_tools['describe_fleet_advisor_databases']
        result = func()

        assert result == expected_result

    def test_describe_fleet_advisor_lsa_analysis_success(self, setup_tools):
        """Test describe_fleet_advisor_lsa_analysis successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'analysis': {}}
        manager.describe_lsa_analysis.return_value = expected_result

        func = mcp.registered_tools['describe_fleet_advisor_lsa_analysis']
        result = func(50, 'token')

        assert result == expected_result
        manager.describe_lsa_analysis.assert_called_once_with(max_results=50, marker='token')

    def test_run_fleet_advisor_lsa_analysis_success(self, setup_tools):
        """Test run_fleet_advisor_lsa_analysis successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.run_lsa_analysis.return_value = expected_result

        func = mcp.registered_tools['run_fleet_advisor_lsa_analysis']
        result = func()

        assert result == expected_result
        manager.run_lsa_analysis.assert_called_once()

    def test_run_fleet_advisor_lsa_analysis_read_only_mode(self, setup_tools):
        """Test run_fleet_advisor_lsa_analysis in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['run_fleet_advisor_lsa_analysis']
        result = func()

        assert result['success'] is False

    def test_describe_fleet_advisor_schema_object_summary_success(self, setup_tools):
        """Test describe_fleet_advisor_schema_object_summary successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'summary': {}}
        manager.describe_schema_object_summary.return_value = expected_result

        func = mcp.registered_tools['describe_fleet_advisor_schema_object_summary']
        result = func()

        assert result == expected_result

    def test_describe_fleet_advisor_schemas_success(self, setup_tools):
        """Test describe_fleet_advisor_schemas successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'schemas': []}
        manager.list_schemas.return_value = expected_result

        func = mcp.registered_tools['describe_fleet_advisor_schemas']
        result = func()

        assert result == expected_result

    def test_fleet_advisor_exception_handling(self, setup_tools):
        """Test exception handling across Fleet Advisor tools."""
        mcp, config, manager = setup_tools

        manager.list_collectors.side_effect = Exception('Network error')

        func = mcp.registered_tools['describe_fleet_advisor_collectors']
        result = func()

        assert result['success'] is False
        assert 'Network error' in result['error']['message']


class TestRecommendationTools:
    """Test recommendation operation tools."""

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

    def test_describe_recommendations_success(self, setup_tools):
        """Test describe_recommendations successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'recommendations': []}
        manager.list_recommendations.return_value = expected_result

        func = mcp.registered_tools['describe_recommendations']
        result = func()

        assert result == expected_result
        manager.list_recommendations.assert_called_once()

    def test_describe_recommendations_with_filters(self, setup_tools):
        """Test describe_recommendations with filters."""
        mcp, config, manager = setup_tools

        expected_result = {'recommendations': []}
        manager.list_recommendations.return_value = expected_result
        filters = [{'Name': 'type', 'Values': ['cost']}]

        func = mcp.registered_tools['describe_recommendations']
        result = func(filters, 50, 'token')

        assert result == expected_result
        manager.list_recommendations.assert_called_once_with(
            filters=filters, max_results=50, marker='token'
        )

    def test_describe_recommendation_limitations_success(self, setup_tools):
        """Test describe_recommendation_limitations successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'limitations': []}
        manager.list_recommendation_limitations.return_value = expected_result

        func = mcp.registered_tools['describe_recommendation_limitations']
        result = func()

        assert result == expected_result

    def test_start_recommendations_success(self, setup_tools):
        """Test start_recommendations successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started'}
        manager.start_recommendations.return_value = expected_result
        settings = {'threshold': 0.8}

        func = mcp.registered_tools['start_recommendations']
        result = func('db-123', settings)

        assert result == expected_result
        manager.start_recommendations.assert_called_once_with(
            database_id='db-123', settings=settings
        )

    def test_start_recommendations_read_only_mode(self, setup_tools):
        """Test start_recommendations in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['start_recommendations']
        result = func('db-123', {})

        assert result['success'] is False
        assert 'read-only mode' in result['error']['message']

    def test_batch_start_recommendations_success(self, setup_tools):
        """Test batch_start_recommendations successful call."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started', 'count': 2}
        manager.batch_start_recommendations.return_value = expected_result
        data = [{'database_id': 'db-1'}, {'database_id': 'db-2'}]

        func = mcp.registered_tools['batch_start_recommendations']
        result = func(data)

        assert result == expected_result
        manager.batch_start_recommendations.assert_called_once_with(data=data)

    def test_batch_start_recommendations_read_only_mode(self, setup_tools):
        """Test batch_start_recommendations in read-only mode."""
        mcp, config, manager = setup_tools
        config.read_only_mode = True

        func = mcp.registered_tools['batch_start_recommendations']
        result = func([])

        assert result['success'] is False

    def test_batch_start_recommendations_with_none_data(self, setup_tools):
        """Test batch_start_recommendations with None data."""
        mcp, config, manager = setup_tools

        expected_result = {'status': 'started', 'count': 0}
        manager.batch_start_recommendations.return_value = expected_result

        func = mcp.registered_tools['batch_start_recommendations']
        result = func(None)

        assert result == expected_result
        manager.batch_start_recommendations.assert_called_once_with(data=None)

    def test_recommendation_exception_handling(self, setup_tools):
        """Test exception handling across recommendation tools."""
        mcp, config, manager = setup_tools

        manager.list_recommendations.side_effect = DMSMCPException(
            'API Error', details={'code': 'InvalidRequest'}
        )

        func = mcp.registered_tools['describe_recommendations']
        result = func()

        assert result['success'] is False
        assert 'API Error' in result['error']['message']


class TestToolRegistration:
    """Test tool registration functions."""

    def test_all_metadata_model_tools_registered(self):
        """Verify all metadata model tools are registered."""
        mcp = Mock()
        registered_count = 0

        def tool_decorator():
            def wrapper(func):
                nonlocal registered_count
                registered_count += 1
                return func

            return wrapper

        mcp.tool = tool_decorator
        config = Mock()
        config.read_only_mode = False
        manager = Mock()

        register_metadata_model_tools(mcp, config, manager)

        # Should register 15 metadata model tools
        assert registered_count == 15

    def test_all_fleet_advisor_tools_registered(self):
        """Verify all Fleet Advisor tools are registered."""
        mcp = Mock()
        registered_count = 0

        def tool_decorator():
            def wrapper(func):
                nonlocal registered_count
                registered_count += 1
                return func

            return wrapper

        mcp.tool = tool_decorator
        config = Mock()
        config.read_only_mode = False
        manager = Mock()

        register_fleet_advisor_tools(mcp, config, manager)

        # Should register 9 Fleet Advisor tools
        assert registered_count == 9

    def test_all_recommendation_tools_registered(self):
        """Verify all recommendation tools are registered."""
        mcp = Mock()
        registered_count = 0

        def tool_decorator():
            def wrapper(func):
                nonlocal registered_count
                registered_count += 1
                return func

            return wrapper

        mcp.tool = tool_decorator
        config = Mock()
        config.read_only_mode = False
        manager = Mock()

        register_recommendation_tools(mcp, config, manager)

        # Should register 4 recommendation tools
        assert registered_count == 4
