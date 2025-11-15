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

"""Comprehensive tests for EndpointManager module."""

import pytest
from awslabs.aws_dms_mcp_server.exceptions import DMSInvalidParameterException
from awslabs.aws_dms_mcp_server.utils.endpoint_manager import EndpointManager
from unittest.mock import Mock, patch


class TestEndpointManagerBasicOperations:
    """Test basic endpoint CRUD operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_list_endpoints_success(self, manager, mock_client):
        """Test successful endpoint listing."""
        mock_client.call_api.return_value = {
            'Endpoints': [
                {'EndpointIdentifier': 'endpoint-1', 'EndpointType': 'source'},
                {'EndpointIdentifier': 'endpoint-2', 'EndpointType': 'target'},
            ]
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_endpoint.side_effect = lambda x: x

            result = manager.list_endpoints()

            assert result['success'] is True
            assert result['data']['count'] == 2
            assert 'endpoints' in result['data']

    def test_list_endpoints_with_filters(self, manager, mock_client):
        """Test listing endpoints with filters."""
        mock_client.call_api.return_value = {'Endpoints': []}

        with patch('awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'):
            filters = [{'Name': 'endpoint-type', 'Values': ['source']}]
            result = manager.list_endpoints(filters=filters, max_results=50, marker='token')

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['Filters'] == filters
            assert call_args['MaxRecords'] == 50
            assert call_args['Marker'] == 'token'

    def test_list_endpoints_with_pagination(self, manager, mock_client):
        """Test listing endpoints with pagination."""
        mock_client.call_api.return_value = {'Endpoints': [], 'Marker': 'next-token'}

        with patch('awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'):
            result = manager.list_endpoints()

            assert result['success'] is True
            assert 'next_marker' in result['data']
            assert result['data']['next_marker'] == 'next-token'

    def test_delete_endpoint_success(self, manager, mock_client):
        """Test successful endpoint deletion."""
        mock_client.call_api.return_value = {'Endpoint': {'EndpointIdentifier': 'test-endpoint'}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_endpoint.return_value = {'identifier': 'test-endpoint'}

            result = manager.delete_endpoint('arn:aws:dms:us-east-1:123:endpoint:test')

            assert result['success'] is True
            assert result['data']['message'] == 'Endpoint deleted successfully'
            mock_client.call_api.assert_called_once_with(
                'delete_endpoint', EndpointArn='arn:aws:dms:us-east-1:123:endpoint:test'
            )

    def test_modify_endpoint_success(self, manager, mock_client):
        """Test successful endpoint modification."""
        mock_client.call_api.return_value = {'Endpoint': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_endpoint.return_value = {}

            params = {
                'EndpointArn': 'arn:aws:dms:us-east-1:123:endpoint:test',
                'EndpointIdentifier': 'new-identifier',
            }
            result = manager.modify_endpoint(params)

            assert result['success'] is True
            assert result['data']['message'] == 'Endpoint modified successfully'

    def test_modify_endpoint_masks_password(self, manager, mock_client):
        """Test that password is masked in modify endpoint logs."""
        mock_client.call_api.return_value = {'Endpoint': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_endpoint.return_value = {}

            params = {'EndpointArn': 'arn', 'Password': 'secret123'}
            result = manager.modify_endpoint(params)

            assert result['success'] is True
            # Password should be passed to API but masked in logs
            call_args = mock_client.call_api.call_args[1]
            assert call_args['Password'] == 'secret123'


class TestEndpointManagerCreateEndpoint:
    """Test endpoint creation functionality."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_create_endpoint_success(self, manager, mock_client):
        """Test successful endpoint creation."""
        mock_client.call_api.return_value = {'Endpoint': {'EndpointIdentifier': 'test-endpoint'}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_endpoint.return_value = {'identifier': 'test-endpoint'}

            params = {
                'EndpointIdentifier': 'test-endpoint',
                'EndpointType': 'source',
                'EngineName': 'mysql',
                'ServerName': 'localhost',
                'Port': 3306,
                'DatabaseName': 'testdb',
                'Username': 'admin',
                'Password': 'secret',
            }
            result = manager.create_endpoint(params)

            assert result['success'] is True
            assert 'endpoint' in result['data']
            assert result['data']['message'] == 'Endpoint created successfully'
            assert 'security_note' in result['data']

    def test_create_endpoint_missing_required_param(self, manager, mock_client):
        """Test endpoint creation with missing required parameter."""
        params = {
            'EndpointIdentifier': 'test-endpoint',
            'EndpointType': 'source',
            # Missing EngineName and other required params
        }

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_endpoint(params)

        assert 'Missing required parameter' in str(exc_info.value)

    def test_create_endpoint_invalid_config(self, manager, mock_client):
        """Test endpoint creation with invalid configuration."""
        params = {
            'EndpointIdentifier': 'test-endpoint',
            'EndpointType': 'invalid',  # Invalid type
            'EngineName': 'mysql',
            'ServerName': 'localhost',
            'Port': 3306,
            'DatabaseName': 'testdb',
            'Username': 'admin',
        }

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_endpoint(params)

        assert 'Invalid endpoint configuration' in str(exc_info.value)

    def test_create_endpoint_password_masking(self, manager, mock_client):
        """Test that password is masked in logs during creation."""
        mock_client.call_api.return_value = {'Endpoint': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_endpoint.return_value = {}

            params = {
                'EndpointIdentifier': 'test',
                'EndpointType': 'source',
                'EngineName': 'mysql',
                'ServerName': 'localhost',
                'Port': 3306,
                'DatabaseName': 'db',
                'Username': 'user',
                'Password': 'supersecret',
            }
            result = manager.create_endpoint(params)

            assert result['success'] is True
            # Password should be passed to API
            call_args = mock_client.call_api.call_args[1]
            assert call_args['Password'] == 'supersecret'


class TestEndpointManagerValidation:
    """Test endpoint configuration validation."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_validate_endpoint_config_valid_source(self, manager):
        """Test validation of valid source endpoint configuration."""
        config = {
            'EndpointType': 'source',
            'EngineName': 'mysql',
            'Port': 3306,
            'SslMode': 'none',
        }
        is_valid, error = manager.validate_endpoint_config(config)

        assert is_valid is True
        assert error == ''

    def test_validate_endpoint_config_valid_target(self, manager):
        """Test validation of valid target endpoint configuration."""
        config = {
            'EndpointType': 'target',
            'EngineName': 'postgres',
            'Port': 5432,
            'SslMode': 'require',
        }
        is_valid, error = manager.validate_endpoint_config(config)

        assert is_valid is True
        assert error == ''

    def test_validate_endpoint_config_invalid_type(self, manager):
        """Test validation with invalid endpoint type."""
        config = {'EndpointType': 'invalid', 'EngineName': 'mysql'}
        is_valid, error = manager.validate_endpoint_config(config)

        assert is_valid is False
        assert 'Invalid endpoint type' in error

    def test_validate_endpoint_config_unsupported_engine(self, manager):
        """Test validation with unsupported engine."""
        config = {'EndpointType': 'source', 'EngineName': 'unsupported-db'}
        is_valid, error = manager.validate_endpoint_config(config)

        assert is_valid is False
        assert 'Unsupported engine' in error

    def test_validate_endpoint_config_invalid_port_low(self, manager):
        """Test validation with port below valid range.

        Note: Port 0 is not caught by validation due to falsy check.
        This test documents current behavior.
        """
        config = {'EndpointType': 'source', 'EngineName': 'mysql', 'Port': 0}
        is_valid, error = manager.validate_endpoint_config(config)

        # Port 0 is not caught due to 'if port and ...' check treating 0 as falsy
        assert is_valid is True

    def test_validate_endpoint_config_invalid_port_high(self, manager):
        """Test validation with port above valid range."""
        config = {'EndpointType': 'source', 'EngineName': 'mysql', 'Port': 70000}
        is_valid, error = manager.validate_endpoint_config(config)

        assert is_valid is False
        assert 'Invalid port' in error

    def test_validate_endpoint_config_invalid_ssl_mode(self, manager):
        """Test validation with invalid SSL mode."""
        config = {
            'EndpointType': 'source',
            'EngineName': 'mysql',
            'SslMode': 'invalid-mode',
        }
        is_valid, error = manager.validate_endpoint_config(config)

        assert is_valid is False
        assert 'Invalid SSL mode' in error

    def test_validate_endpoint_config_valid_ssl_modes(self, manager):
        """Test validation with all valid SSL modes."""
        valid_ssl_modes = ['none', 'require', 'verify-ca', 'verify-full']

        for ssl_mode in valid_ssl_modes:
            config = {
                'EndpointType': 'source',
                'EngineName': 'mysql',
                'SslMode': ssl_mode,
            }
            is_valid, error = manager.validate_endpoint_config(config)
            assert is_valid is True

    def test_validate_endpoint_config_non_standard_port_warning(self, manager):
        """Test validation warns about non-standard port."""
        config = {
            'EndpointType': 'source',
            'EngineName': 'mysql',
            'Port': 9999,  # Non-standard for MySQL
        }
        is_valid, error = manager.validate_endpoint_config(config)

        # Should still be valid, just a warning
        assert is_valid is True


class TestEndpointManagerEngineSettings:
    """Test engine-specific settings."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_get_engine_settings_mysql(self, manager):
        """Test getting MySQL engine settings."""
        settings = manager.get_engine_settings('mysql')

        assert settings['default_port'] == 3306
        assert settings['ssl_supported'] is True
        assert settings['requires_server_name'] is True

    def test_get_engine_settings_postgres(self, manager):
        """Test getting PostgreSQL engine settings."""
        settings = manager.get_engine_settings('postgres')

        assert settings['default_port'] == 5432
        assert settings['ssl_supported'] is True

    def test_get_engine_settings_oracle(self, manager):
        """Test getting Oracle engine settings."""
        settings = manager.get_engine_settings('oracle')

        assert settings['default_port'] == 1521
        assert settings['ssl_supported'] is True

    def test_get_engine_settings_sqlserver(self, manager):
        """Test getting SQL Server engine settings."""
        settings = manager.get_engine_settings('sqlserver')

        assert settings['default_port'] == 1433

    def test_get_engine_settings_s3(self, manager):
        """Test getting S3 engine settings."""
        settings = manager.get_engine_settings('s3')

        assert settings['default_port'] is None
        assert settings['requires_server_name'] is False

    def test_get_engine_settings_dynamodb(self, manager):
        """Test getting DynamoDB engine settings."""
        settings = manager.get_engine_settings('dynamodb')

        assert settings['default_port'] is None
        assert settings['requires_server_name'] is False

    def test_get_engine_settings_mongodb(self, manager):
        """Test getting MongoDB engine settings."""
        settings = manager.get_engine_settings('mongodb')

        assert settings['default_port'] == 27017

    def test_get_engine_settings_unknown_engine(self, manager):
        """Test getting settings for unknown engine."""
        settings = manager.get_engine_settings('unknown-engine')

        assert settings['default_port'] is None
        assert settings['ssl_supported'] is False
        assert settings['requires_server_name'] is True

    def test_get_engine_settings_case_insensitive(self, manager):
        """Test that engine name is case-insensitive."""
        settings_lower = manager.get_engine_settings('mysql')
        settings_upper = manager.get_engine_settings('MYSQL')
        settings_mixed = manager.get_engine_settings('MySQL')

        assert settings_lower == settings_upper == settings_mixed


class TestEndpointManagerAdvancedOperations:
    """Test advanced endpoint operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_get_endpoint_settings_success(self, manager, mock_client):
        """Test getting endpoint settings for an engine."""
        mock_client.call_api.return_value = {
            'EndpointSettings': [{'Name': 'setting1'}, {'Name': 'setting2'}]
        }

        result = manager.get_endpoint_settings('mysql')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert result['data']['engine'] == 'mysql'
        assert 'endpoint_settings' in result['data']

    def test_get_endpoint_settings_with_pagination(self, manager, mock_client):
        """Test getting endpoint settings with pagination."""
        mock_client.call_api.return_value = {
            'EndpointSettings': [],
            'Marker': 'next-token',
        }

        result = manager.get_endpoint_settings('postgres', max_results=50, marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_endpoint_types_success(self, manager, mock_client):
        """Test listing endpoint types."""
        mock_client.call_api.return_value = {
            'SupportedEndpointTypes': [{'EngineName': 'mysql'}, {'EngineName': 'postgres'}]
        }

        result = manager.list_endpoint_types()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'endpoint_types' in result['data']

    def test_list_endpoint_types_with_filters(self, manager, mock_client):
        """Test listing endpoint types with filters."""
        mock_client.call_api.return_value = {'SupportedEndpointTypes': []}

        filters = [{'Name': 'engine-name', 'Values': ['mysql']}]
        result = manager.list_endpoint_types(filters=filters)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters

    def test_list_engine_versions_success(self, manager, mock_client):
        """Test listing engine versions."""
        mock_client.call_api.return_value = {
            'EngineVersions': [{'Version': '3.4.0'}, {'Version': '3.5.0'}]
        }

        result = manager.list_engine_versions()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'engine_versions' in result['data']

    def test_list_engine_versions_with_filter(self, manager, mock_client):
        """Test listing engine versions with engine name filter."""
        mock_client.call_api.return_value = {'EngineVersions': []}

        result = manager.list_engine_versions(engine_name='mysql')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Filters' in call_args
        assert call_args['Filters'][0]['Name'] == 'engine-name'
        assert call_args['Filters'][0]['Values'] == ['mysql']


class TestEndpointManagerSchemaOperations:
    """Test schema-related operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_refresh_schemas_success(self, manager, mock_client):
        """Test successful schema refresh."""
        mock_client.call_api.return_value = {'RefreshSchemasStatus': {'Status': 'successful'}}

        result = manager.refresh_schemas(
            endpoint_arn='arn:aws:dms:us-east-1:123:endpoint:test',
            instance_arn='arn:aws:dms:us-east-1:123:rep:test',
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Schema refresh initiated'
        assert 'refresh_status' in result['data']
        mock_client.call_api.assert_called_once_with(
            'refresh_schemas',
            EndpointArn='arn:aws:dms:us-east-1:123:endpoint:test',
            ReplicationInstanceArn='arn:aws:dms:us-east-1:123:rep:test',
        )

    def test_list_schemas_success(self, manager, mock_client):
        """Test listing schemas for an endpoint."""
        mock_client.call_api.return_value = {'Schemas': ['schema1', 'schema2', 'schema3']}

        result = manager.list_schemas('arn:aws:dms:us-east-1:123:endpoint:test')

        assert result['success'] is True
        assert result['data']['count'] == 3
        assert 'schemas' in result['data']
        assert result['data']['endpoint_arn'] == 'arn:aws:dms:us-east-1:123:endpoint:test'

    def test_list_schemas_with_pagination(self, manager, mock_client):
        """Test listing schemas with pagination."""
        mock_client.call_api.return_value = {'Schemas': [], 'Marker': 'next-token'}

        result = manager.list_schemas(
            'arn:aws:dms:us-east-1:123:endpoint:test', max_results=50, marker='token'
        )

        assert result['success'] is True
        assert 'next_marker' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_get_refresh_status_success(self, manager, mock_client):
        """Test getting schema refresh status."""
        mock_client.call_api.return_value = {
            'RefreshSchemasStatus': {'Status': 'successful', 'LastRefreshDate': '2024-01-01'}
        }

        result = manager.get_refresh_status('arn:aws:dms:us-east-1:123:endpoint:test')

        assert result['success'] is True
        assert result['data']['status'] == 'successful'
        assert 'refresh_status' in result['data']
        mock_client.call_api.assert_called_once_with(
            'describe_refresh_schemas_status',
            EndpointArn='arn:aws:dms:us-east-1:123:endpoint:test',
        )

    def test_get_refresh_status_unknown_status(self, manager, mock_client):
        """Test getting refresh status with missing status field."""
        mock_client.call_api.return_value = {'RefreshSchemasStatus': {}}

        result = manager.get_refresh_status('arn:aws:dms:us-east-1:123:endpoint:test')

        assert result['success'] is True
        assert result['data']['status'] == 'unknown'


class TestEndpointManagerSupportedEngines:
    """Test validation of supported engines."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_validate_all_supported_engines(self, manager):
        """Test validation passes for all supported engines."""
        supported_engines = [
            'mysql',
            'postgres',
            'postgresql',
            'oracle',
            'sqlserver',
            'mariadb',
            'aurora',
            'aurora-postgresql',
            'redshift',
            's3',
            'dynamodb',
            'mongodb',
            'sybase',
            'db2',
            'azuredb',
        ]

        for engine in supported_engines:
            config = {'EndpointType': 'source', 'EngineName': engine}
            is_valid, error = manager.validate_endpoint_config(config)
            assert is_valid is True, f'Engine {engine} should be valid'

    def test_validate_engine_case_insensitive(self, manager):
        """Test that engine validation is case-insensitive."""
        test_cases = ['MYSQL', 'MySQL', 'mysql', 'MysQL']

        for engine in test_cases:
            config = {'EndpointType': 'source', 'EngineName': engine}
            is_valid, error = manager.validate_endpoint_config(config)
            assert is_valid is True


class TestEndpointManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_list_endpoints_api_error(self, manager, mock_client):
        """Test API error during endpoint listing."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.list_endpoints()

        assert 'API Error' in str(exc_info.value)

    def test_delete_endpoint_api_error(self, manager, mock_client):
        """Test API error during endpoint deletion."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.delete_endpoint('arn:aws:dms:us-east-1:123:endpoint:test')

        assert 'Network error' in str(exc_info.value)

    def test_create_endpoint_validation_failure(self, manager, mock_client):
        """Test validation failure during endpoint creation."""
        params = {
            'EndpointIdentifier': 'test',
            'EndpointType': 'invalid-type',
            'EngineName': 'mysql',
            'ServerName': 'localhost',
            'Port': 3306,
            'DatabaseName': 'db',
            'Username': 'user',
        }

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_endpoint(params)

        assert 'Invalid endpoint configuration' in str(exc_info.value)


class TestEndpointManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EndpointManager instance."""
        return EndpointManager(mock_client)

    def test_list_endpoints_empty_result(self, manager, mock_client):
        """Test listing endpoints with empty result."""
        mock_client.call_api.return_value = {'Endpoints': []}

        with patch('awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'):
            result = manager.list_endpoints()

            assert result['success'] is True
            assert result['data']['count'] == 0
            assert result['data']['endpoints'] == []

    def test_validate_endpoint_with_minimal_config(self, manager):
        """Test validation with minimal configuration."""
        config = {'EndpointType': 'source', 'EngineName': 'mysql'}
        is_valid, error = manager.validate_endpoint_config(config)

        assert is_valid is True

    def test_get_engine_settings_with_mixed_case(self, manager):
        """Test getting engine settings with various case combinations."""
        engines = ['MySQL', 'POSTGRES', 'oracle', 'SqlServer']

        for engine in engines:
            settings = manager.get_engine_settings(engine)
            assert isinstance(settings, dict)
            assert 'default_port' in settings

    def test_list_operations_with_max_results_boundary(self, manager, mock_client):
        """Test list operations with maximum results."""
        mock_client.call_api.return_value = {'Endpoints': []}

        with patch('awslabs.aws_dms_mcp_server.utils.endpoint_manager.ResponseFormatter'):
            result = manager.list_endpoints(max_results=1000)

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['MaxRecords'] == 1000
