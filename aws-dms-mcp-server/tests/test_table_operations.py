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

"""Comprehensive tests for TableOperations module."""

import pytest
from awslabs.aws_dms_mcp_server.exceptions import DMSInvalidParameterException
from awslabs.aws_dms_mcp_server.utils.table_operations import TableOperations
from unittest.mock import Mock, patch


class TestTableOperationsGetStatistics:
    """Test table statistics retrieval."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TableOperations instance."""
        return TableOperations(mock_client)

    def test_get_table_statistics_success(self, manager, mock_client):
        """Test successful table statistics retrieval."""
        mock_client.call_api.return_value = {
            'TableStatistics': [
                {
                    'SchemaName': 'public',
                    'TableName': 'users',
                    'Inserts': 100,
                    'Deletes': 10,
                    'Updates': 50,
                },
                {
                    'SchemaName': 'public',
                    'TableName': 'orders',
                    'Inserts': 200,
                    'Deletes': 20,
                    'Updates': 100,
                },
            ]
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_table_stats.side_effect = lambda x: {
                'schema_name': x.get('SchemaName'),
                'table_name': x.get('TableName'),
                'inserts': x.get('Inserts', 0),
                'deletes': x.get('Deletes', 0),
                'updates': x.get('Updates', 0),
                'ddls': x.get('Ddls', 0),
                'full_load_rows': x.get('FullLoadRows', 0),
                'full_load_error_rows': x.get('FullLoadErrorRows', 0),
            }

            result = manager.get_table_statistics('arn:test:task')

            assert result['success'] is True
            assert result['data']['count'] == 2
            assert 'summary' in result['data']
            assert result['data']['summary']['total_tables'] == 2
            assert result['data']['summary']['total_inserts'] == 300
            assert result['data']['summary']['total_deletes'] == 30
            assert result['data']['summary']['total_updates'] == 150

    def test_get_table_statistics_with_filters(self, manager, mock_client):
        """Test table statistics retrieval with filters."""
        mock_client.call_api.return_value = {'TableStatistics': []}

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            filters = [{'Name': 'schema-name', 'Values': ['public']}]
            result = manager.get_table_statistics(
                'arn:test:task', filters=filters, max_results=50, marker='token'
            )

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['Filters'] == filters
            assert call_args['MaxRecords'] == 50
            assert call_args['Marker'] == 'token'

    def test_get_table_statistics_with_pagination(self, manager, mock_client):
        """Test table statistics with pagination."""
        mock_client.call_api.return_value = {
            'TableStatistics': [],
            'Marker': 'next-token',
        }

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            result = manager.get_table_statistics('arn:test:task')

            assert result['success'] is True
            assert 'next_marker' in result['data']
            assert result['data']['next_marker'] == 'next-token'

    def test_get_table_statistics_summary_calculation(self, manager, mock_client):
        """Test summary statistics calculation."""
        mock_client.call_api.return_value = {
            'TableStatistics': [
                {
                    'Inserts': 100,
                    'Deletes': 10,
                    'Updates': 50,
                    'Ddls': 5,
                    'FullLoadRows': 1000,
                    'FullLoadErrorRows': 2,
                    'FullLoadCondtnlChkFailedRows': 0,
                },
                {
                    'Inserts': 200,
                    'Deletes': 20,
                    'Updates': 100,
                    'Ddls': 10,
                    'FullLoadRows': 2000,
                    'FullLoadErrorRows': 3,
                    'FullLoadCondtnlChkFailedRows': 0,
                },
            ]
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_table_stats.side_effect = lambda x: {
                'inserts': x.get('Inserts', 0),
                'deletes': x.get('Deletes', 0),
                'updates': x.get('Updates', 0),
                'ddls': x.get('Ddls', 0),
                'full_load_rows': x.get('FullLoadRows', 0),
                'full_load_error_rows': x.get('FullLoadErrorRows', 0),
            }

            result = manager.get_table_statistics('arn:test:task')

            assert result['success'] is True
            summary = result['data']['summary']
            assert summary['total_inserts'] == 300
            assert summary['total_deletes'] == 30
            assert summary['total_updates'] == 150
            assert summary['total_ddls'] == 15
            assert summary['total_full_load_rows'] == 3000
            assert summary['total_error_rows'] == 5

    def test_get_table_statistics_empty_result(self, manager, mock_client):
        """Test table statistics with empty result."""
        mock_client.call_api.return_value = {'TableStatistics': []}

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            result = manager.get_table_statistics('arn:test:task')

            assert result['success'] is True
            assert result['data']['count'] == 0
            assert result['data']['summary']['total_tables'] == 0


class TestTableOperationsReloadTables:
    """Test table reload operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TableOperations instance."""
        return TableOperations(mock_client)

    def test_reload_tables_success(self, manager, mock_client):
        """Test successful table reload."""
        mock_client.call_api.return_value = {}

        tables = [
            {'SchemaName': 'public', 'TableName': 'users'},
            {'SchemaName': 'public', 'TableName': 'orders'},
        ]
        result = manager.reload_tables('arn:test:task', tables)

        assert result['success'] is True
        assert result['data']['tables_reloaded'] == 2
        assert result['data']['reload_option'] == 'data-reload'
        assert 'Table reload initiated' in result['data']['message']

    def test_reload_tables_with_validate_only(self, manager, mock_client):
        """Test table reload with validate-only option."""
        mock_client.call_api.return_value = {}

        tables = [{'SchemaName': 'public', 'TableName': 'users'}]
        result = manager.reload_tables('arn:test:task', tables, reload_option='validate-only')

        assert result['success'] is True
        assert result['data']['reload_option'] == 'validate-only'
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReloadOption'] == 'validate-only'

    def test_reload_tables_empty_list_error(self, manager, mock_client):
        """Test table reload with empty tables list."""
        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_tables('arn:test:task', [])

        assert 'Tables list cannot be empty' in str(exc_info.value)

    def test_reload_tables_missing_schema_name(self, manager, mock_client):
        """Test table reload with missing SchemaName."""
        tables = [{'TableName': 'users'}]

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_tables('arn:test:task', tables)

        assert "missing 'SchemaName'" in str(exc_info.value)

    def test_reload_tables_missing_table_name(self, manager, mock_client):
        """Test table reload with missing TableName."""
        tables = [{'SchemaName': 'public'}]

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_tables('arn:test:task', tables)

        assert "missing 'TableName'" in str(exc_info.value)

    def test_reload_tables_invalid_reload_option(self, manager, mock_client):
        """Test table reload with invalid reload option."""
        tables = [{'SchemaName': 'public', 'TableName': 'users'}]

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_tables('arn:test:task', tables, reload_option='invalid-option')

        assert 'Invalid reload option' in str(exc_info.value)

    def test_reload_tables_multiple_tables(self, manager, mock_client):
        """Test reloading multiple tables."""
        mock_client.call_api.return_value = {}

        tables = [
            {'SchemaName': 'schema1', 'TableName': 'table1'},
            {'SchemaName': 'schema1', 'TableName': 'table2'},
            {'SchemaName': 'schema2', 'TableName': 'table3'},
        ]
        result = manager.reload_tables('arn:test:task', tables)

        assert result['success'] is True
        assert result['data']['tables_reloaded'] == 3


class TestTableOperationsFormatStatistics:
    """Test statistics formatting."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TableOperations instance."""
        return TableOperations(mock_client)

    def test_format_statistics_with_state_descriptions(self, manager):
        """Test statistics formatting with state descriptions."""
        stats = [
            {'TableName': 'table1', 'TableState': 'Table completed'},
            {'TableName': 'table2', 'TableState': 'Table loading'},
            {'TableName': 'table3', 'TableState': 'Table error'},
        ]

        with patch(
            'awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_table_stats.side_effect = lambda x: {
                'table_name': x.get('TableName'),
                'table_state': x.get('TableState'),
            }

            result = manager.format_statistics(stats)

            assert len(result) == 3
            assert 'state_description' in result[0]
            assert result[0]['state_description'] == 'Full load and ongoing replication complete'
            assert result[1]['state_description'] == 'Full load in progress'
            assert result[2]['state_description'] == 'Error occurred during replication'

    def test_format_statistics_unknown_state(self, manager):
        """Test statistics formatting with unknown state."""
        stats = [{'TableName': 'table1', 'TableState': 'Unknown state'}]

        with patch(
            'awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_table_stats.side_effect = lambda x: {
                'table_name': x.get('TableName'),
                'table_state': x.get('TableState'),
            }

            result = manager.format_statistics(stats)

            assert len(result) == 1
            assert 'state_description' not in result[0]


class TestTableOperationsGetReplicationTableStatistics:
    """Test replication table statistics retrieval."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TableOperations instance."""
        return TableOperations(mock_client)

    def test_get_replication_table_statistics_with_task_arn(self, manager, mock_client):
        """Test replication table statistics with task ARN."""
        mock_client.call_api.return_value = {
            'ReplicationTableStatistics': [
                {'SchemaName': 'public', 'TableName': 'users'},
            ]
        }

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            result = manager.get_replication_table_statistics(task_arn='arn:test:task')

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['ReplicationTaskArn'] == 'arn:test:task'
            assert 'ReplicationConfigArn' not in call_args

    def test_get_replication_table_statistics_with_config_arn(self, manager, mock_client):
        """Test replication table statistics with config ARN."""
        mock_client.call_api.return_value = {'ReplicationTableStatistics': []}

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            result = manager.get_replication_table_statistics(config_arn='arn:test:config')

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['ReplicationConfigArn'] == 'arn:test:config'
            assert 'ReplicationTaskArn' not in call_args

    def test_get_replication_table_statistics_missing_arn_error(self, manager, mock_client):
        """Test replication table statistics with missing ARN."""
        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.get_replication_table_statistics()

        assert 'Must provide either task_arn or config_arn' in str(exc_info.value)

    def test_get_replication_table_statistics_with_filters(self, manager, mock_client):
        """Test replication table statistics with filters."""
        mock_client.call_api.return_value = {'ReplicationTableStatistics': []}

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            filters = [{'Name': 'schema-name', 'Values': ['public']}]
            result = manager.get_replication_table_statistics(
                task_arn='arn:task', filters=filters, max_results=50, marker='token'
            )

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['Filters'] == filters

    def test_get_replication_table_statistics_with_pagination(self, manager, mock_client):
        """Test replication table statistics with pagination."""
        mock_client.call_api.return_value = {
            'ReplicationTableStatistics': [],
            'Marker': 'next-token',
        }

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            result = manager.get_replication_table_statistics(task_arn='arn:task')

            assert result['success'] is True
            assert 'next_marker' in result['data']
            assert result['data']['next_marker'] == 'next-token'


class TestTableOperationsReloadServerlessTables:
    """Test serverless table reload operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TableOperations instance."""
        return TableOperations(mock_client)

    def test_reload_serverless_tables_success(self, manager, mock_client):
        """Test successful serverless table reload."""
        mock_client.call_api.return_value = {}

        tables = [
            {'SchemaName': 'public', 'TableName': 'users'},
            {'SchemaName': 'public', 'TableName': 'orders'},
        ]
        result = manager.reload_serverless_tables('arn:test:config', tables)

        assert result['success'] is True
        assert result['data']['tables_reloaded'] == 2
        assert result['data']['config_arn'] == 'arn:test:config'
        assert result['data']['reload_option'] == 'data-reload'

    def test_reload_serverless_tables_with_validate_only(self, manager, mock_client):
        """Test serverless table reload with validate-only option."""
        mock_client.call_api.return_value = {}

        tables = [{'SchemaName': 'public', 'TableName': 'users'}]
        result = manager.reload_serverless_tables(
            'arn:test:config', tables, reload_option='validate-only'
        )

        assert result['success'] is True
        assert result['data']['reload_option'] == 'validate-only'
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReloadOption'] == 'validate-only'

    def test_reload_serverless_tables_empty_list_error(self, manager, mock_client):
        """Test serverless table reload with empty tables list."""
        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_serverless_tables('arn:test:config', [])

        assert 'Tables list cannot be empty' in str(exc_info.value)

    def test_reload_serverless_tables_missing_schema_name(self, manager, mock_client):
        """Test serverless table reload with missing SchemaName."""
        tables = [{'TableName': 'users'}]

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_serverless_tables('arn:test:config', tables)

        assert "missing 'SchemaName'" in str(exc_info.value)

    def test_reload_serverless_tables_missing_table_name(self, manager, mock_client):
        """Test serverless table reload with missing TableName."""
        tables = [{'SchemaName': 'public'}]

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_serverless_tables('arn:test:config', tables)

        assert "missing 'TableName'" in str(exc_info.value)

    def test_reload_serverless_tables_invalid_reload_option(self, manager, mock_client):
        """Test serverless table reload with invalid reload option."""
        tables = [{'SchemaName': 'public', 'TableName': 'users'}]

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.reload_serverless_tables(
                'arn:test:config', tables, reload_option='invalid-option'
            )

        assert 'Invalid reload option' in str(exc_info.value)


class TestTableOperationsErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TableOperations instance."""
        return TableOperations(mock_client)

    def test_get_table_statistics_api_error(self, manager, mock_client):
        """Test API error during table statistics retrieval."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.get_table_statistics('arn:test:task')

        assert 'API Error' in str(exc_info.value)

    def test_reload_tables_api_error(self, manager, mock_client):
        """Test API error during table reload."""
        tables = [{'SchemaName': 'public', 'TableName': 'users'}]
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.reload_tables('arn:test:task', tables)

        assert 'Network error' in str(exc_info.value)

    def test_get_replication_table_statistics_api_error(self, manager, mock_client):
        """Test API error during replication table statistics retrieval."""
        mock_client.call_api.side_effect = Exception('Service error')

        with pytest.raises(Exception) as exc_info:
            manager.get_replication_table_statistics(task_arn='arn:task')

        assert 'Service error' in str(exc_info.value)


class TestTableOperationsEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TableOperations instance."""
        return TableOperations(mock_client)

    def test_get_table_statistics_max_results_boundary(self, manager, mock_client):
        """Test table statistics with maximum results."""
        mock_client.call_api.return_value = {'TableStatistics': []}

        with patch('awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'):
            result = manager.get_table_statistics('arn:test:task', max_results=1000)

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['MaxRecords'] == 1000

    def test_reload_tables_many_tables(self, manager, mock_client):
        """Test reloading many tables at once."""
        mock_client.call_api.return_value = {}

        tables = [{'SchemaName': f'schema{i}', 'TableName': f'table{i}'} for i in range(50)]
        result = manager.reload_tables('arn:test:task', tables)

        assert result['success'] is True
        assert result['data']['tables_reloaded'] == 50

    def test_format_statistics_completion_percent_calculation(self, manager, mock_client):
        """Test completion percentage calculation in statistics."""
        mock_client.call_api.return_value = {
            'TableStatistics': [
                {'FullLoadCondtnlChkFailedRows': 0},
                {'FullLoadCondtnlChkFailedRows': 0},
            ]
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.table_operations.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_table_stats.side_effect = lambda x: {
                'completion_percent': 75.5 if x else None
            }

            result = manager.get_table_statistics('arn:test:task')

            assert result['success'] is True
            assert 'average_completion_percent' in result['data']['summary']

    def test_reload_tables_valid_reload_options(self, manager, mock_client):
        """Test all valid reload options."""
        mock_client.call_api.return_value = {}
        tables = [{'SchemaName': 'public', 'TableName': 'users'}]

        valid_options = ['data-reload', 'validate-only']
        for option in valid_options:
            result = manager.reload_tables('arn:test:task', tables, reload_option=option)
            assert result['success'] is True
            mock_client.call_api.reset_mock()
