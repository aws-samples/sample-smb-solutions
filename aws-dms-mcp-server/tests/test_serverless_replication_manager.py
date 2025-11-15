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

"""Comprehensive tests for ServerlessReplicationManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.serverless_replication_manager import (
    ServerlessReplicationManager,
)
from unittest.mock import Mock


class TestServerlessReplicationManagerCreateConfig:
    """Test replication configuration creation."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_create_replication_config_success(self, manager, mock_client):
        """Test successful replication config creation."""
        mock_client.call_api.return_value = {
            'ReplicationConfig': {'ReplicationConfigIdentifier': 'test-config'}
        }

        compute_config = {'ReplicationSubnetGroupId': 'subnet-group', 'MaxCapacityUnits': 16}
        result = manager.create_replication_config(
            'test-config',
            'arn:source',
            'arn:target',
            compute_config,
            'full-load',
            '{"rules": []}',
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Replication config created successfully'
        assert 'replication_config' in result['data']

    def test_create_replication_config_with_all_params(self, manager, mock_client):
        """Test replication config creation with all parameters."""
        mock_client.call_api.return_value = {'ReplicationConfig': {}}

        compute_config = {'MaxCapacityUnits': 32}
        tags = [{'Key': 'Environment', 'Value': 'Test'}]
        result = manager.create_replication_config(
            'test-config',
            'arn:source',
            'arn:target',
            compute_config,
            'full-load-and-cdc',
            '{"rules": []}',
            replication_settings='{"settings": {}}',
            supplemental_settings='{"supplemental": {}}',
            resource_identifier='resource-1',
            tags=tags,
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationSettings'] == '{"settings": {}}'
        assert call_args['SupplementalSettings'] == '{"supplemental": {}}'
        assert call_args['ResourceIdentifier'] == 'resource-1'
        assert call_args['Tags'] == tags

    def test_create_replication_config_minimal_params(self, manager, mock_client):
        """Test replication config creation with minimal parameters."""
        mock_client.call_api.return_value = {'ReplicationConfig': {}}

        compute_config = {'MaxCapacityUnits': 16}
        result = manager.create_replication_config(
            'config', 'arn:src', 'arn:tgt', compute_config, 'cdc', '{}'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationConfigIdentifier'] == 'config'
        assert 'ReplicationSettings' not in call_args
        assert 'SupplementalSettings' not in call_args

    def test_create_replication_config_replication_types(self, manager, mock_client):
        """Test replication config creation with different replication types."""
        mock_client.call_api.return_value = {'ReplicationConfig': {}}

        compute_config = {'MaxCapacityUnits': 16}
        replication_types = ['full-load', 'cdc', 'full-load-and-cdc']

        for repl_type in replication_types:
            result = manager.create_replication_config(
                'config', 'arn:src', 'arn:tgt', compute_config, repl_type, '{}'
            )
            assert result['success'] is True
            mock_client.call_api.reset_mock()


class TestServerlessReplicationManagerModifyConfig:
    """Test replication configuration modification."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_modify_replication_config_success(self, manager, mock_client):
        """Test successful replication config modification."""
        mock_client.call_api.return_value = {
            'ReplicationConfig': {'ReplicationConfigArn': 'arn:config'}
        }

        result = manager.modify_replication_config('arn:config')

        assert result['success'] is True
        assert result['data']['message'] == 'Replication config modified successfully'

    def test_modify_replication_config_with_all_params(self, manager, mock_client):
        """Test replication config modification with all parameters."""
        mock_client.call_api.return_value = {'ReplicationConfig': {}}

        compute_config = {'MaxCapacityUnits': 32}
        result = manager.modify_replication_config(
            'arn:config',
            identifier='new-identifier',
            compute_config=compute_config,
            replication_type='full-load-and-cdc',
            table_mappings='{"new": "mappings"}',
            replication_settings='{"new": "settings"}',
            supplemental_settings='{"new": "supplemental"}',
            source_endpoint_arn='arn:new-source',
            target_endpoint_arn='arn:new-target',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationConfigIdentifier'] == 'new-identifier'
        assert call_args['ComputeConfig'] == compute_config
        assert call_args['ReplicationType'] == 'full-load-and-cdc'
        assert call_args['SourceEndpointArn'] == 'arn:new-source'
        assert call_args['TargetEndpointArn'] == 'arn:new-target'

    def test_modify_replication_config_partial_update(self, manager, mock_client):
        """Test replication config modification with partial parameters."""
        mock_client.call_api.return_value = {'ReplicationConfig': {}}

        compute_config = {'MaxCapacityUnits': 64}
        result = manager.modify_replication_config('arn:config', compute_config=compute_config)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ComputeConfig'] == compute_config
        assert 'ReplicationConfigIdentifier' not in call_args


class TestServerlessReplicationManagerDeleteConfig:
    """Test replication configuration deletion."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_delete_replication_config_success(self, manager, mock_client):
        """Test successful replication config deletion."""
        mock_client.call_api.return_value = {
            'ReplicationConfig': {'ReplicationConfigArn': 'arn:config'}
        }

        result = manager.delete_replication_config('arn:config')

        assert result['success'] is True
        assert result['data']['message'] == 'Replication config deleted successfully'
        mock_client.call_api.assert_called_once_with(
            'delete_replication_config', ReplicationConfigArn='arn:config'
        )


class TestServerlessReplicationManagerListConfigs:
    """Test replication configuration listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_list_replication_configs_success(self, manager, mock_client):
        """Test successful replication configs listing."""
        mock_client.call_api.return_value = {
            'ReplicationConfigs': [
                {'ReplicationConfigArn': 'arn:config-1'},
                {'ReplicationConfigArn': 'arn:config-2'},
            ]
        }

        result = manager.list_replication_configs()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['replication_configs']) == 2

    def test_list_replication_configs_with_filters(self, manager, mock_client):
        """Test replication configs listing with filters."""
        mock_client.call_api.return_value = {'ReplicationConfigs': []}

        filters = [{'Name': 'replication-type', 'Values': ['full-load']}]
        result = manager.list_replication_configs(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_replication_configs_with_pagination(self, manager, mock_client):
        """Test replication configs listing with pagination."""
        mock_client.call_api.return_value = {
            'ReplicationConfigs': [],
            'Marker': 'next-token',
        }

        result = manager.list_replication_configs()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_list_replication_configs_empty_result(self, manager, mock_client):
        """Test replication configs listing with empty result."""
        mock_client.call_api.return_value = {'ReplicationConfigs': []}

        result = manager.list_replication_configs()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['replication_configs'] == []


class TestServerlessReplicationManagerListReplications:
    """Test replications listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_list_replications_success(self, manager, mock_client):
        """Test successful replications listing."""
        mock_client.call_api.return_value = {
            'Replications': [
                {'ReplicationArn': 'arn:repl-1'},
                {'ReplicationArn': 'arn:repl-2'},
            ]
        }

        result = manager.list_replications()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['replications']) == 2

    def test_list_replications_with_filters(self, manager, mock_client):
        """Test replications listing with filters."""
        mock_client.call_api.return_value = {'Replications': []}

        filters = [{'Name': 'status', 'Values': ['running']}]
        result = manager.list_replications(filters=filters, max_results=25)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 25

    def test_list_replications_with_pagination(self, manager, mock_client):
        """Test replications listing with pagination."""
        mock_client.call_api.return_value = {'Replications': [], 'Marker': 'next-token'}

        result = manager.list_replications(marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'


class TestServerlessReplicationManagerStartReplication:
    """Test replication start operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_start_replication_success(self, manager, mock_client):
        """Test successful replication start."""
        mock_client.call_api.return_value = {'Replication': {'ReplicationArn': 'arn:repl'}}

        result = manager.start_replication('arn:config', 'start-replication')

        assert result['success'] is True
        assert 'Replication started with type: start-replication' in result['data']['message']
        assert 'replication' in result['data']

    def test_start_replication_with_cdc_time(self, manager, mock_client):
        """Test replication start with CDC start time."""
        mock_client.call_api.return_value = {'Replication': {}}

        result = manager.start_replication(
            'arn:config', 'resume-processing', cdc_start_time='2024-01-01T00:00:00Z'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CdcStartTime'] == '2024-01-01T00:00:00Z'

    def test_start_replication_with_cdc_position(self, manager, mock_client):
        """Test replication start with CDC position."""
        mock_client.call_api.return_value = {'Replication': {}}

        result = manager.start_replication(
            'arn:config',
            'resume-processing',
            cdc_start_position='mysql-bin.000001:1234',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CdcStartPosition'] == 'mysql-bin.000001:1234'

    def test_start_replication_with_cdc_stop_position(self, manager, mock_client):
        """Test replication start with CDC stop position."""
        mock_client.call_api.return_value = {'Replication': {}}

        result = manager.start_replication(
            'arn:config', 'start-replication', cdc_stop_position='mysql-bin.000001:5678'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CdcStopPosition'] == 'mysql-bin.000001:5678'

    def test_start_replication_with_all_cdc_params(self, manager, mock_client):
        """Test replication start with all CDC parameters."""
        mock_client.call_api.return_value = {'Replication': {}}

        result = manager.start_replication(
            'arn:config',
            'resume-processing',
            cdc_start_time='2024-01-01T00:00:00Z',
            cdc_start_position='pos1',
            cdc_stop_position='pos2',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CdcStartTime'] == '2024-01-01T00:00:00Z'
        assert call_args['CdcStartPosition'] == 'pos1'
        assert call_args['CdcStopPosition'] == 'pos2'

    def test_start_replication_types(self, manager, mock_client):
        """Test replication start with different start types."""
        mock_client.call_api.return_value = {'Replication': {}}

        start_types = ['start-replication', 'resume-processing', 'reload-target']

        for start_type in start_types:
            result = manager.start_replication('arn:config', start_type)
            assert result['success'] is True
            assert start_type in result['data']['message']
            mock_client.call_api.reset_mock()


class TestServerlessReplicationManagerStopReplication:
    """Test replication stop operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_stop_replication_success(self, manager, mock_client):
        """Test successful replication stop."""
        mock_client.call_api.return_value = {'Replication': {'ReplicationArn': 'arn:repl'}}

        result = manager.stop_replication('arn:config')

        assert result['success'] is True
        assert result['data']['message'] == 'Replication stop initiated'
        mock_client.call_api.assert_called_once_with(
            'stop_replication', ReplicationConfigArn='arn:config'
        )


class TestServerlessReplicationManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_create_replication_config_api_error(self, manager, mock_client):
        """Test API error during config creation."""
        mock_client.call_api.side_effect = Exception('API Error')

        compute_config = {'MaxCapacityUnits': 16}
        with pytest.raises(Exception) as exc_info:
            manager.create_replication_config(
                'config', 'arn:src', 'arn:tgt', compute_config, 'full-load', '{}'
            )

        assert 'API Error' in str(exc_info.value)

    def test_modify_replication_config_api_error(self, manager, mock_client):
        """Test API error during config modification."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.modify_replication_config('arn:config')

        assert 'Network error' in str(exc_info.value)

    def test_delete_replication_config_api_error(self, manager, mock_client):
        """Test API error during config deletion."""
        mock_client.call_api.side_effect = Exception('Service error')

        with pytest.raises(Exception) as exc_info:
            manager.delete_replication_config('arn:config')

        assert 'Service error' in str(exc_info.value)

    def test_start_replication_api_error(self, manager, mock_client):
        """Test API error during replication start."""
        mock_client.call_api.side_effect = Exception('Timeout error')

        with pytest.raises(Exception) as exc_info:
            manager.start_replication('arn:config', 'start-replication')

        assert 'Timeout error' in str(exc_info.value)

    def test_stop_replication_api_error(self, manager, mock_client):
        """Test API error during replication stop."""
        mock_client.call_api.side_effect = Exception('Connection error')

        with pytest.raises(Exception) as exc_info:
            manager.stop_replication('arn:config')

        assert 'Connection error' in str(exc_info.value)


class TestServerlessReplicationManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessReplicationManager instance."""
        return ServerlessReplicationManager(mock_client)

    def test_list_replication_configs_max_results_boundary(self, manager, mock_client):
        """Test replication configs listing with maximum results."""
        mock_client.call_api.return_value = {'ReplicationConfigs': []}

        result = manager.list_replication_configs(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_list_replications_max_results_boundary(self, manager, mock_client):
        """Test replications listing with maximum results."""
        mock_client.call_api.return_value = {'Replications': []}

        result = manager.list_replications(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_create_replication_config_with_multiple_tags(self, manager, mock_client):
        """Test replication config creation with multiple tags."""
        mock_client.call_api.return_value = {'ReplicationConfig': {}}

        compute_config = {'MaxCapacityUnits': 16}
        tags = [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Team', 'Value': 'DataEngineering'},
            {'Key': 'Project', 'Value': 'Migration'},
        ]
        result = manager.create_replication_config(
            'config', 'arn:src', 'arn:tgt', compute_config, 'cdc', '{}', tags=tags
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Tags'] == tags
        assert len(call_args['Tags']) == 3

    def test_start_replication_without_cdc_params(self, manager, mock_client):
        """Test replication start without CDC parameters."""
        mock_client.call_api.return_value = {'Replication': {}}

        result = manager.start_replication('arn:config', 'start-replication')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'CdcStartTime' not in call_args
        assert 'CdcStartPosition' not in call_args
        assert 'CdcStopPosition' not in call_args

    def test_modify_replication_config_compute_capacity_units(self, manager, mock_client):
        """Test modifying compute capacity units."""
        mock_client.call_api.return_value = {'ReplicationConfig': {}}

        # Test different capacity unit values
        capacity_values = [1, 8, 16, 32, 64, 128, 256]

        for capacity in capacity_values:
            compute_config = {'MaxCapacityUnits': capacity}
            result = manager.modify_replication_config('arn:config', compute_config=compute_config)
            assert result['success'] is True
            mock_client.call_api.reset_mock()
