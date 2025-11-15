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

"""Comprehensive tests for MaintenanceManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.maintenance_manager import MaintenanceManager
from unittest.mock import Mock


class TestMaintenanceManagerMaintenanceActions:
    """Test maintenance action operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MaintenanceManager instance."""
        return MaintenanceManager(mock_client)

    def test_apply_pending_maintenance_action_immediate(self, manager, mock_client):
        """Test applying pending maintenance action immediately."""
        mock_client.call_api.return_value = {
            'ResourcePendingMaintenanceActions': {
                'ResourceIdentifier': 'arn:aws:dms:us-east-1:123:rep:test',
                'PendingMaintenanceActionDetails': [{'Action': 'system-update'}],
            }
        }

        result = manager.apply_pending_maintenance_action(
            'arn:aws:dms:us-east-1:123:rep:test', 'system-update', 'immediate'
        )

        assert result['success'] is True
        assert 'system-update' in result['data']['message']
        assert 'immediate' in result['data']['message']
        assert 'resource' in result['data']
        mock_client.call_api.assert_called_once_with(
            'apply_pending_maintenance_action',
            ReplicationInstanceArn='arn:aws:dms:us-east-1:123:rep:test',
            ApplyAction='system-update',
            OptInType='immediate',
        )

    def test_apply_pending_maintenance_action_next_maintenance(self, manager, mock_client):
        """Test applying pending maintenance action during next maintenance window."""
        mock_client.call_api.return_value = {'ResourcePendingMaintenanceActions': {}}

        result = manager.apply_pending_maintenance_action(
            'arn:aws:dms:us-east-1:123:rep:test', 'db-upgrade', 'next-maintenance'
        )

        assert result['success'] is True
        assert 'db-upgrade' in result['data']['message']
        assert 'next-maintenance' in result['data']['message']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['OptInType'] == 'next-maintenance'

    def test_apply_pending_maintenance_action_undo_opt_in(self, manager, mock_client):
        """Test undoing opt-in for maintenance action."""
        mock_client.call_api.return_value = {'ResourcePendingMaintenanceActions': {}}

        result = manager.apply_pending_maintenance_action(
            'arn:aws:dms:us-east-1:123:rep:test', 'system-update', 'undo-opt-in'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['OptInType'] == 'undo-opt-in'

    def test_list_pending_maintenance_actions_success(self, manager, mock_client):
        """Test successful pending maintenance actions listing."""
        mock_client.call_api.return_value = {
            'PendingMaintenanceActions': [
                {
                    'ResourceIdentifier': 'arn:1',
                    'PendingMaintenanceActionDetails': [{'Action': 'system-update'}],
                },
                {
                    'ResourceIdentifier': 'arn:2',
                    'PendingMaintenanceActionDetails': [{'Action': 'db-upgrade'}],
                },
            ]
        }

        result = manager.list_pending_maintenance_actions()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'pending_maintenance_actions' in result['data']

    def test_list_pending_maintenance_actions_with_resource_arn(self, manager, mock_client):
        """Test listing pending maintenance actions for specific resource."""
        mock_client.call_api.return_value = {'PendingMaintenanceActions': []}

        result = manager.list_pending_maintenance_actions(
            resource_arn='arn:aws:dms:us-east-1:123:rep:test'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationInstanceArn'] == 'arn:aws:dms:us-east-1:123:rep:test'

    def test_list_pending_maintenance_actions_with_filters(self, manager, mock_client):
        """Test listing pending maintenance actions with filters."""
        mock_client.call_api.return_value = {'PendingMaintenanceActions': []}

        filters = [{'Name': 'action', 'Values': ['system-update']}]
        result = manager.list_pending_maintenance_actions(
            filters=filters, max_results=50, marker='token'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_pending_maintenance_actions_with_pagination(self, manager, mock_client):
        """Test listing pending maintenance actions with pagination."""
        mock_client.call_api.return_value = {
            'PendingMaintenanceActions': [],
            'Marker': 'next-token',
        }

        result = manager.list_pending_maintenance_actions()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'


class TestMaintenanceManagerAccountAttributes:
    """Test account attributes operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MaintenanceManager instance."""
        return MaintenanceManager(mock_client)

    def test_get_account_attributes_success(self, manager, mock_client):
        """Test successful account attributes retrieval."""
        mock_client.call_api.return_value = {
            'AccountQuotas': [
                {'AccountQuotaName': 'ReplicationInstances', 'Max': 20, 'Used': 5},
                {'AccountQuotaName': 'AllocatedStorage', 'Max': 10000, 'Used': 500},
            ],
            'UniqueAccountIdentifier': 'account-123',
        }

        result = manager.get_account_attributes()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'account_quotas' in result['data']
        assert result['data']['unique_account_identifier'] == 'account-123'

    def test_get_account_attributes_empty_quotas(self, manager, mock_client):
        """Test account attributes with empty quotas."""
        mock_client.call_api.return_value = {
            'AccountQuotas': [],
            'UniqueAccountIdentifier': 'account-456',
        }

        result = manager.get_account_attributes()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['account_quotas'] == []

    def test_get_account_attributes_no_unique_identifier(self, manager, mock_client):
        """Test account attributes without unique identifier."""
        mock_client.call_api.return_value = {'AccountQuotas': []}

        result = manager.get_account_attributes()

        assert result['success'] is True
        assert result['data']['unique_account_identifier'] is None


class TestMaintenanceManagerTagOperations:
    """Test tag management operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MaintenanceManager instance."""
        return MaintenanceManager(mock_client)

    def test_add_tags_success(self, manager, mock_client):
        """Test successful tag addition."""
        mock_client.call_api.return_value = {}

        tags = [{'Key': 'Environment', 'Value': 'Production'}, {'Key': 'Owner', 'Value': 'Team'}]
        result = manager.add_tags('arn:aws:dms:us-east-1:123:rep:test', tags)

        assert result['success'] is True
        assert result['data']['message'] == 'Tags added successfully'
        assert result['data']['tags_added'] == 2
        assert result['data']['resource_arn'] == 'arn:aws:dms:us-east-1:123:rep:test'
        mock_client.call_api.assert_called_once_with(
            'add_tags_to_resource',
            ResourceArn='arn:aws:dms:us-east-1:123:rep:test',
            Tags=tags,
        )

    def test_add_tags_single(self, manager, mock_client):
        """Test adding single tag."""
        mock_client.call_api.return_value = {}

        tags = [{'Key': 'Environment', 'Value': 'Development'}]
        result = manager.add_tags('arn:aws:dms:us-east-1:123:rep:test', tags)

        assert result['success'] is True
        assert result['data']['tags_added'] == 1

    def test_add_tags_multiple(self, manager, mock_client):
        """Test adding multiple tags."""
        mock_client.call_api.return_value = {}

        tags = [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Owner', 'Value': 'Team'},
            {'Key': 'Project', 'Value': 'DMS'},
            {'Key': 'CostCenter', 'Value': 'Engineering'},
        ]
        result = manager.add_tags('arn:test', tags)

        assert result['success'] is True
        assert result['data']['tags_added'] == 4

    def test_remove_tags_success(self, manager, mock_client):
        """Test successful tag removal."""
        mock_client.call_api.return_value = {}

        tag_keys = ['Environment', 'Owner']
        result = manager.remove_tags('arn:aws:dms:us-east-1:123:rep:test', tag_keys)

        assert result['success'] is True
        assert result['data']['message'] == 'Tags removed successfully'
        assert result['data']['tags_removed'] == 2
        assert result['data']['resource_arn'] == 'arn:aws:dms:us-east-1:123:rep:test'
        mock_client.call_api.assert_called_once_with(
            'remove_tags_from_resource',
            ResourceArn='arn:aws:dms:us-east-1:123:rep:test',
            TagKeys=tag_keys,
        )

    def test_remove_tags_single(self, manager, mock_client):
        """Test removing single tag."""
        mock_client.call_api.return_value = {}

        result = manager.remove_tags('arn:test', ['Environment'])

        assert result['success'] is True
        assert result['data']['tags_removed'] == 1

    def test_list_tags_success(self, manager, mock_client):
        """Test successful tag listing."""
        mock_client.call_api.return_value = {
            'TagList': [
                {'Key': 'Environment', 'Value': 'Production'},
                {'Key': 'Owner', 'Value': 'Team'},
            ]
        }

        result = manager.list_tags('arn:aws:dms:us-east-1:123:rep:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'tags' in result['data']
        assert result['data']['resource_arn'] == 'arn:aws:dms:us-east-1:123:rep:test'

    def test_list_tags_empty(self, manager, mock_client):
        """Test listing tags with empty result."""
        mock_client.call_api.return_value = {'TagList': []}

        result = manager.list_tags('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['tags'] == []


class TestMaintenanceManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MaintenanceManager instance."""
        return MaintenanceManager(mock_client)

    def test_apply_pending_maintenance_action_api_error(self, manager, mock_client):
        """Test API error during maintenance action application."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.apply_pending_maintenance_action('arn:test', 'action', 'immediate')

        assert 'API Error' in str(exc_info.value)

    def test_list_pending_maintenance_actions_api_error(self, manager, mock_client):
        """Test API error during pending maintenance actions listing."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.list_pending_maintenance_actions()

        assert 'Network error' in str(exc_info.value)

    def test_get_account_attributes_api_error(self, manager, mock_client):
        """Test API error during account attributes retrieval."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.get_account_attributes()

        assert 'API Error' in str(exc_info.value)

    def test_add_tags_api_error(self, manager, mock_client):
        """Test API error during tag addition."""
        mock_client.call_api.side_effect = Exception('Tag error')

        with pytest.raises(Exception) as exc_info:
            manager.add_tags('arn:test', [{'Key': 'test', 'Value': 'value'}])

        assert 'Tag error' in str(exc_info.value)

    def test_remove_tags_api_error(self, manager, mock_client):
        """Test API error during tag removal."""
        mock_client.call_api.side_effect = Exception('Remove error')

        with pytest.raises(Exception) as exc_info:
            manager.remove_tags('arn:test', ['Environment'])

        assert 'Remove error' in str(exc_info.value)

    def test_list_tags_api_error(self, manager, mock_client):
        """Test API error during tag listing."""
        mock_client.call_api.side_effect = Exception('List error')

        with pytest.raises(Exception) as exc_info:
            manager.list_tags('arn:test')

        assert 'List error' in str(exc_info.value)


class TestMaintenanceManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MaintenanceManager instance."""
        return MaintenanceManager(mock_client)

    def test_list_pending_maintenance_actions_with_max_results_boundary(
        self, manager, mock_client
    ):
        """Test list pending maintenance actions with maximum results."""
        mock_client.call_api.return_value = {'PendingMaintenanceActions': []}

        result = manager.list_pending_maintenance_actions(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_add_tags_empty_list(self, manager, mock_client):
        """Test adding empty tag list."""
        mock_client.call_api.return_value = {}

        result = manager.add_tags('arn:test', [])

        assert result['success'] is True
        assert result['data']['tags_added'] == 0

    def test_remove_tags_empty_list(self, manager, mock_client):
        """Test removing empty tag key list."""
        mock_client.call_api.return_value = {}

        result = manager.remove_tags('arn:test', [])

        assert result['success'] is True
        assert result['data']['tags_removed'] == 0

    def test_add_tags_with_special_characters(self, manager, mock_client):
        """Test adding tags with special characters."""
        mock_client.call_api.return_value = {}

        tags = [
            {'Key': 'Project:Name', 'Value': 'DMS-Migration'},
            {'Key': 'Owner_Email', 'Value': 'team@example.com'},
        ]
        result = manager.add_tags('arn:test', tags)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Tags'] == tags

    def test_list_pending_maintenance_actions_without_optional_params(self, manager, mock_client):
        """Test listing pending maintenance actions without optional parameters."""
        mock_client.call_api.return_value = {'PendingMaintenanceActions': []}

        result = manager.list_pending_maintenance_actions()

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 100
        assert 'ReplicationInstanceArn' not in call_args
        assert 'Filters' not in call_args
        assert 'Marker' not in call_args

    def test_get_account_attributes_multiple_quotas(self, manager, mock_client):
        """Test account attributes with multiple quotas."""
        mock_client.call_api.return_value = {
            'AccountQuotas': [
                {'AccountQuotaName': 'ReplicationInstances', 'Max': 20},
                {'AccountQuotaName': 'AllocatedStorage', 'Max': 10000},
                {'AccountQuotaName': 'Endpoints', 'Max': 100},
                {'AccountQuotaName': 'Certificates', 'Max': 50},
            ],
            'UniqueAccountIdentifier': 'account-abc',
        }

        result = manager.get_account_attributes()

        assert result['success'] is True
        assert result['data']['count'] == 4

    def test_list_pending_maintenance_actions_multiple_pages(self, manager, mock_client):
        """Test listing pending maintenance actions across multiple pages."""
        # First call
        mock_client.call_api.return_value = {
            'PendingMaintenanceActions': [{'ResourceIdentifier': 'arn:1'}],
            'Marker': 'token-1',
        }

        result1 = manager.list_pending_maintenance_actions()

        assert result1['success'] is True
        assert result1['data']['count'] == 1
        assert result1['data']['next_marker'] == 'token-1'

        # Second call with token
        mock_client.call_api.return_value = {
            'PendingMaintenanceActions': [{'ResourceIdentifier': 'arn:2'}]
        }

        result2 = manager.list_pending_maintenance_actions(marker='token-1')

        assert result2['success'] is True
        assert result2['data']['count'] == 1
        assert 'next_marker' not in result2['data']

    def test_apply_maintenance_action_with_long_arn(self, manager, mock_client):
        """Test applying maintenance action with long ARN."""
        mock_client.call_api.return_value = {'ResourcePendingMaintenanceActions': {}}

        long_arn = 'arn:aws:dms:us-east-1:123456789012:rep:' + 'a' * 100
        result = manager.apply_pending_maintenance_action(long_arn, 'action', 'immediate')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationInstanceArn'] == long_arn

    def test_list_tags_with_many_tags(self, manager, mock_client):
        """Test listing many tags."""
        tags = [{'Key': f'Tag{i}', 'Value': f'Value{i}'} for i in range(50)]
        mock_client.call_api.return_value = {'TagList': tags}

        result = manager.list_tags('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 50
