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

"""Comprehensive tests for SubnetGroupManager module."""

import pytest
from awslabs.aws_dms_mcp_server.exceptions import DMSInvalidParameterException
from awslabs.aws_dms_mcp_server.utils.subnet_group_manager import SubnetGroupManager
from unittest.mock import Mock


class TestSubnetGroupManagerCreateOperations:
    """Test subnet group creation operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create SubnetGroupManager instance."""
        return SubnetGroupManager(mock_client)

    def test_create_subnet_group_success(self, manager, mock_client):
        """Test successful subnet group creation."""
        mock_client.call_api.return_value = {
            'ReplicationSubnetGroup': {
                'ReplicationSubnetGroupIdentifier': 'test-subnet-group',
                'SubnetIds': ['subnet-1', 'subnet-2'],
            }
        }

        result = manager.create_subnet_group(
            identifier='test-subnet-group',
            description='Test subnet group',
            subnet_ids=['subnet-1', 'subnet-2'],
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Replication subnet group created successfully'
        assert 'subnet_group' in result['data']
        mock_client.call_api.assert_called_once()

    def test_create_subnet_group_with_tags(self, manager, mock_client):
        """Test subnet group creation with tags."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        tags = [{'Key': 'Environment', 'Value': 'Production'}, {'Key': 'Owner', 'Value': 'Team'}]
        result = manager.create_subnet_group(
            identifier='test-subnet-group',
            description='Test subnet group',
            subnet_ids=['subnet-1'],
            tags=tags,
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Tags'] == tags

    def test_create_subnet_group_single_subnet(self, manager, mock_client):
        """Test subnet group creation with single subnet."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        result = manager.create_subnet_group(
            identifier='test-subnet-group',
            description='Single subnet group',
            subnet_ids=['subnet-1'],
        )

        assert result['success'] is True

    def test_create_subnet_group_multiple_subnets(self, manager, mock_client):
        """Test subnet group creation with multiple subnets."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        subnet_ids = ['subnet-1', 'subnet-2', 'subnet-3', 'subnet-4']
        result = manager.create_subnet_group(
            identifier='test-subnet-group', description='Multi subnet group', subnet_ids=subnet_ids
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SubnetIds'] == subnet_ids

    def test_create_subnet_group_empty_subnet_ids(self, manager, mock_client):
        """Test subnet group creation with empty subnet IDs."""
        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_subnet_group(
                identifier='test-subnet-group', description='Test', subnet_ids=[]
            )

        assert 'At least one subnet ID is required' in str(exc_info.value)

    def test_create_subnet_group_without_tags(self, manager, mock_client):
        """Test subnet group creation without tags."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        result = manager.create_subnet_group(
            identifier='test-subnet-group', description='No tags', subnet_ids=['subnet-1']
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Tags' not in call_args


class TestSubnetGroupManagerModifyOperations:
    """Test subnet group modification operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create SubnetGroupManager instance."""
        return SubnetGroupManager(mock_client)

    def test_modify_subnet_group_description_only(self, manager, mock_client):
        """Test modifying subnet group description only."""
        mock_client.call_api.return_value = {
            'ReplicationSubnetGroup': {'ReplicationSubnetGroupIdentifier': 'test-subnet-group'}
        }

        result = manager.modify_subnet_group(
            identifier='test-subnet-group', description='Updated description'
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Replication subnet group modified successfully'
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationSubnetGroupDescription'] == 'Updated description'
        assert 'SubnetIds' not in call_args

    def test_modify_subnet_group_subnet_ids_only(self, manager, mock_client):
        """Test modifying subnet group subnet IDs only."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        result = manager.modify_subnet_group(
            identifier='test-subnet-group', subnet_ids=['subnet-3', 'subnet-4']
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SubnetIds'] == ['subnet-3', 'subnet-4']
        assert 'ReplicationSubnetGroupDescription' not in call_args

    def test_modify_subnet_group_both_params(self, manager, mock_client):
        """Test modifying both description and subnet IDs."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        result = manager.modify_subnet_group(
            identifier='test-subnet-group',
            description='New description',
            subnet_ids=['subnet-5', 'subnet-6'],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationSubnetGroupDescription'] == 'New description'
        assert call_args['SubnetIds'] == ['subnet-5', 'subnet-6']

    def test_modify_subnet_group_empty_subnet_ids(self, manager, mock_client):
        """Test modifying subnet group with empty subnet IDs.

        Note: Empty list [] is falsy in Python, so validation is skipped.
        This test documents the current behavior.
        """
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        # Empty list is falsy, so validation is skipped and no exception is raised
        result = manager.modify_subnet_group(identifier='test-subnet-group', subnet_ids=[])

        assert result['success'] is True
        # SubnetIds parameter should not be included when list is empty
        call_args = mock_client.call_api.call_args[1]
        assert 'SubnetIds' not in call_args

    def test_modify_subnet_group_no_changes(self, manager, mock_client):
        """Test modifying subnet group with no parameters."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        result = manager.modify_subnet_group(identifier='test-subnet-group')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationSubnetGroupIdentifier'] == 'test-subnet-group'
        assert 'ReplicationSubnetGroupDescription' not in call_args
        assert 'SubnetIds' not in call_args


class TestSubnetGroupManagerListOperations:
    """Test subnet group listing operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create SubnetGroupManager instance."""
        return SubnetGroupManager(mock_client)

    def test_list_subnet_groups_success(self, manager, mock_client):
        """Test successful subnet group listing."""
        mock_client.call_api.return_value = {
            'ReplicationSubnetGroups': [
                {'ReplicationSubnetGroupIdentifier': 'group-1', 'SubnetIds': ['subnet-1']},
                {'ReplicationSubnetGroupIdentifier': 'group-2', 'SubnetIds': ['subnet-2']},
            ]
        }

        result = manager.list_subnet_groups()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'subnet_groups' in result['data']

    def test_list_subnet_groups_with_filters(self, manager, mock_client):
        """Test listing subnet groups with filters."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroups': []}

        filters = [{'Name': 'subnet-group-identifier', 'Values': ['test-group']}]
        result = manager.list_subnet_groups(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_subnet_groups_with_pagination(self, manager, mock_client):
        """Test listing subnet groups with pagination."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroups': [], 'Marker': 'next-token'}

        result = manager.list_subnet_groups()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_list_subnet_groups_empty(self, manager, mock_client):
        """Test listing subnet groups with empty result."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroups': []}

        result = manager.list_subnet_groups()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['subnet_groups'] == []

    def test_list_subnet_groups_without_optional_params(self, manager, mock_client):
        """Test listing subnet groups without optional parameters."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroups': []}

        result = manager.list_subnet_groups()

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 100
        assert 'Filters' not in call_args
        assert 'Marker' not in call_args


class TestSubnetGroupManagerDeleteOperations:
    """Test subnet group deletion operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create SubnetGroupManager instance."""
        return SubnetGroupManager(mock_client)

    def test_delete_subnet_group_success(self, manager, mock_client):
        """Test successful subnet group deletion."""
        mock_client.call_api.return_value = {}

        result = manager.delete_subnet_group('test-subnet-group')

        assert result['success'] is True
        assert result['data']['message'] == 'Replication subnet group deleted successfully'
        assert result['data']['identifier'] == 'test-subnet-group'
        mock_client.call_api.assert_called_once_with(
            'delete_replication_subnet_group',
            ReplicationSubnetGroupIdentifier='test-subnet-group',
        )

    def test_delete_subnet_group_with_special_characters(self, manager, mock_client):
        """Test deleting subnet group with special characters in identifier."""
        mock_client.call_api.return_value = {}

        result = manager.delete_subnet_group('test-subnet-group-123_ABC')

        assert result['success'] is True
        assert result['data']['identifier'] == 'test-subnet-group-123_ABC'


class TestSubnetGroupManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create SubnetGroupManager instance."""
        return SubnetGroupManager(mock_client)

    def test_create_subnet_group_api_error(self, manager, mock_client):
        """Test API error during subnet group creation."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.create_subnet_group('test', 'desc', ['subnet-1'])

        assert 'API Error' in str(exc_info.value)

    def test_modify_subnet_group_api_error(self, manager, mock_client):
        """Test API error during subnet group modification."""
        mock_client.call_api.side_effect = Exception('Modify failed')

        with pytest.raises(Exception) as exc_info:
            manager.modify_subnet_group('test', description='new')

        assert 'Modify failed' in str(exc_info.value)

    def test_list_subnet_groups_api_error(self, manager, mock_client):
        """Test API error during subnet group listing."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.list_subnet_groups()

        assert 'Network error' in str(exc_info.value)

    def test_delete_subnet_group_api_error(self, manager, mock_client):
        """Test API error during subnet group deletion."""
        mock_client.call_api.side_effect = Exception('Delete failed')

        with pytest.raises(Exception) as exc_info:
            manager.delete_subnet_group('test')

        assert 'Delete failed' in str(exc_info.value)


class TestSubnetGroupManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create SubnetGroupManager instance."""
        return SubnetGroupManager(mock_client)

    def test_create_subnet_group_long_description(self, manager, mock_client):
        """Test creating subnet group with long description."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        long_description = 'A' * 500
        result = manager.create_subnet_group('test', long_description, ['subnet-1'])

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationSubnetGroupDescription'] == long_description

    def test_create_subnet_group_many_subnets(self, manager, mock_client):
        """Test creating subnet group with many subnets."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        subnet_ids = [f'subnet-{i}' for i in range(20)]
        result = manager.create_subnet_group('test', 'Many subnets', subnet_ids)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['SubnetIds']) == 20

    def test_list_subnet_groups_with_max_results_boundary(self, manager, mock_client):
        """Test list subnet groups with maximum results."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroups': []}

        result = manager.list_subnet_groups(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_list_subnet_groups_multiple_pages(self, manager, mock_client):
        """Test listing subnet groups across multiple pages."""
        # First call
        mock_client.call_api.return_value = {
            'ReplicationSubnetGroups': [{'ReplicationSubnetGroupIdentifier': 'group-1'}],
            'Marker': 'token-1',
        }

        result1 = manager.list_subnet_groups()

        assert result1['success'] is True
        assert result1['data']['count'] == 1
        assert result1['data']['next_marker'] == 'token-1'

        # Second call with token
        mock_client.call_api.return_value = {
            'ReplicationSubnetGroups': [{'ReplicationSubnetGroupIdentifier': 'group-2'}]
        }

        result2 = manager.list_subnet_groups(marker='token-1')

        assert result2['success'] is True
        assert result2['data']['count'] == 1
        assert 'next_marker' not in result2['data']

    def test_create_subnet_group_with_multiple_tags(self, manager, mock_client):
        """Test creating subnet group with multiple tags."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        tags = [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Owner', 'Value': 'Team'},
            {'Key': 'Project', 'Value': 'DMS-Migration'},
            {'Key': 'CostCenter', 'Value': 'Engineering'},
        ]
        result = manager.create_subnet_group('test', 'Tagged group', ['subnet-1'], tags=tags)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['Tags']) == 4

    def test_modify_subnet_group_replace_all_subnets(self, manager, mock_client):
        """Test modifying subnet group by replacing all subnets."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroup': {}}

        new_subnets = ['subnet-4', 'subnet-5', 'subnet-6']

        result = manager.modify_subnet_group('test', subnet_ids=new_subnets)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SubnetIds'] == new_subnets
        assert 'subnet-1' not in call_args['SubnetIds']

    def test_delete_subnet_group_empty_identifier(self, manager, mock_client):
        """Test deleting subnet group with empty identifier."""
        mock_client.call_api.return_value = {}

        result = manager.delete_subnet_group('')

        assert result['success'] is True
        assert result['data']['identifier'] == ''

    def test_list_subnet_groups_with_multiple_filters(self, manager, mock_client):
        """Test listing subnet groups with multiple filters."""
        mock_client.call_api.return_value = {'ReplicationSubnetGroups': []}

        filters = [
            {'Name': 'vpc-id', 'Values': ['vpc-123']},
            {'Name': 'subnet-availability-zone', 'Values': ['us-east-1a', 'us-east-1b']},
        ]
        result = manager.list_subnet_groups(filters=filters)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['Filters']) == 2
