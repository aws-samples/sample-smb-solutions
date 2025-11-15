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

"""Comprehensive tests for ReplicationInstanceManager module."""

import pytest
from awslabs.aws_dms_mcp_server.exceptions import (
    DMSInvalidParameterException,
    DMSResourceNotFoundException,
)
from awslabs.aws_dms_mcp_server.utils.replication_instance_manager import (
    ReplicationInstanceManager,
)
from unittest.mock import Mock, patch


class TestReplicationInstanceManagerBasicOperations:
    """Test basic replication instance operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_list_instances_success(self, manager, mock_client):
        """Test successful instance listing."""
        mock_client.call_api.return_value = {
            'ReplicationInstances': [
                {'ReplicationInstanceIdentifier': 'instance-1', 'Status': 'available'},
                {'ReplicationInstanceIdentifier': 'instance-2', 'Status': 'available'},
            ]
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_instance.side_effect = lambda x: x

            result = manager.list_instances()

            assert result['success'] is True
            assert result['data']['count'] == 2
            assert 'instances' in result['data']

    def test_list_instances_with_filters(self, manager, mock_client):
        """Test listing instances with filters."""
        mock_client.call_api.return_value = {'ReplicationInstances': []}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ):
            filters = [{'Name': 'replication-instance-class', 'Values': ['dms.t3.medium']}]
            result = manager.list_instances(filters=filters, max_results=50, marker='token')

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['Filters'] == filters
            assert call_args['MaxRecords'] == 50
            assert call_args['Marker'] == 'token'

    def test_list_instances_with_pagination(self, manager, mock_client):
        """Test listing instances with pagination."""
        mock_client.call_api.return_value = {'ReplicationInstances': [], 'Marker': 'next-token'}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ):
            result = manager.list_instances()

            assert result['success'] is True
            assert 'next_marker' in result['data']
            assert result['data']['next_marker'] == 'next-token'

    def test_list_instances_empty_result(self, manager, mock_client):
        """Test listing instances with empty result."""
        mock_client.call_api.return_value = {'ReplicationInstances': []}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ):
            result = manager.list_instances()

            assert result['success'] is True
            assert result['data']['count'] == 0
            assert result['data']['instances'] == []


class TestReplicationInstanceManagerCreateInstance:
    """Test replication instance creation."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_create_instance_success(self, manager, mock_client):
        """Test successful instance creation."""
        mock_client.call_api.return_value = {
            'ReplicationInstance': {'ReplicationInstanceIdentifier': 'test-instance'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_instance.return_value = {'identifier': 'test-instance'}

            params = {
                'ReplicationInstanceIdentifier': 'test-instance',
                'ReplicationInstanceClass': 'dms.t3.medium',
                'AllocatedStorage': 50,
            }
            result = manager.create_instance(params)

            assert result['success'] is True
            assert 'instance' in result['data']
            assert result['data']['message'] == 'Replication instance creation initiated'

    def test_create_instance_missing_identifier(self, manager, mock_client):
        """Test instance creation with missing identifier."""
        params = {'ReplicationInstanceClass': 'dms.t3.medium'}

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_instance(params)

        assert 'Missing required parameter' in str(exc_info.value)
        assert 'ReplicationInstanceIdentifier' in str(exc_info.value)

    def test_create_instance_missing_class(self, manager, mock_client):
        """Test instance creation with missing class."""
        params = {'ReplicationInstanceIdentifier': 'test-instance'}

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_instance(params)

        assert 'Missing required parameter' in str(exc_info.value)
        assert 'ReplicationInstanceClass' in str(exc_info.value)

    def test_create_instance_invalid_class(self, manager, mock_client):
        """Test instance creation with invalid class."""
        params = {
            'ReplicationInstanceIdentifier': 'test-instance',
            'ReplicationInstanceClass': 'invalid.instance.class',
        }

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_instance(params)

        assert 'Invalid instance class' in str(exc_info.value)


class TestReplicationInstanceManagerValidation:
    """Test instance class validation."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_validate_instance_class_valid_t2_micro(self, manager):
        """Test validation of valid t2.micro instance class."""
        assert manager.validate_instance_class('dms.t2.micro') is True

    def test_validate_instance_class_valid_t3_medium(self, manager):
        """Test validation of valid t3.medium instance class."""
        assert manager.validate_instance_class('dms.t3.medium') is True

    def test_validate_instance_class_valid_c5_large(self, manager):
        """Test validation of valid c5.large instance class."""
        assert manager.validate_instance_class('dms.c5.large') is True

    def test_validate_instance_class_valid_r5_xlarge(self, manager):
        """Test validation of valid r5.xlarge instance class."""
        assert manager.validate_instance_class('dms.r5.xlarge') is True

    def test_validate_instance_class_invalid(self, manager):
        """Test validation of invalid instance class."""
        assert manager.validate_instance_class('invalid.class') is False

    def test_validate_instance_class_empty(self, manager):
        """Test validation of empty instance class."""
        assert manager.validate_instance_class('') is False

    def test_validate_all_supported_classes(self, manager):
        """Test validation of all supported instance classes."""
        supported_classes = [
            'dms.t2.micro',
            'dms.t2.small',
            'dms.t2.medium',
            'dms.t2.large',
            'dms.t3.micro',
            'dms.t3.small',
            'dms.t3.medium',
            'dms.t3.large',
            'dms.c4.large',
            'dms.c4.xlarge',
            'dms.c4.2xlarge',
            'dms.c4.4xlarge',
            'dms.c5.large',
            'dms.c5.xlarge',
            'dms.c5.2xlarge',
            'dms.c5.4xlarge',
            'dms.r4.large',
            'dms.r4.xlarge',
            'dms.r4.2xlarge',
            'dms.r4.4xlarge',
            'dms.r5.large',
            'dms.r5.xlarge',
            'dms.r5.2xlarge',
            'dms.r5.4xlarge',
        ]

        for instance_class in supported_classes:
            assert manager.validate_instance_class(instance_class) is True


class TestReplicationInstanceManagerInstanceDetails:
    """Test getting instance details."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_get_instance_details_success(self, manager, mock_client):
        """Test successful instance details retrieval."""
        mock_client.call_api.return_value = {
            'ReplicationInstances': [{'ReplicationInstanceIdentifier': 'test-instance'}]
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_instance.return_value = {'identifier': 'test-instance'}

            result = manager.get_instance_details('arn:aws:dms:us-east-1:123:rep:test')

            assert result['success'] is True
            assert 'identifier' in result['data']

    def test_get_instance_details_not_found(self, manager, mock_client):
        """Test instance details when instance not found."""
        mock_client.call_api.return_value = {'ReplicationInstances': []}

        with pytest.raises(DMSResourceNotFoundException) as exc_info:
            manager.get_instance_details('arn:aws:dms:us-east-1:123:rep:test')

        assert 'Replication instance not found' in str(exc_info.value)


class TestReplicationInstanceManagerModifyDelete:
    """Test instance modification and deletion."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_modify_instance_success(self, manager, mock_client):
        """Test successful instance modification."""
        mock_client.call_api.return_value = {
            'ReplicationInstance': {'ReplicationInstanceIdentifier': 'test-instance'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_instance.return_value = {'identifier': 'test-instance'}

            params = {
                'ReplicationInstanceArn': 'arn:aws:dms:us-east-1:123:rep:test',
                'AllocatedStorage': 100,
            }
            result = manager.modify_instance(params)

            assert result['success'] is True
            assert result['data']['message'] == 'Instance modified successfully'
            assert 'instance' in result['data']

    def test_delete_instance_success(self, manager, mock_client):
        """Test successful instance deletion."""
        mock_client.call_api.return_value = {
            'ReplicationInstance': {'ReplicationInstanceIdentifier': 'test-instance'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_instance.return_value = {'identifier': 'test-instance'}

            result = manager.delete_instance('arn:aws:dms:us-east-1:123:rep:test')

            assert result['success'] is True
            assert result['data']['message'] == 'Instance deleted successfully'
            assert 'instance' in result['data']
            mock_client.call_api.assert_called_once_with(
                'delete_replication_instance',
                ReplicationInstanceArn='arn:aws:dms:us-east-1:123:rep:test',
            )


class TestReplicationInstanceManagerReboot:
    """Test instance reboot operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_reboot_instance_success(self, manager, mock_client):
        """Test successful instance reboot."""
        mock_client.call_api.return_value = {
            'ReplicationInstance': {'ReplicationInstanceIdentifier': 'test-instance'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_instance.return_value = {'identifier': 'test-instance'}

            result = manager.reboot_instance('arn:aws:dms:us-east-1:123:rep:test')

            assert result['success'] is True
            assert result['data']['message'] == 'Instance reboot initiated'
            assert 'instance' in result['data']
            mock_client.call_api.assert_called_once_with(
                'reboot_replication_instance',
                ReplicationInstanceArn='arn:aws:dms:us-east-1:123:rep:test',
                ForceFailover=False,
            )

    def test_reboot_instance_with_force_failover(self, manager, mock_client):
        """Test instance reboot with force failover."""
        mock_client.call_api.return_value = {
            'ReplicationInstance': {'ReplicationInstanceIdentifier': 'test-instance'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.replication_instance_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_instance.return_value = {'identifier': 'test-instance'}

            result = manager.reboot_instance(
                'arn:aws:dms:us-east-1:123:rep:test', force_failover=True
            )

            assert result['success'] is True
            assert result['data']['message'] == 'Instance reboot initiated'
            call_args = mock_client.call_api.call_args[1]
            assert call_args['ForceFailover'] is True


class TestReplicationInstanceManagerOrderable:
    """Test orderable instance operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_list_orderable_instances_success(self, manager, mock_client):
        """Test successful orderable instances listing."""
        mock_client.call_api.return_value = {
            'OrderableReplicationInstances': [
                {'ReplicationInstanceClass': 'dms.t3.medium'},
                {'ReplicationInstanceClass': 'dms.t3.large'},
            ]
        }

        result = manager.list_orderable_instances()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'orderable_instances' in result['data']

    def test_list_orderable_instances_with_pagination(self, manager, mock_client):
        """Test orderable instances with pagination."""
        mock_client.call_api.return_value = {
            'OrderableReplicationInstances': [],
            'Marker': 'next-token',
        }

        result = manager.list_orderable_instances(max_results=50, marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_orderable_instances_empty(self, manager, mock_client):
        """Test orderable instances with empty result."""
        mock_client.call_api.return_value = {'OrderableReplicationInstances': []}

        result = manager.list_orderable_instances()

        assert result['success'] is True
        assert result['data']['count'] == 0


class TestReplicationInstanceManagerTaskLogs:
    """Test task log operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_get_task_logs_success(self, manager, mock_client):
        """Test successful task logs retrieval."""
        mock_client.call_api.return_value = {
            'ReplicationInstanceTaskLogs': [
                {'ReplicationTaskName': 'task-1', 'ReplicationTaskArn': 'arn:1'},
                {'ReplicationTaskName': 'task-2', 'ReplicationTaskArn': 'arn:2'},
            ]
        }

        result = manager.get_task_logs('arn:aws:dms:us-east-1:123:rep:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'task_logs' in result['data']
        mock_client.call_api.assert_called_once_with(
            'describe_replication_instance_task_logs',
            ReplicationInstanceArn='arn:aws:dms:us-east-1:123:rep:test',
            MaxRecords=100,
        )

    def test_get_task_logs_with_pagination(self, manager, mock_client):
        """Test task logs with pagination."""
        mock_client.call_api.return_value = {
            'ReplicationInstanceTaskLogs': [],
            'Marker': 'next-token',
        }

        result = manager.get_task_logs(
            'arn:aws:dms:us-east-1:123:rep:test', max_results=50, marker='token'
        )

        assert result['success'] is True
        assert 'next_marker' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_get_task_logs_empty(self, manager, mock_client):
        """Test task logs with empty result."""
        mock_client.call_api.return_value = {'ReplicationInstanceTaskLogs': []}

        result = manager.get_task_logs('arn:aws:dms:us-east-1:123:rep:test')

        assert result['success'] is True
        assert result['data']['count'] == 0


class TestReplicationInstanceManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ReplicationInstanceManager instance."""
        return ReplicationInstanceManager(mock_client)

    def test_list_instances_api_error(self, manager, mock_client):
        """Test API error during instance listing."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.list_instances()

        assert 'API Error' in str(exc_info.value)

    def test_create_instance_api_error(self, manager, mock_client):
        """Test API error during instance creation."""
        mock_client.call_api.side_effect = Exception('Network error')

        params = {
            'ReplicationInstanceIdentifier': 'test',
            'ReplicationInstanceClass': 'dms.t3.medium',
        }

        with pytest.raises(Exception) as exc_info:
            manager.create_instance(params)

        assert 'Network error' in str(exc_info.value)

    def test_delete_instance_api_error(self, manager, mock_client):
        """Test API error during instance deletion."""
        mock_client.call_api.side_effect = Exception('Delete failed')

        with pytest.raises(Exception) as exc_info:
            manager.delete_instance('arn:aws:dms:us-east-1:123:rep:test')

        assert 'Delete failed' in str(exc_info.value)
