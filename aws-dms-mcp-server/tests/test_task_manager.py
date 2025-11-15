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

"""Comprehensive tests for TaskManager module."""

import json
import pytest
from awslabs.aws_dms_mcp_server.exceptions import (
    DMSInvalidParameterException,
    DMSValidationException,
)
from awslabs.aws_dms_mcp_server.utils.task_manager import TaskManager
from unittest.mock import Mock, patch


class TestTaskManagerBasicOperations:
    """Test basic task CRUD operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    def test_list_tasks_success(self, manager, mock_client):
        """Test successful task listing."""
        mock_client.call_api.return_value = {
            'ReplicationTasks': [
                {'ReplicationTaskIdentifier': 'task-1'},
                {'ReplicationTaskIdentifier': 'task-2'},
            ]
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.side_effect = lambda x: x

            result = manager.list_tasks()

            assert result['success'] is True
            assert result['data']['count'] == 2
            assert 'tasks' in result['data']

    def test_list_tasks_with_filters(self, manager, mock_client):
        """Test listing tasks with filters."""
        mock_client.call_api.return_value = {'ReplicationTasks': []}

        with patch('awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'):
            filters = [{'Name': 'replication-task-arn', 'Values': ['arn:test']}]
            result = manager.list_tasks(filters=filters, max_results=50, marker='token')

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['Filters'] == filters
            assert call_args['MaxRecords'] == 50
            assert call_args['Marker'] == 'token'

    def test_list_tasks_without_settings(self, manager, mock_client):
        """Test listing tasks without settings."""
        mock_client.call_api.return_value = {'ReplicationTasks': []}

        with patch('awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'):
            result = manager.list_tasks(without_settings=True)

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['WithoutSettings'] is True

    def test_list_tasks_with_pagination(self, manager, mock_client):
        """Test listing tasks with pagination."""
        mock_client.call_api.return_value = {'ReplicationTasks': [], 'Marker': 'next-token'}

        with patch('awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'):
            result = manager.list_tasks()

            assert result['success'] is True
            assert 'next_marker' in result['data']
            assert result['data']['next_marker'] == 'next-token'

    def test_delete_task_success(self, manager, mock_client):
        """Test successful task deletion."""
        mock_client.call_api.return_value = {
            'ReplicationTask': {'ReplicationTaskIdentifier': 'test-task'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {'identifier': 'test-task'}

            result = manager.delete_task('arn:aws:dms:us-east-1:123:task:test')

            assert result['success'] is True
            assert result['data']['message'] == 'Replication task deleted successfully'
            mock_client.call_api.assert_called_once_with(
                'delete_replication_task',
                ReplicationTaskArn='arn:aws:dms:us-east-1:123:task:test',
            )


class TestTaskManagerCreateTask:
    """Test task creation functionality."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    @pytest.fixture
    def valid_params(self):
        """Create valid task creation parameters."""
        return {
            'ReplicationTaskIdentifier': 'test-task',
            'SourceEndpointArn': 'arn:aws:dms:us-east-1:123:endpoint:source',
            'TargetEndpointArn': 'arn:aws:dms:us-east-1:123:endpoint:target',
            'ReplicationInstanceArn': 'arn:aws:dms:us-east-1:123:rep:instance',
            'MigrationType': 'full-load',
            'TableMappings': json.dumps(
                {
                    'rules': [
                        {
                            'rule-type': 'selection',
                            'rule-id': '1',
                            'rule-action': 'include',
                            'object-locator': {'schema-name': 'public', 'table-name': '%'},
                        }
                    ]
                }
            ),
        }

    def test_create_task_success(self, manager, mock_client, valid_params):
        """Test successful task creation."""
        mock_client.call_api.return_value = {
            'ReplicationTask': {'ReplicationTaskIdentifier': 'test-task'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {'identifier': 'test-task'}

            result = manager.create_task(valid_params)

            assert result['success'] is True
            assert 'task' in result['data']
            assert result['data']['message'] == 'Replication task created successfully'

    def test_create_task_missing_required_param(self, manager, mock_client):
        """Test task creation with missing required parameter."""
        params = {
            'ReplicationTaskIdentifier': 'test-task',
            # Missing other required parameters
        }

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_task(params)

        assert 'Missing required parameter' in str(exc_info.value)

    def test_create_task_invalid_table_mappings(self, manager, mock_client):
        """Test task creation with invalid table mappings."""
        params = {
            'ReplicationTaskIdentifier': 'test-task',
            'SourceEndpointArn': 'arn:source',
            'TargetEndpointArn': 'arn:target',
            'ReplicationInstanceArn': 'arn:instance',
            'MigrationType': 'full-load',
            'TableMappings': 'invalid-json',
        }

        with pytest.raises(DMSValidationException) as exc_info:
            manager.create_task(params)

        assert 'Invalid table mappings' in str(exc_info.value)

    def test_create_task_invalid_migration_type(self, manager, mock_client, valid_params):
        """Test task creation with invalid migration type."""
        valid_params['MigrationType'] = 'invalid-type'

        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.create_task(valid_params)

        assert 'Invalid migration type' in str(exc_info.value)

    def test_create_task_valid_migration_types(self, manager, mock_client, valid_params):
        """Test task creation with all valid migration types."""
        mock_client.call_api.return_value = {'ReplicationTask': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {}

            valid_types = ['full-load', 'cdc', 'full-load-and-cdc']

            for migration_type in valid_types:
                valid_params['MigrationType'] = migration_type
                result = manager.create_task(valid_params)
                assert result['success'] is True
                mock_client.call_api.reset_mock()


class TestTaskManagerStartStopTask:
    """Test task start and stop operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    def test_start_task_success(self, manager, mock_client):
        """Test successful task start."""
        mock_client.call_api.return_value = {
            'ReplicationTask': {'ReplicationTaskIdentifier': 'test-task'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {'identifier': 'test-task'}

            result = manager.start_task('arn:aws:dms:us-east-1:123:task:test', 'start-replication')

            assert result['success'] is True
            assert (
                'Replication task started with type: start-replication'
                in result['data']['message']
            )

    def test_start_task_with_cdc_position(self, manager, mock_client):
        """Test starting task with CDC start position."""
        mock_client.call_api.return_value = {'ReplicationTask': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {}

            result = manager.start_task(
                'arn:aws:dms:us-east-1:123:task:test',
                'resume-processing',
                cdc_start_position='mysql-bin.000001:1234',
            )

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['CdcStartPosition'] == 'mysql-bin.000001:1234'

    def test_start_task_invalid_start_type(self, manager, mock_client):
        """Test starting task with invalid start type."""
        with pytest.raises(DMSInvalidParameterException) as exc_info:
            manager.start_task('arn:aws:dms:us-east-1:123:task:test', 'invalid-type')

        assert 'Invalid start type' in str(exc_info.value)

    def test_start_task_valid_start_types(self, manager, mock_client):
        """Test starting task with all valid start types."""
        mock_client.call_api.return_value = {'ReplicationTask': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {}

            valid_types = ['start-replication', 'resume-processing', 'reload-target']

            for start_type in valid_types:
                result = manager.start_task('arn:test', start_type)
                assert result['success'] is True
                mock_client.call_api.reset_mock()

    def test_stop_task_success(self, manager, mock_client):
        """Test successful task stop."""
        mock_client.call_api.return_value = {
            'ReplicationTask': {'ReplicationTaskIdentifier': 'test-task'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {'identifier': 'test-task'}

            result = manager.stop_task('arn:aws:dms:us-east-1:123:task:test')

            assert result['success'] is True
            assert result['data']['message'] == 'Replication task stop initiated'
            mock_client.call_api.assert_called_once_with(
                'stop_replication_task',
                ReplicationTaskArn='arn:aws:dms:us-east-1:123:task:test',
            )


class TestTaskManagerModifyTask:
    """Test task modification functionality."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    def test_modify_task_success(self, manager, mock_client):
        """Test successful task modification."""
        mock_client.call_api.return_value = {'ReplicationTask': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {}

            params = {
                'ReplicationTaskArn': 'arn:aws:dms:us-east-1:123:task:test',
                'ReplicationTaskIdentifier': 'new-identifier',
            }
            result = manager.modify_task(params)

            assert result['success'] is True
            assert result['data']['message'] == 'Replication task modified successfully'

    def test_modify_task_with_valid_table_mappings(self, manager, mock_client):
        """Test modifying task with valid table mappings."""
        mock_client.call_api.return_value = {'ReplicationTask': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {}

            params = {
                'ReplicationTaskArn': 'arn:test',
                'TableMappings': json.dumps(
                    {
                        'rules': [
                            {
                                'rule-type': 'selection',
                                'rule-id': '1',
                                'rule-action': 'include',
                                'object-locator': {'schema-name': 'public', 'table-name': '%'},
                            }
                        ]
                    }
                ),
            }
            result = manager.modify_task(params)

            assert result['success'] is True

    def test_modify_task_with_invalid_table_mappings(self, manager, mock_client):
        """Test modifying task with invalid table mappings."""
        params = {
            'ReplicationTaskArn': 'arn:test',
            'TableMappings': 'invalid-json',
        }

        with pytest.raises(DMSValidationException) as exc_info:
            manager.modify_task(params)

        assert 'Invalid table mappings' in str(exc_info.value)


class TestTaskManagerMoveTask:
    """Test task move functionality."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    def test_move_task_success(self, manager, mock_client):
        """Test successful task move."""
        mock_client.call_api.return_value = {
            'ReplicationTask': {'ReplicationTaskIdentifier': 'test-task'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {'identifier': 'test-task'}

            result = manager.move_task(
                'arn:aws:dms:us-east-1:123:task:test',
                'arn:aws:dms:us-east-1:123:rep:new-instance',
            )

            assert result['success'] is True
            assert result['data']['message'] == 'Replication task moved successfully'
            mock_client.call_api.assert_called_once_with(
                'move_replication_task',
                ReplicationTaskArn='arn:aws:dms:us-east-1:123:task:test',
                TargetReplicationInstanceArn='arn:aws:dms:us-east-1:123:rep:new-instance',
            )


class TestTaskManagerTableMappingsValidation:
    """Test table mappings validation."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    def test_validate_table_mappings_valid(self, manager):
        """Test validation of valid table mappings."""
        mappings = json.dumps(
            {
                'rules': [
                    {
                        'rule-type': 'selection',
                        'rule-id': '1',
                        'rule-action': 'include',
                        'object-locator': {'schema-name': 'public', 'table-name': '%'},
                    }
                ]
            }
        )

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is True
        assert error == ''

    def test_validate_table_mappings_invalid_json(self, manager):
        """Test validation with invalid JSON."""
        mappings = 'invalid-json'

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert 'Invalid JSON' in error

    def test_validate_table_mappings_missing_rules(self, manager):
        """Test validation with missing rules key."""
        mappings = json.dumps({'no-rules-key': []})

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert "Missing required key: 'rules'" in error

    def test_validate_table_mappings_rules_not_array(self, manager):
        """Test validation with rules not being an array."""
        mappings = json.dumps({'rules': 'not-an-array'})

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert "'rules' must be an array" in error

    def test_validate_table_mappings_empty_rules(self, manager):
        """Test validation with empty rules array."""
        mappings = json.dumps({'rules': []})

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert 'At least one rule is required' in error

    def test_validate_table_mappings_rule_not_object(self, manager):
        """Test validation with rule not being an object."""
        mappings = json.dumps({'rules': ['not-an-object']})

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert 'Rule 0 must be an object' in error

    def test_validate_table_mappings_missing_rule_type(self, manager):
        """Test validation with missing rule-type."""
        mappings = json.dumps({'rules': [{'no-rule-type': 'value'}]})

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert "Rule 0 missing 'rule-type'" in error

    def test_validate_table_mappings_invalid_rule_type(self, manager):
        """Test validation with invalid rule-type."""
        mappings = json.dumps({'rules': [{'rule-type': 'invalid-type'}]})

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert 'Rule 0 has invalid rule-type' in error

    def test_validate_table_mappings_valid_rule_types(self, manager):
        """Test validation with all valid rule types."""
        valid_rule_types = ['selection', 'transformation', 'table-settings']

        for rule_type in valid_rule_types:
            mappings = json.dumps(
                {
                    'rules': [
                        {
                            'rule-type': rule_type,
                            'rule-id': '1',
                            'rule-action': 'include',
                            'object-locator': {'schema-name': 'public'},
                        }
                    ]
                }
            )
            is_valid, error = manager.validate_table_mappings(mappings)
            assert is_valid is True

    def test_validate_table_mappings_selection_missing_rule_id(self, manager):
        """Test validation of selection rule missing rule-id."""
        mappings = json.dumps(
            {
                'rules': [
                    {
                        'rule-type': 'selection',
                        'rule-action': 'include',
                        'object-locator': {},
                    }
                ]
            }
        )

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert "Selection rule 0 missing 'rule-id'" in error

    def test_validate_table_mappings_selection_missing_rule_action(self, manager):
        """Test validation of selection rule missing rule-action."""
        mappings = json.dumps(
            {'rules': [{'rule-type': 'selection', 'rule-id': '1', 'object-locator': {}}]}
        )

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert "Selection rule 0 missing 'rule-action'" in error

    def test_validate_table_mappings_selection_invalid_action(self, manager):
        """Test validation of selection rule with invalid action."""
        mappings = json.dumps(
            {
                'rules': [
                    {
                        'rule-type': 'selection',
                        'rule-id': '1',
                        'rule-action': 'invalid-action',
                        'object-locator': {},
                    }
                ]
            }
        )

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert 'Selection rule 0 has invalid rule-action' in error

    def test_validate_table_mappings_selection_missing_object_locator(self, manager):
        """Test validation of selection rule missing object-locator."""
        mappings = json.dumps(
            {'rules': [{'rule-type': 'selection', 'rule-id': '1', 'rule-action': 'include'}]}
        )

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is False
        assert "Selection rule 0 missing 'object-locator'" in error

    def test_validate_table_mappings_valid_selection_actions(self, manager):
        """Test validation with all valid selection actions."""
        valid_actions = ['include', 'exclude', 'explicit']

        for action in valid_actions:
            mappings = json.dumps(
                {
                    'rules': [
                        {
                            'rule-type': 'selection',
                            'rule-id': '1',
                            'rule-action': action,
                            'object-locator': {'schema-name': 'public'},
                        }
                    ]
                }
            )
            is_valid, error = manager.validate_table_mappings(mappings)
            assert is_valid is True

    def test_validate_table_mappings_multiple_rules(self, manager):
        """Test validation with multiple rules."""
        mappings = json.dumps(
            {
                'rules': [
                    {
                        'rule-type': 'selection',
                        'rule-id': '1',
                        'rule-action': 'include',
                        'object-locator': {'schema-name': 'public'},
                    },
                    {
                        'rule-type': 'selection',
                        'rule-id': '2',
                        'rule-action': 'exclude',
                        'object-locator': {'schema-name': 'private'},
                    },
                ]
            }
        )

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is True

    def test_validate_table_mappings_transformation_rule(self, manager):
        """Test validation with transformation rule."""
        mappings = json.dumps({'rules': [{'rule-type': 'transformation', 'rule-id': '1'}]})

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is True


class TestTaskManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    def test_list_tasks_api_error(self, manager, mock_client):
        """Test API error during task listing."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.list_tasks()

        assert 'API Error' in str(exc_info.value)

    def test_start_task_api_error(self, manager, mock_client):
        """Test API error during task start."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.start_task('arn:test', 'start-replication')

        assert 'Network error' in str(exc_info.value)

    def test_delete_task_api_error(self, manager, mock_client):
        """Test API error during task deletion."""
        mock_client.call_api.side_effect = Exception('Service error')

        with pytest.raises(Exception) as exc_info:
            manager.delete_task('arn:test')

        assert 'Service error' in str(exc_info.value)


class TestTaskManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create TaskManager instance."""
        return TaskManager(mock_client)

    def test_list_tasks_empty_result(self, manager, mock_client):
        """Test listing tasks with empty result."""
        mock_client.call_api.return_value = {'ReplicationTasks': []}

        with patch('awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'):
            result = manager.list_tasks()

            assert result['success'] is True
            assert result['data']['count'] == 0
            assert result['data']['tasks'] == []

    def test_list_tasks_max_results_boundary(self, manager, mock_client):
        """Test listing tasks with maximum results."""
        mock_client.call_api.return_value = {'ReplicationTasks': []}

        with patch('awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'):
            result = manager.list_tasks(max_results=1000)

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert call_args['MaxRecords'] == 1000

    def test_validate_table_mappings_complex_valid(self, manager):
        """Test validation with complex valid table mappings."""
        mappings = json.dumps(
            {
                'rules': [
                    {
                        'rule-type': 'selection',
                        'rule-id': '1',
                        'rule-action': 'include',
                        'object-locator': {
                            'schema-name': 'public',
                            'table-name': 'users',
                        },
                    },
                    {
                        'rule-type': 'selection',
                        'rule-id': '2',
                        'rule-action': 'exclude',
                        'object-locator': {
                            'schema-name': 'private',
                            'table-name': '%',
                        },
                    },
                    {
                        'rule-type': 'transformation',
                        'rule-id': '3',
                        'rule-action': 'rename',
                        'rule-target': 'table',
                        'object-locator': {
                            'schema-name': 'public',
                            'table-name': 'old_table',
                        },
                        'value': 'new_table',
                    },
                ]
            }
        )

        is_valid, error = manager.validate_table_mappings(mappings)

        assert is_valid is True

    def test_start_task_without_cdc_position(self, manager, mock_client):
        """Test starting task without CDC position."""
        mock_client.call_api.return_value = {'ReplicationTask': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {}

            result = manager.start_task('arn:test', 'start-replication')

            assert result['success'] is True
            call_args = mock_client.call_api.call_args[1]
            assert 'CdcStartPosition' not in call_args

    def test_modify_task_without_table_mappings(self, manager, mock_client):
        """Test modifying task without changing table mappings."""
        mock_client.call_api.return_value = {'ReplicationTask': {}}

        with patch(
            'awslabs.aws_dms_mcp_server.utils.task_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {}

            params = {'ReplicationTaskArn': 'arn:test'}
            result = manager.modify_task(params)

            assert result['success'] is True
