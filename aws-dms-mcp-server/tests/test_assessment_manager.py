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

"""Comprehensive tests for AssessmentManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.assessment_manager import AssessmentManager
from unittest.mock import Mock, patch


class TestAssessmentManagerStartAssessment:
    """Test assessment start operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_start_assessment_success(self, manager, mock_client):
        """Test successful assessment start (legacy API)."""
        mock_client.call_api.return_value = {
            'ReplicationTask': {'ReplicationTaskIdentifier': 'test-task'}
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.assessment_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {'identifier': 'test-task'}

            result = manager.start_assessment('arn:test:task')

            assert result['success'] is True
            assert result['data']['message'] == 'Task assessment started'
            assert 'task' in result['data']
            mock_client.call_api.assert_called_once_with(
                'start_replication_task_assessment', ReplicationTaskArn='arn:test:task'
            )

    def test_start_assessment_with_formatter(self, manager, mock_client):
        """Test assessment start with response formatter."""
        mock_client.call_api.return_value = {
            'ReplicationTask': {
                'ReplicationTaskIdentifier': 'task-1',
                'Status': 'testing',
            }
        }

        with patch(
            'awslabs.aws_dms_mcp_server.utils.assessment_manager.ResponseFormatter'
        ) as mock_formatter:
            mock_formatter.format_task.return_value = {'identifier': 'task-1', 'status': 'testing'}

            result = manager.start_assessment('arn:test')

            assert result['success'] is True
            mock_formatter.format_task.assert_called_once()


class TestAssessmentManagerStartAssessmentRun:
    """Test assessment run start operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_start_assessment_run_success(self, manager, mock_client):
        """Test successful assessment run start."""
        mock_client.call_api.return_value = {
            'ReplicationTaskAssessmentRun': {'AssessmentRunId': 'run-1'}
        }

        result = manager.start_assessment_run(
            'arn:test:task',
            'arn:iam:role',
            's3-bucket',
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Assessment run started successfully'
        assert 'assessment_run' in result['data']

    def test_start_assessment_run_with_all_params(self, manager, mock_client):
        """Test assessment run start with all parameters."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentRun': {}}

        result = manager.start_assessment_run(
            'arn:test:task',
            'arn:iam:role',
            's3-bucket',
            result_location_folder='results/',
            result_encryption_mode='sse-kms',
            result_kms_key_arn='arn:kms:key',
            assessment_run_name='test-run',
            include_only=['check-1', 'check-2'],
            exclude=['check-3'],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ResultLocationFolder'] == 'results/'
        assert call_args['ResultEncryptionMode'] == 'sse-kms'
        assert call_args['ResultKmsKeyArn'] == 'arn:kms:key'
        assert call_args['AssessmentRunName'] == 'test-run'
        assert call_args['IncludeOnly'] == ['check-1', 'check-2']
        assert call_args['Exclude'] == ['check-3']

    def test_start_assessment_run_minimal_params(self, manager, mock_client):
        """Test assessment run start with minimal parameters."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentRun': {}}

        result = manager.start_assessment_run('arn:task', 'arn:role', 'bucket')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationTaskArn'] == 'arn:task'
        assert call_args['ServiceAccessRoleArn'] == 'arn:role'
        assert call_args['ResultLocationBucket'] == 'bucket'
        assert 'ResultLocationFolder' not in call_args

    def test_start_assessment_run_with_encryption(self, manager, mock_client):
        """Test assessment run start with encryption."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentRun': {}}

        result = manager.start_assessment_run(
            'arn:task',
            'arn:role',
            'bucket',
            result_encryption_mode='sse-kms',
            result_kms_key_arn='arn:kms:key',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ResultEncryptionMode'] == 'sse-kms'
        assert call_args['ResultKmsKeyArn'] == 'arn:kms:key'


class TestAssessmentManagerCancelAssessmentRun:
    """Test assessment run cancellation."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_cancel_assessment_run_success(self, manager, mock_client):
        """Test successful assessment run cancellation."""
        mock_client.call_api.return_value = {
            'ReplicationTaskAssessmentRun': {'AssessmentRunId': 'run-1', 'Status': 'cancelling'}
        }

        result = manager.cancel_assessment_run('arn:assessment:run')

        assert result['success'] is True
        assert result['data']['message'] == 'Assessment run cancelled'
        mock_client.call_api.assert_called_once_with(
            'cancel_replication_task_assessment_run',
            ReplicationTaskAssessmentRunArn='arn:assessment:run',
        )


class TestAssessmentManagerDeleteAssessmentRun:
    """Test assessment run deletion."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_delete_assessment_run_success(self, manager, mock_client):
        """Test successful assessment run deletion."""
        mock_client.call_api.return_value = {
            'ReplicationTaskAssessmentRun': {'AssessmentRunId': 'run-1'}
        }

        result = manager.delete_assessment_run('arn:assessment:run')

        assert result['success'] is True
        assert result['data']['message'] == 'Assessment run deleted successfully'
        mock_client.call_api.assert_called_once_with(
            'delete_replication_task_assessment_run',
            ReplicationTaskAssessmentRunArn='arn:assessment:run',
        )


class TestAssessmentManagerListAssessmentResults:
    """Test assessment results listing (legacy API)."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_list_assessment_results_success(self, manager, mock_client):
        """Test successful assessment results listing."""
        mock_client.call_api.return_value = {
            'ReplicationTaskAssessmentResults': [
                {'ResultId': 'result-1'},
                {'ResultId': 'result-2'},
            ]
        }

        result = manager.list_assessment_results()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['assessment_results']) == 2

    def test_list_assessment_results_with_task_arn(self, manager, mock_client):
        """Test assessment results listing filtered by task ARN."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentResults': []}

        result = manager.list_assessment_results(task_arn='arn:test:task')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationTaskArn'] == 'arn:test:task'

    def test_list_assessment_results_with_pagination(self, manager, mock_client):
        """Test assessment results listing with pagination."""
        mock_client.call_api.return_value = {
            'ReplicationTaskAssessmentResults': [],
            'Marker': 'next-token',
        }

        result = manager.list_assessment_results(max_results=50, marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_list_assessment_results_empty(self, manager, mock_client):
        """Test assessment results listing with empty result."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentResults': []}

        result = manager.list_assessment_results()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['assessment_results'] == []


class TestAssessmentManagerListAssessmentRuns:
    """Test assessment runs listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_list_assessment_runs_success(self, manager, mock_client):
        """Test successful assessment runs listing."""
        mock_client.call_api.return_value = {
            'ReplicationTaskAssessmentRuns': [
                {'AssessmentRunId': 'run-1'},
                {'AssessmentRunId': 'run-2'},
            ]
        }

        result = manager.list_assessment_runs()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['assessment_runs']) == 2

    def test_list_assessment_runs_with_filters(self, manager, mock_client):
        """Test assessment runs listing with filters."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentRuns': []}

        filters = [{'Name': 'status', 'Values': ['completed']}]
        result = manager.list_assessment_runs(filters=filters, max_results=25)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 25

    def test_list_assessment_runs_with_pagination(self, manager, mock_client):
        """Test assessment runs listing with pagination."""
        mock_client.call_api.return_value = {
            'ReplicationTaskAssessmentRuns': [],
            'Marker': 'next-token',
        }

        result = manager.list_assessment_runs(marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'


class TestAssessmentManagerListIndividualAssessments:
    """Test individual assessments listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_list_individual_assessments_success(self, manager, mock_client):
        """Test successful individual assessments listing."""
        mock_client.call_api.return_value = {
            'ReplicationTaskIndividualAssessments': [
                {'AssessmentId': 'assessment-1'},
                {'AssessmentId': 'assessment-2'},
            ]
        }

        result = manager.list_individual_assessments()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['individual_assessments']) == 2

    def test_list_individual_assessments_with_filters(self, manager, mock_client):
        """Test individual assessments listing with filters."""
        mock_client.call_api.return_value = {'ReplicationTaskIndividualAssessments': []}

        filters = [{'Name': 'assessment-name', 'Values': ['test-assessment']}]
        result = manager.list_individual_assessments(filters=filters, max_results=10)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 10

    def test_list_individual_assessments_with_pagination(self, manager, mock_client):
        """Test individual assessments listing with pagination."""
        mock_client.call_api.return_value = {
            'ReplicationTaskIndividualAssessments': [],
            'Marker': 'next-token',
        }

        result = manager.list_individual_assessments(marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']


class TestAssessmentManagerListApplicableAssessments:
    """Test applicable assessments listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_list_applicable_assessments_success(self, manager, mock_client):
        """Test successful applicable assessments listing."""
        mock_client.call_api.return_value = {
            'IndividualAssessmentNames': ['assessment-1', 'assessment-2', 'assessment-3']
        }

        result = manager.list_applicable_assessments()

        assert result['success'] is True
        assert result['data']['count'] == 3
        assert len(result['data']['applicable_assessments']) == 3

    def test_list_applicable_assessments_with_all_params(self, manager, mock_client):
        """Test applicable assessments listing with all parameters."""
        mock_client.call_api.return_value = {'IndividualAssessmentNames': []}

        result = manager.list_applicable_assessments(
            task_arn='arn:task',
            migration_type='full-load',
            source_engine_name='mysql',
            target_engine_name='postgres',
            replication_instance_arn='arn:instance',
            max_results=50,
            marker='token',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ReplicationTaskArn'] == 'arn:task'
        assert call_args['MigrationType'] == 'full-load'
        assert call_args['SourceEngineName'] == 'mysql'
        assert call_args['TargetEngineName'] == 'postgres'
        assert call_args['ReplicationInstanceArn'] == 'arn:instance'

    def test_list_applicable_assessments_minimal_params(self, manager, mock_client):
        """Test applicable assessments listing with minimal parameters."""
        mock_client.call_api.return_value = {'IndividualAssessmentNames': []}

        result = manager.list_applicable_assessments()

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'ReplicationTaskArn' not in call_args
        assert 'MigrationType' not in call_args

    def test_list_applicable_assessments_with_pagination(self, manager, mock_client):
        """Test applicable assessments listing with pagination."""
        mock_client.call_api.return_value = {
            'IndividualAssessmentNames': [],
            'Marker': 'next-token',
        }

        result = manager.list_applicable_assessments(marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']

    def test_list_applicable_assessments_by_engines(self, manager, mock_client):
        """Test applicable assessments listing filtered by engines."""
        mock_client.call_api.return_value = {'IndividualAssessmentNames': []}

        result = manager.list_applicable_assessments(
            source_engine_name='oracle', target_engine_name='postgres'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SourceEngineName'] == 'oracle'
        assert call_args['TargetEngineName'] == 'postgres'


class TestAssessmentManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_start_assessment_api_error(self, manager, mock_client):
        """Test API error during assessment start."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.start_assessment('arn:task')

        assert 'API Error' in str(exc_info.value)

    def test_start_assessment_run_api_error(self, manager, mock_client):
        """Test API error during assessment run start."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.start_assessment_run('arn:task', 'arn:role', 'bucket')

        assert 'Network error' in str(exc_info.value)

    def test_cancel_assessment_run_api_error(self, manager, mock_client):
        """Test API error during assessment run cancellation."""
        mock_client.call_api.side_effect = Exception('Service error')

        with pytest.raises(Exception) as exc_info:
            manager.cancel_assessment_run('arn:run')

        assert 'Service error' in str(exc_info.value)

    def test_delete_assessment_run_api_error(self, manager, mock_client):
        """Test API error during assessment run deletion."""
        mock_client.call_api.side_effect = Exception('Timeout error')

        with pytest.raises(Exception) as exc_info:
            manager.delete_assessment_run('arn:run')

        assert 'Timeout error' in str(exc_info.value)


class TestAssessmentManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create AssessmentManager instance."""
        return AssessmentManager(mock_client)

    def test_list_assessment_results_max_results_boundary(self, manager, mock_client):
        """Test assessment results listing with maximum results."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentResults': []}

        result = manager.list_assessment_results(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_start_assessment_run_with_multiple_includes(self, manager, mock_client):
        """Test assessment run start with multiple include checks."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentRun': {}}

        include_checks = ['check-1', 'check-2', 'check-3', 'check-4', 'check-5']
        result = manager.start_assessment_run(
            'arn:task', 'arn:role', 'bucket', include_only=include_checks
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['IncludeOnly'] == include_checks
        assert len(call_args['IncludeOnly']) == 5

    def test_start_assessment_run_with_multiple_excludes(self, manager, mock_client):
        """Test assessment run start with multiple exclude checks."""
        mock_client.call_api.return_value = {'ReplicationTaskAssessmentRun': {}}

        exclude_checks = ['check-a', 'check-b', 'check-c']
        result = manager.start_assessment_run(
            'arn:task', 'arn:role', 'bucket', exclude=exclude_checks
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Exclude'] == exclude_checks

    def test_list_individual_assessments_empty_filters(self, manager, mock_client):
        """Test individual assessments listing with empty filters list."""
        mock_client.call_api.return_value = {'ReplicationTaskIndividualAssessments': []}

        result = manager.list_individual_assessments(filters=[])

        assert result['success'] is True
        # Empty filters list is not passed to API (falsy value)
        call_args = mock_client.call_api.call_args[1]
        assert 'Filters' not in call_args
