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

"""Comprehensive tests for FleetAdvisorManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.fleet_advisor_manager import FleetAdvisorManager
from unittest.mock import Mock


class TestFleetAdvisorManagerCollectorOperations:
    """Test Fleet Advisor collector operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create FleetAdvisorManager instance."""
        return FleetAdvisorManager(mock_client)

    def test_create_collector_success(self, manager, mock_client):
        """Test successful collector creation."""
        mock_client.call_api.return_value = {
            'Collector': {
                'CollectorReferencedId': 'collector-123',
                'CollectorName': 'test-collector',
            }
        }

        result = manager.create_collector(
            name='test-collector',
            description='Test collector',
            service_access_role_arn='arn:aws:iam::123:role/test',
            s3_bucket_name='test-bucket',
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Fleet Advisor collector created'
        assert 'collector' in result['data']
        mock_client.call_api.assert_called_once_with(
            'create_fleet_advisor_collector',
            CollectorName='test-collector',
            Description='Test collector',
            ServiceAccessRoleArn='arn:aws:iam::123:role/test',
            S3BucketName='test-bucket',
        )

    def test_delete_collector_success(self, manager, mock_client):
        """Test successful collector deletion."""
        mock_client.call_api.return_value = {}

        result = manager.delete_collector('collector-123')

        assert result['success'] is True
        assert result['data']['message'] == 'Fleet Advisor collector deleted'
        mock_client.call_api.assert_called_once_with(
            'delete_fleet_advisor_collector', CollectorReferencedId='collector-123'
        )

    def test_list_collectors_success(self, manager, mock_client):
        """Test successful collector listing."""
        mock_client.call_api.return_value = {
            'Collectors': [
                {'CollectorReferencedId': 'collector-1', 'CollectorName': 'collector-1'},
                {'CollectorReferencedId': 'collector-2', 'CollectorName': 'collector-2'},
            ]
        }

        result = manager.list_collectors()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'collectors' in result['data']

    def test_list_collectors_with_filters(self, manager, mock_client):
        """Test listing collectors with filters."""
        mock_client.call_api.return_value = {'Collectors': []}

        filters = [{'Name': 'collector-name', 'Values': ['test-collector']}]
        result = manager.list_collectors(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['NextToken'] == 'token'

    def test_list_collectors_with_pagination(self, manager, mock_client):
        """Test listing collectors with pagination."""
        mock_client.call_api.return_value = {'Collectors': [], 'NextToken': 'next-token'}

        result = manager.list_collectors()

        assert result['success'] is True
        assert 'next_token' in result['data']
        assert result['data']['next_token'] == 'next-token'

    def test_list_collectors_empty(self, manager, mock_client):
        """Test listing collectors with empty result."""
        mock_client.call_api.return_value = {'Collectors': []}

        result = manager.list_collectors()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['collectors'] == []


class TestFleetAdvisorManagerDatabaseOperations:
    """Test Fleet Advisor database operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create FleetAdvisorManager instance."""
        return FleetAdvisorManager(mock_client)

    def test_delete_databases_success(self, manager, mock_client):
        """Test successful database deletion."""
        mock_client.call_api.return_value = {'DatabaseIds': ['db-1', 'db-2']}

        result = manager.delete_databases(['db-1', 'db-2'])

        assert result['success'] is True
        assert result['data']['message'] == 'Fleet Advisor databases deleted'
        assert result['data']['database_ids'] == ['db-1', 'db-2']
        mock_client.call_api.assert_called_once_with(
            'delete_fleet_advisor_databases', DatabaseIds=['db-1', 'db-2']
        )

    def test_delete_databases_single(self, manager, mock_client):
        """Test deleting single database."""
        mock_client.call_api.return_value = {'DatabaseIds': ['db-1']}

        result = manager.delete_databases(['db-1'])

        assert result['success'] is True
        assert len(result['data']['database_ids']) == 1

    def test_list_databases_success(self, manager, mock_client):
        """Test successful database listing."""
        mock_client.call_api.return_value = {
            'Databases': [
                {'DatabaseId': 'db-1', 'DatabaseName': 'database-1'},
                {'DatabaseId': 'db-2', 'DatabaseName': 'database-2'},
                {'DatabaseId': 'db-3', 'DatabaseName': 'database-3'},
            ]
        }

        result = manager.list_databases()

        assert result['success'] is True
        assert result['data']['count'] == 3
        assert 'databases' in result['data']

    def test_list_databases_with_filters(self, manager, mock_client):
        """Test listing databases with filters."""
        mock_client.call_api.return_value = {'Databases': []}

        filters = [{'Name': 'database-engine', 'Values': ['mysql']}]
        result = manager.list_databases(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['NextToken'] == 'token'

    def test_list_databases_with_pagination(self, manager, mock_client):
        """Test listing databases with pagination."""
        mock_client.call_api.return_value = {'Databases': [], 'NextToken': 'next-token'}

        result = manager.list_databases()

        assert result['success'] is True
        assert 'next_token' in result['data']
        assert result['data']['next_token'] == 'next-token'

    def test_list_databases_empty(self, manager, mock_client):
        """Test listing databases with empty result."""
        mock_client.call_api.return_value = {'Databases': []}

        result = manager.list_databases()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['databases'] == []


class TestFleetAdvisorManagerLSAAnalysis:
    """Test Fleet Advisor LSA analysis operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create FleetAdvisorManager instance."""
        return FleetAdvisorManager(mock_client)

    def test_describe_lsa_analysis_success(self, manager, mock_client):
        """Test successful LSA analysis description."""
        mock_client.call_api.return_value = {
            'Analysis': [
                {'AnalysisId': 'analysis-1', 'Status': 'completed'},
                {'AnalysisId': 'analysis-2', 'Status': 'completed'},
            ]
        }

        result = manager.describe_lsa_analysis()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'lsa_analysis' in result['data']

    def test_describe_lsa_analysis_with_pagination(self, manager, mock_client):
        """Test LSA analysis with pagination."""
        mock_client.call_api.return_value = {'Analysis': [], 'NextToken': 'next-token'}

        result = manager.describe_lsa_analysis(max_results=50, marker='token')

        assert result['success'] is True
        assert 'next_token' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 50
        assert call_args['NextToken'] == 'token'

    def test_describe_lsa_analysis_empty(self, manager, mock_client):
        """Test LSA analysis with empty result."""
        mock_client.call_api.return_value = {'Analysis': []}

        result = manager.describe_lsa_analysis()

        assert result['success'] is True
        assert result['data']['count'] == 0

    def test_run_lsa_analysis_success(self, manager, mock_client):
        """Test successful LSA analysis run."""
        mock_client.call_api.return_value = {
            'LSAAnalysisRun': {'AnalysisId': 'analysis-123', 'Status': 'running'}
        }

        result = manager.run_lsa_analysis()

        assert result['success'] is True
        assert result['data']['message'] == 'Fleet Advisor LSA analysis started'
        assert 'lsa_analysis_run' in result['data']
        mock_client.call_api.assert_called_once_with('run_fleet_advisor_lsa_analysis')


class TestFleetAdvisorManagerSchemaOperations:
    """Test Fleet Advisor schema operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create FleetAdvisorManager instance."""
        return FleetAdvisorManager(mock_client)

    def test_describe_schema_object_summary_success(self, manager, mock_client):
        """Test successful schema object summary description."""
        mock_client.call_api.return_value = {
            'FleetAdvisorSchemaObjects': [
                {'ObjectType': 'TABLE', 'NumberOfObjects': 10},
                {'ObjectType': 'VIEW', 'NumberOfObjects': 5},
            ]
        }

        result = manager.describe_schema_object_summary()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'schema_objects' in result['data']

    def test_describe_schema_object_summary_with_filters(self, manager, mock_client):
        """Test schema object summary with filters."""
        mock_client.call_api.return_value = {'FleetAdvisorSchemaObjects': []}

        filters = [{'Name': 'object-type', 'Values': ['TABLE']}]
        result = manager.describe_schema_object_summary(
            filters=filters, max_results=50, marker='token'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['NextToken'] == 'token'

    def test_describe_schema_object_summary_with_pagination(self, manager, mock_client):
        """Test schema object summary with pagination."""
        mock_client.call_api.return_value = {
            'FleetAdvisorSchemaObjects': [],
            'NextToken': 'next-token',
        }

        result = manager.describe_schema_object_summary()

        assert result['success'] is True
        assert 'next_token' in result['data']

    def test_list_schemas_success(self, manager, mock_client):
        """Test successful schema listing."""
        mock_client.call_api.return_value = {
            'FleetAdvisorSchemas': [
                {'SchemaId': 'schema-1', 'SchemaName': 'public'},
                {'SchemaId': 'schema-2', 'SchemaName': 'information_schema'},
            ]
        }

        result = manager.list_schemas()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'schemas' in result['data']

    def test_list_schemas_with_filters(self, manager, mock_client):
        """Test listing schemas with filters."""
        mock_client.call_api.return_value = {'FleetAdvisorSchemas': []}

        filters = [{'Name': 'schema-name', 'Values': ['public']}]
        result = manager.list_schemas(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['NextToken'] == 'token'

    def test_list_schemas_with_pagination(self, manager, mock_client):
        """Test listing schemas with pagination."""
        mock_client.call_api.return_value = {'FleetAdvisorSchemas': [], 'NextToken': 'next-token'}

        result = manager.list_schemas()

        assert result['success'] is True
        assert 'next_token' in result['data']

    def test_list_schemas_empty(self, manager, mock_client):
        """Test listing schemas with empty result."""
        mock_client.call_api.return_value = {'FleetAdvisorSchemas': []}

        result = manager.list_schemas()

        assert result['success'] is True
        assert result['data']['count'] == 0


class TestFleetAdvisorManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create FleetAdvisorManager instance."""
        return FleetAdvisorManager(mock_client)

    def test_create_collector_api_error(self, manager, mock_client):
        """Test API error during collector creation."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.create_collector('test', 'desc', 'arn', 'bucket')

        assert 'API Error' in str(exc_info.value)

    def test_delete_collector_api_error(self, manager, mock_client):
        """Test API error during collector deletion."""
        mock_client.call_api.side_effect = Exception('Delete failed')

        with pytest.raises(Exception) as exc_info:
            manager.delete_collector('collector-123')

        assert 'Delete failed' in str(exc_info.value)

    def test_list_collectors_api_error(self, manager, mock_client):
        """Test API error during collector listing."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.list_collectors()

        assert 'Network error' in str(exc_info.value)

    def test_delete_databases_api_error(self, manager, mock_client):
        """Test API error during database deletion."""
        mock_client.call_api.side_effect = Exception('Delete error')

        with pytest.raises(Exception) as exc_info:
            manager.delete_databases(['db-1'])

        assert 'Delete error' in str(exc_info.value)

    def test_list_databases_api_error(self, manager, mock_client):
        """Test API error during database listing."""
        mock_client.call_api.side_effect = Exception('List error')

        with pytest.raises(Exception) as exc_info:
            manager.list_databases()

        assert 'List error' in str(exc_info.value)

    def test_run_lsa_analysis_api_error(self, manager, mock_client):
        """Test API error during LSA analysis run."""
        mock_client.call_api.side_effect = Exception('Analysis error')

        with pytest.raises(Exception) as exc_info:
            manager.run_lsa_analysis()

        assert 'Analysis error' in str(exc_info.value)


class TestFleetAdvisorManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create FleetAdvisorManager instance."""
        return FleetAdvisorManager(mock_client)

    def test_delete_databases_empty_list(self, manager, mock_client):
        """Test deleting databases with empty list."""
        mock_client.call_api.return_value = {'DatabaseIds': []}

        result = manager.delete_databases([])

        assert result['success'] is True
        assert result['data']['database_ids'] == []

    def test_list_operations_with_max_results_boundary(self, manager, mock_client):
        """Test list operations with maximum results."""
        mock_client.call_api.return_value = {'Collectors': []}

        result = manager.list_collectors(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_list_collectors_without_optional_params(self, manager, mock_client):
        """Test listing collectors without optional parameters."""
        mock_client.call_api.return_value = {'Collectors': []}

        result = manager.list_collectors()

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 100
        assert 'Filters' not in call_args
        assert 'NextToken' not in call_args

    def test_describe_lsa_analysis_without_optional_params(self, manager, mock_client):
        """Test describing LSA analysis without optional parameters."""
        mock_client.call_api.return_value = {'Analysis': []}

        result = manager.describe_lsa_analysis()

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 100
        assert 'NextToken' not in call_args

    def test_list_schemas_multiple_pages(self, manager, mock_client):
        """Test listing schemas across multiple pages."""
        # First call returns data with token
        mock_client.call_api.return_value = {
            'FleetAdvisorSchemas': [{'SchemaId': 'schema-1'}],
            'NextToken': 'token-1',
        }

        result1 = manager.list_schemas()

        assert result1['success'] is True
        assert result1['data']['count'] == 1
        assert result1['data']['next_token'] == 'token-1'

        # Second call with token
        mock_client.call_api.return_value = {
            'FleetAdvisorSchemas': [{'SchemaId': 'schema-2'}]
            # No NextToken means last page
        }

        result2 = manager.list_schemas(marker='token-1')

        assert result2['success'] is True
        assert result2['data']['count'] == 1
        assert 'next_token' not in result2['data']

    def test_create_collector_with_all_params(self, manager, mock_client):
        """Test creating collector with all parameters."""
        mock_client.call_api.return_value = {'Collector': {'CollectorReferencedId': 'col-123'}}

        result = manager.create_collector(
            name='detailed-collector',
            description='A detailed test collector with all params',
            service_access_role_arn='arn:aws:iam::123456789012:role/DMS-FleetAdvisor-Role',
            s3_bucket_name='my-fleet-advisor-bucket',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CollectorName'] == 'detailed-collector'
        assert 'detailed test collector' in call_args['Description'].lower()
        assert call_args['ServiceAccessRoleArn'].startswith('arn:aws:iam::')
        assert call_args['S3BucketName'] == 'my-fleet-advisor-bucket'
