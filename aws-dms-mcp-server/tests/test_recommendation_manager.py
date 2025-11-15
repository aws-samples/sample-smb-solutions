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

"""Comprehensive tests for RecommendationManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.recommendation_manager import RecommendationManager
from unittest.mock import Mock


class TestRecommendationManagerListOperations:
    """Test recommendation listing operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create RecommendationManager instance."""
        return RecommendationManager(mock_client)

    def test_list_recommendations_success(self, manager, mock_client):
        """Test successful recommendations listing."""
        mock_client.call_api.return_value = {
            'Recommendations': [
                {'RecommendationId': 'rec-1', 'Type': 'instance-type'},
                {'RecommendationId': 'rec-2', 'Type': 'storage-optimization'},
            ]
        }

        result = manager.list_recommendations()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'recommendations' in result['data']

    def test_list_recommendations_with_filters(self, manager, mock_client):
        """Test listing recommendations with filters."""
        mock_client.call_api.return_value = {'Recommendations': []}

        filters = [{'Name': 'recommendation-type', 'Values': ['instance-type']}]
        result = manager.list_recommendations(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['NextToken'] == 'token'

    def test_list_recommendations_with_pagination(self, manager, mock_client):
        """Test listing recommendations with pagination."""
        mock_client.call_api.return_value = {'Recommendations': [], 'NextToken': 'next-token'}

        result = manager.list_recommendations()

        assert result['success'] is True
        assert 'next_token' in result['data']
        assert result['data']['next_token'] == 'next-token'

    def test_list_recommendations_empty(self, manager, mock_client):
        """Test listing recommendations with empty result."""
        mock_client.call_api.return_value = {'Recommendations': []}

        result = manager.list_recommendations()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['recommendations'] == []

    def test_list_recommendations_without_optional_params(self, manager, mock_client):
        """Test listing recommendations without optional parameters."""
        mock_client.call_api.return_value = {'Recommendations': []}

        result = manager.list_recommendations()

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 100
        assert 'Filters' not in call_args
        assert 'NextToken' not in call_args


class TestRecommendationManagerLimitations:
    """Test recommendation limitations operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create RecommendationManager instance."""
        return RecommendationManager(mock_client)

    def test_list_recommendation_limitations_success(self, manager, mock_client):
        """Test successful limitations listing."""
        mock_client.call_api.return_value = {
            'Limitations': [
                {'LimitationType': 'memory', 'MaxValue': '256GB'},
                {'LimitationType': 'storage', 'MaxValue': '10TB'},
            ]
        }

        result = manager.list_recommendation_limitations()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'limitations' in result['data']

    def test_list_recommendation_limitations_with_filters(self, manager, mock_client):
        """Test listing limitations with filters."""
        mock_client.call_api.return_value = {'Limitations': []}

        filters = [{'Name': 'limitation-type', 'Values': ['memory']}]
        result = manager.list_recommendation_limitations(
            filters=filters, max_results=50, marker='token'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['NextToken'] == 'token'

    def test_list_recommendation_limitations_with_pagination(self, manager, mock_client):
        """Test listing limitations with pagination."""
        mock_client.call_api.return_value = {'Limitations': [], 'NextToken': 'next-token'}

        result = manager.list_recommendation_limitations()

        assert result['success'] is True
        assert 'next_token' in result['data']
        assert result['data']['next_token'] == 'next-token'

    def test_list_recommendation_limitations_empty(self, manager, mock_client):
        """Test listing limitations with empty result."""
        mock_client.call_api.return_value = {'Limitations': []}

        result = manager.list_recommendation_limitations()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['limitations'] == []


class TestRecommendationManagerStartRecommendations:
    """Test starting recommendations operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create RecommendationManager instance."""
        return RecommendationManager(mock_client)

    def test_start_recommendations_success(self, manager, mock_client):
        """Test successful recommendations start."""
        mock_client.call_api.return_value = {}

        settings = {'AnalysisType': 'performance', 'IncludeHistoricalData': True}
        result = manager.start_recommendations('database-123', settings)

        assert result['success'] is True
        assert result['data']['message'] == 'Recommendations generation started'
        assert result['data']['database_id'] == 'database-123'
        mock_client.call_api.assert_called_once_with(
            'start_recommendations', DatabaseId='database-123', Settings=settings
        )

    def test_start_recommendations_minimal_settings(self, manager, mock_client):
        """Test starting recommendations with minimal settings."""
        mock_client.call_api.return_value = {}

        settings = {}
        result = manager.start_recommendations('database-456', settings)

        assert result['success'] is True
        assert result['data']['database_id'] == 'database-456'

    def test_start_recommendations_complex_settings(self, manager, mock_client):
        """Test starting recommendations with complex settings."""
        mock_client.call_api.return_value = {}

        settings = {
            'AnalysisType': 'comprehensive',
            'IncludeHistoricalData': True,
            'TimeRangeHours': 168,
            'Thresholds': {'CPUUtilization': 80, 'MemoryUtilization': 75},
        }
        result = manager.start_recommendations('database-789', settings)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Settings'] == settings


class TestRecommendationManagerBatchOperations:
    """Test batch recommendations operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create RecommendationManager instance."""
        return RecommendationManager(mock_client)

    def test_batch_start_recommendations_success(self, manager, mock_client):
        """Test successful batch recommendations start."""
        mock_client.call_api.return_value = {'ErrorEntries': []}

        data = [
            {'DatabaseId': 'db-1', 'Settings': {'AnalysisType': 'performance'}},
            {'DatabaseId': 'db-2', 'Settings': {'AnalysisType': 'cost'}},
        ]
        result = manager.batch_start_recommendations(data)

        assert result['success'] is True
        assert result['data']['error_entries'] == []
        assert 'Batch recommendations started' in result['data']['message']

    def test_batch_start_recommendations_with_errors(self, manager, mock_client):
        """Test batch recommendations with errors."""
        mock_client.call_api.return_value = {
            'ErrorEntries': [
                {'DatabaseId': 'db-1', 'ErrorMessage': 'Database not found'},
                {'DatabaseId': 'db-3', 'ErrorMessage': 'Invalid settings'},
            ]
        }

        data = [
            {'DatabaseId': 'db-1', 'Settings': {}},
            {'DatabaseId': 'db-2', 'Settings': {}},
            {'DatabaseId': 'db-3', 'Settings': {}},
        ]
        result = manager.batch_start_recommendations(data)

        assert result['success'] is False
        assert len(result['data']['error_entries']) == 2
        assert 'errors: 2' in result['data']['message']

    def test_batch_start_recommendations_empty_data(self, manager, mock_client):
        """Test batch recommendations with empty data."""
        mock_client.call_api.return_value = {'ErrorEntries': []}

        result = manager.batch_start_recommendations([])

        assert result['success'] is True
        # Empty list is still passed as Data parameter
        mock_client.call_api.assert_called_once()

    def test_batch_start_recommendations_none_data(self, manager, mock_client):
        """Test batch recommendations with None data."""
        mock_client.call_api.return_value = {'ErrorEntries': []}

        result = manager.batch_start_recommendations(None)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        # When data is None, Data key should not be present
        assert 'Data' not in call_args

    def test_batch_start_recommendations_single_database(self, manager, mock_client):
        """Test batch recommendations with single database."""
        mock_client.call_api.return_value = {'ErrorEntries': []}

        data = [{'DatabaseId': 'db-1', 'Settings': {'AnalysisType': 'comprehensive'}}]
        result = manager.batch_start_recommendations(data)

        assert result['success'] is True
        assert len(result['data']['error_entries']) == 0


class TestRecommendationManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create RecommendationManager instance."""
        return RecommendationManager(mock_client)

    def test_list_recommendations_api_error(self, manager, mock_client):
        """Test API error during recommendations listing."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.list_recommendations()

        assert 'API Error' in str(exc_info.value)

    def test_list_recommendation_limitations_api_error(self, manager, mock_client):
        """Test API error during limitations listing."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.list_recommendation_limitations()

        assert 'Network error' in str(exc_info.value)

    def test_start_recommendations_api_error(self, manager, mock_client):
        """Test API error during recommendations start."""
        mock_client.call_api.side_effect = Exception('Start failed')

        with pytest.raises(Exception) as exc_info:
            manager.start_recommendations('db-1', {})

        assert 'Start failed' in str(exc_info.value)

    def test_batch_start_recommendations_api_error(self, manager, mock_client):
        """Test API error during batch recommendations start."""
        mock_client.call_api.side_effect = Exception('Batch error')

        with pytest.raises(Exception) as exc_info:
            manager.batch_start_recommendations([{'DatabaseId': 'db-1', 'Settings': {}}])

        assert 'Batch error' in str(exc_info.value)


class TestRecommendationManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create RecommendationManager instance."""
        return RecommendationManager(mock_client)

    def test_list_recommendations_with_max_results_boundary(self, manager, mock_client):
        """Test list recommendations with maximum results."""
        mock_client.call_api.return_value = {'Recommendations': []}

        result = manager.list_recommendations(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_list_recommendations_multiple_pages(self, manager, mock_client):
        """Test listing recommendations across multiple pages."""
        # First call
        mock_client.call_api.return_value = {
            'Recommendations': [{'RecommendationId': 'rec-1'}],
            'NextToken': 'token-1',
        }

        result1 = manager.list_recommendations()

        assert result1['success'] is True
        assert result1['data']['count'] == 1
        assert result1['data']['next_token'] == 'token-1'

        # Second call with token
        mock_client.call_api.return_value = {'Recommendations': [{'RecommendationId': 'rec-2'}]}

        result2 = manager.list_recommendations(marker='token-1')

        assert result2['success'] is True
        assert result2['data']['count'] == 1
        assert 'next_token' not in result2['data']

    def test_batch_start_with_large_dataset(self, manager, mock_client):
        """Test batch start with large dataset."""
        mock_client.call_api.return_value = {'ErrorEntries': []}

        # Create large dataset
        data = [{'DatabaseId': f'db-{i}', 'Settings': {}} for i in range(100)]
        result = manager.batch_start_recommendations(data)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['Data']) == 100

    def test_start_recommendations_with_empty_database_id(self, manager, mock_client):
        """Test starting recommendations with empty database ID."""
        mock_client.call_api.return_value = {}

        result = manager.start_recommendations('', {})

        assert result['success'] is True
        assert result['data']['database_id'] == ''

    def test_batch_partial_errors(self, manager, mock_client):
        """Test batch recommendations with partial errors."""
        mock_client.call_api.return_value = {
            'ErrorEntries': [{'DatabaseId': 'db-2', 'ErrorMessage': 'Invalid'}]
        }

        data = [
            {'DatabaseId': 'db-1', 'Settings': {}},
            {'DatabaseId': 'db-2', 'Settings': {}},
            {'DatabaseId': 'db-3', 'Settings': {}},
        ]
        result = manager.batch_start_recommendations(data)

        assert result['success'] is False
        assert len(result['data']['error_entries']) == 1
        assert 'errors: 1' in result['data']['message']

    def test_list_limitations_with_all_optional_params(self, manager, mock_client):
        """Test listing limitations with all optional parameters."""
        mock_client.call_api.return_value = {'Limitations': []}

        filters = [
            {'Name': 'limitation-type', 'Values': ['memory', 'storage']},
            {'Name': 'database-engine', 'Values': ['mysql']},
        ]
        result = manager.list_recommendation_limitations(
            filters=filters, max_results=25, marker='custom-token'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 25
        assert call_args['NextToken'] == 'custom-token'

    def test_start_recommendations_settings_preservation(self, manager, mock_client):
        """Test that settings are preserved in start_recommendations."""
        mock_client.call_api.return_value = {}

        original_settings = {
            'AnalysisType': 'comprehensive',
            'NestedObject': {'Key': 'Value', 'Number': 42},
        }
        result = manager.start_recommendations('db-1', original_settings)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Settings'] == original_settings
        assert call_args['Settings']['NestedObject']['Number'] == 42
