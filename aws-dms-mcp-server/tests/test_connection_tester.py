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

"""Comprehensive tests for ConnectionTester module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.connection_tester import ConnectionTester
from datetime import datetime, timedelta
from unittest.mock import Mock, patch


class TestConnectionTesterBasicOperations:
    """Test basic connection testing operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def tester(self, mock_client):
        """Create ConnectionTester instance with caching disabled."""
        return ConnectionTester(mock_client, enable_caching=False)

    @pytest.fixture
    def tester_with_cache(self, mock_client):
        """Create ConnectionTester instance with caching enabled."""
        return ConnectionTester(mock_client, enable_caching=True)

    def test_initialization_with_caching(self, mock_client):
        """Test initialization with caching enabled."""
        tester = ConnectionTester(mock_client, enable_caching=True)
        assert tester.enable_caching is True
        assert tester._cache == {}

    def test_initialization_without_caching(self, mock_client):
        """Test initialization with caching disabled."""
        tester = ConnectionTester(mock_client, enable_caching=False)
        assert tester.enable_caching is False
        assert tester._cache == {}

    def test_test_connection_successful(self, tester, mock_client):
        """Test successful connection test."""
        mock_client.call_api.return_value = {
            'Connection': {
                'Status': 'successful',
                'EndpointIdentifier': 'test-endpoint',
                'ReplicationInstanceIdentifier': 'test-instance',
            }
        }

        result = tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        assert result['success'] is True
        assert result['data']['status'] == 'successful'
        assert result['error'] is None

    def test_test_connection_failed(self, tester, mock_client):
        """Test failed connection test."""
        # Initial test_connection call
        mock_client.call_api.return_value = {
            'Connection': {'Status': 'testing', 'LastFailureMessage': 'Connection failed'}
        }

        # Mock describe_connections for polling - will return failed status immediately
        def mock_call_api(operation, **kwargs):
            if operation == 'test_connection':
                return {'Connection': {'Status': 'testing'}}
            elif operation == 'describe_connections':
                return {
                    'Connections': [
                        {
                            'Status': 'failed',
                            'LastFailureMessage': 'Connection timeout',
                            'EndpointIdentifier': 'test-endpoint',
                            'ReplicationInstanceIdentifier': 'test-instance',
                        }
                    ]
                }
            return {}

        mock_client.call_api.side_effect = mock_call_api

        result = tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        assert result['success'] is False
        assert result['data']['status'] == 'failed'
        assert result['error'] is not None
        assert 'Connection timeout' in result['error']['message']


class TestConnectionTesterPolling:
    """Test connection test polling behavior."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def tester(self, mock_client):
        """Create ConnectionTester instance."""
        return ConnectionTester(mock_client, enable_caching=False)

    @patch('awslabs.aws_dms_mcp_server.utils.connection_tester.time.sleep')
    def test_test_connection_polling_success(self, mock_sleep, tester, mock_client):
        """Test connection test with polling until success."""
        call_count = [0]

        def mock_call_api(operation, **kwargs):
            if operation == 'test_connection':
                return {'Connection': {'Status': 'testing'}}
            elif operation == 'describe_connections':
                call_count[0] += 1
                if call_count[0] < 3:
                    return {'Connections': [{'Status': 'testing'}]}
                else:
                    return {
                        'Connections': [
                            {
                                'Status': 'successful',
                                'EndpointIdentifier': 'test-endpoint',
                                'ReplicationInstanceIdentifier': 'test-instance',
                            }
                        ]
                    }
            return {}

        mock_client.call_api.side_effect = mock_call_api

        result = tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        assert result['success'] is True
        assert result['data']['status'] == 'successful'
        assert mock_sleep.call_count >= 2

    @patch('awslabs.aws_dms_mcp_server.utils.connection_tester.time.sleep')
    def test_test_connection_polling_timeout(self, mock_sleep, tester, mock_client):
        """Test connection test polling timeout."""

        def mock_call_api(operation, **kwargs):
            if operation == 'test_connection':
                return {'Connection': {'Status': 'testing'}}
            elif operation == 'describe_connections':
                # Always return testing status to trigger timeout
                return {'Connections': [{'Status': 'testing'}]}
            return {}

        mock_client.call_api.side_effect = mock_call_api

        result = tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        assert result['data']['status'] == 'testing'
        # Should have attempted max_attempts (12) times
        assert mock_sleep.call_count == 12


class TestConnectionTesterCaching:
    """Test caching functionality."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def tester(self, mock_client):
        """Create ConnectionTester instance with caching enabled."""
        return ConnectionTester(mock_client, enable_caching=True)

    def test_test_connection_caches_result(self, tester, mock_client):
        """Test that connection test results are cached."""
        mock_client.call_api.return_value = {
            'Connection': {
                'Status': 'successful',
                'EndpointIdentifier': 'test-endpoint',
                'ReplicationInstanceIdentifier': 'test-instance',
            }
        }

        # First call should hit API
        result1 = tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        # Second call should use cache
        result2 = tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        assert result1 == result2
        # API should only be called once
        assert mock_client.call_api.call_count == 1

    def test_test_connection_cache_expiration(self, tester, mock_client):
        """Test that cache expires after 5 minutes."""
        mock_client.call_api.return_value = {
            'Connection': {
                'Status': 'successful',
                'EndpointIdentifier': 'test-endpoint',
                'ReplicationInstanceIdentifier': 'test-instance',
            }
        }

        # First call
        tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        # Manually expire cache
        cache_key = (
            'arn:aws:dms:us-east-1:123:rep:instance:arn:aws:dms:us-east-1:123:endpoint:test'
        )
        tester._cache[cache_key]['_cached_at'] = datetime.utcnow() - timedelta(minutes=6)

        # Second call should hit API due to expired cache
        tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        # API should be called twice
        assert mock_client.call_api.call_count == 2

    def test_clear_cache(self, tester, mock_client):
        """Test cache clearing."""
        mock_client.call_api.return_value = {
            'Connection': {
                'Status': 'successful',
                'EndpointIdentifier': 'test-endpoint',
                'ReplicationInstanceIdentifier': 'test-instance',
            }
        }

        # Add item to cache
        tester.test_connection(
            'arn:aws:dms:us-east-1:123:rep:instance', 'arn:aws:dms:us-east-1:123:endpoint:test'
        )

        assert len(tester._cache) > 0

        # Clear cache
        tester.clear_cache()

        assert len(tester._cache) == 0


class TestConnectionTesterListOperations:
    """Test connection listing operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def tester(self, mock_client):
        """Create ConnectionTester instance."""
        return ConnectionTester(mock_client)

    def test_list_connection_tests_success(self, tester, mock_client):
        """Test successful connection test listing."""
        mock_client.call_api.return_value = {
            'Connections': [
                {
                    'EndpointArn': 'arn:1',
                    'EndpointIdentifier': 'endpoint-1',
                    'ReplicationInstanceArn': 'arn:rep1',
                    'ReplicationInstanceIdentifier': 'instance-1',
                    'Status': 'successful',
                },
                {
                    'EndpointArn': 'arn:2',
                    'EndpointIdentifier': 'endpoint-2',
                    'ReplicationInstanceArn': 'arn:rep2',
                    'ReplicationInstanceIdentifier': 'instance-2',
                    'Status': 'failed',
                    'LastFailureMessage': 'Connection timeout',
                },
            ]
        }

        result = tester.list_connection_tests()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'connections' in result['data']

    def test_list_connection_tests_with_filters(self, tester, mock_client):
        """Test listing connection tests with filters."""
        mock_client.call_api.return_value = {'Connections': []}

        filters = [{'Name': 'endpoint-arn', 'Values': ['arn:aws:dms:us-east-1:123:endpoint:test']}]
        result = tester.list_connection_tests(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_connection_tests_with_pagination(self, tester, mock_client):
        """Test listing connection tests with pagination."""
        mock_client.call_api.return_value = {'Connections': [], 'Marker': 'next-token'}

        result = tester.list_connection_tests()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_list_connection_tests_empty(self, tester, mock_client):
        """Test listing connection tests with empty result."""
        mock_client.call_api.return_value = {'Connections': []}

        result = tester.list_connection_tests()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['connections'] == []


class TestConnectionTesterDeleteOperations:
    """Test connection deletion operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def tester(self, mock_client):
        """Create ConnectionTester instance with caching enabled."""
        return ConnectionTester(mock_client, enable_caching=True)

    def test_delete_connection_success(self, tester, mock_client):
        """Test successful connection deletion."""
        mock_client.call_api.return_value = {
            'Connection': {'EndpointArn': 'arn:endpoint', 'ReplicationInstanceArn': 'arn:instance'}
        }

        result = tester.delete_connection('arn:endpoint', 'arn:instance')

        assert result['success'] is True
        assert result['data']['message'] == 'Connection deleted successfully'
        assert 'connection' in result['data']

    def test_delete_connection_clears_cache(self, tester, mock_client):
        """Test that deleting connection clears cache."""
        # Add item to cache
        cache_key = 'arn:instance:arn:endpoint'
        tester._cache[cache_key] = {'data': 'test'}

        mock_client.call_api.return_value = {'Connection': {}}

        tester.delete_connection('arn:endpoint', 'arn:instance')

        # Cache should be cleared
        assert cache_key not in tester._cache

    def test_delete_connection_when_not_in_cache(self, tester, mock_client):
        """Test deleting connection when not in cache."""
        mock_client.call_api.return_value = {'Connection': {}}

        # Should not raise error
        result = tester.delete_connection('arn:endpoint', 'arn:instance')

        assert result['success'] is True


class TestConnectionTesterErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def tester(self, mock_client):
        """Create ConnectionTester instance."""
        return ConnectionTester(mock_client)

    def test_test_connection_api_error(self, tester, mock_client):
        """Test API error during connection test."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            tester.test_connection('arn:instance', 'arn:endpoint')

        assert 'API Error' in str(exc_info.value)

    def test_list_connection_tests_api_error(self, tester, mock_client):
        """Test API error during connection listing."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            tester.list_connection_tests()

        assert 'Network error' in str(exc_info.value)

    def test_delete_connection_api_error(self, tester, mock_client):
        """Test API error during connection deletion."""
        mock_client.call_api.side_effect = Exception('Delete failed')

        with pytest.raises(Exception) as exc_info:
            tester.delete_connection('arn:endpoint', 'arn:instance')

        assert 'Delete failed' in str(exc_info.value)


class TestConnectionTesterEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def tester(self, mock_client):
        """Create ConnectionTester instance."""
        return ConnectionTester(mock_client, enable_caching=True)

    def test_test_connection_with_none_failure_message(self, tester, mock_client):
        """Test connection test with None failure message."""
        mock_client.call_api.return_value = {
            'Connection': {
                'Status': 'failed',
                'EndpointIdentifier': 'test-endpoint',
                'ReplicationInstanceIdentifier': 'test-instance',
                'LastFailureMessage': None,
            }
        }

        result = tester.test_connection('arn:instance', 'arn:endpoint')

        assert result['success'] is False
        assert result['data']['last_failure_message'] is None

    def test_test_connection_missing_identifiers(self, tester, mock_client):
        """Test connection test with missing identifiers."""
        mock_client.call_api.return_value = {
            'Connection': {'Status': 'successful'}
            # Missing EndpointIdentifier and ReplicationInstanceIdentifier
        }

        result = tester.test_connection('arn:instance', 'arn:endpoint')

        assert result['success'] is True
        assert result['data']['endpoint_identifier'] is None
        assert result['data']['replication_instance_identifier'] is None

    def test_list_connection_tests_with_max_results_boundary(self, tester, mock_client):
        """Test list operations with maximum results."""
        mock_client.call_api.return_value = {'Connections': []}

        result = tester.list_connection_tests(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_cache_with_different_instances_same_endpoint(self, tester, mock_client):
        """Test that cache handles different instances with same endpoint."""
        mock_client.call_api.return_value = {
            'Connection': {
                'Status': 'successful',
                'EndpointIdentifier': 'test-endpoint',
                'ReplicationInstanceIdentifier': 'test-instance',
            }
        }

        # Test with first instance
        tester.test_connection('arn:instance1', 'arn:endpoint')

        # Test with second instance (same endpoint)
        tester.test_connection('arn:instance2', 'arn:endpoint')

        # Should be cached separately
        assert len(tester._cache) == 2
        assert mock_client.call_api.call_count == 2

    @patch('awslabs.aws_dms_mcp_server.utils.connection_tester.time.sleep')
    def test_test_connection_no_connections_returned(self, mock_sleep, tester, mock_client):
        """Test connection test when describe_connections returns no connections."""

        def mock_call_api(operation, **kwargs):
            if operation == 'test_connection':
                return {'Connection': {'Status': 'testing'}}
            elif operation == 'describe_connections':
                return {'Connections': []}  # Empty list
            return {}

        mock_client.call_api.side_effect = mock_call_api

        result = tester.test_connection('arn:instance', 'arn:endpoint')

        # Should timeout with testing status since no connections found
        assert result['data']['status'] == 'testing'
