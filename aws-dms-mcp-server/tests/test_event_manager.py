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

"""Comprehensive tests for EventManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.event_manager import EventManager
from unittest.mock import Mock


class TestEventManagerCreateSubscription:
    """Test event subscription creation."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_create_event_subscription_success(self, manager, mock_client):
        """Test successful event subscription creation."""
        mock_client.call_api.return_value = {
            'EventSubscription': {'SubscriptionName': 'test-subscription'}
        }

        result = manager.create_event_subscription(
            'test-subscription', 'arn:aws:sns:us-east-1:123456789012:test-topic'
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Event subscription created successfully'
        assert 'event_subscription' in result['data']

    def test_create_event_subscription_with_all_params(self, manager, mock_client):
        """Test event subscription creation with all parameters."""
        mock_client.call_api.return_value = {'EventSubscription': {}}

        tags = [{'Key': 'Environment', 'Value': 'Test'}]
        result = manager.create_event_subscription(
            'test-subscription',
            'arn:aws:sns:us-east-1:123456789012:test-topic',
            source_type='replication-instance',
            event_categories=['failure', 'deletion'],
            source_ids=['instance-1', 'instance-2'],
            enabled=True,
            tags=tags,
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SourceType'] == 'replication-instance'
        assert call_args['EventCategories'] == ['failure', 'deletion']
        assert call_args['SourceIds'] == ['instance-1', 'instance-2']
        assert call_args['Enabled'] is True
        assert call_args['Tags'] == tags

    def test_create_event_subscription_minimal_params(self, manager, mock_client):
        """Test event subscription creation with minimal parameters."""
        mock_client.call_api.return_value = {'EventSubscription': {}}

        result = manager.create_event_subscription('test-sub', 'arn:sns:topic')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SubscriptionName'] == 'test-sub'
        assert call_args['SnsTopicArn'] == 'arn:sns:topic'
        assert call_args['Enabled'] is True
        assert 'SourceType' not in call_args

    def test_create_event_subscription_disabled(self, manager, mock_client):
        """Test creating disabled event subscription."""
        mock_client.call_api.return_value = {'EventSubscription': {}}

        result = manager.create_event_subscription('test-sub', 'arn:sns:topic', enabled=False)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Enabled'] is False


class TestEventManagerModifySubscription:
    """Test event subscription modification."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_modify_event_subscription_success(self, manager, mock_client):
        """Test successful event subscription modification."""
        mock_client.call_api.return_value = {
            'EventSubscription': {'SubscriptionName': 'test-subscription'}
        }

        result = manager.modify_event_subscription('test-subscription')

        assert result['success'] is True
        assert result['data']['message'] == 'Event subscription modified successfully'

    def test_modify_event_subscription_all_params(self, manager, mock_client):
        """Test subscription modification with all parameters."""
        mock_client.call_api.return_value = {'EventSubscription': {}}

        result = manager.modify_event_subscription(
            'test-subscription',
            sns_topic_arn='arn:new:topic',
            source_type='replication-task',
            event_categories=['configuration change'],
            enabled=False,
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SnsTopicArn'] == 'arn:new:topic'
        assert call_args['SourceType'] == 'replication-task'
        assert call_args['EventCategories'] == ['configuration change']
        assert call_args['Enabled'] is False

    def test_modify_event_subscription_partial_update(self, manager, mock_client):
        """Test subscription modification with partial parameters."""
        mock_client.call_api.return_value = {'EventSubscription': {}}

        result = manager.modify_event_subscription('test-sub', enabled=True)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Enabled'] is True
        assert 'SnsTopicArn' not in call_args


class TestEventManagerDeleteSubscription:
    """Test event subscription deletion."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_delete_event_subscription_success(self, manager, mock_client):
        """Test successful event subscription deletion."""
        mock_client.call_api.return_value = {
            'EventSubscription': {'SubscriptionName': 'test-subscription'}
        }

        result = manager.delete_event_subscription('test-subscription')

        assert result['success'] is True
        assert result['data']['message'] == 'Event subscription deleted successfully'
        mock_client.call_api.assert_called_once_with(
            'delete_event_subscription', SubscriptionName='test-subscription'
        )


class TestEventManagerListSubscriptions:
    """Test event subscription listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_list_event_subscriptions_success(self, manager, mock_client):
        """Test successful event subscriptions listing."""
        mock_client.call_api.return_value = {
            'EventSubscriptionsList': [
                {'SubscriptionName': 'sub-1'},
                {'SubscriptionName': 'sub-2'},
            ]
        }

        result = manager.list_event_subscriptions()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['event_subscriptions']) == 2

    def test_list_event_subscriptions_with_filters(self, manager, mock_client):
        """Test subscriptions listing with filters."""
        mock_client.call_api.return_value = {'EventSubscriptionsList': []}

        filters = [{'Name': 'enabled', 'Values': ['true']}]
        result = manager.list_event_subscriptions(
            subscription_name='test-sub', filters=filters, max_results=50, marker='token'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SubscriptionName'] == 'test-sub'
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_event_subscriptions_with_pagination(self, manager, mock_client):
        """Test subscriptions listing with pagination."""
        mock_client.call_api.return_value = {
            'EventSubscriptionsList': [],
            'Marker': 'next-token',
        }

        result = manager.list_event_subscriptions()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_list_event_subscriptions_empty_result(self, manager, mock_client):
        """Test subscriptions listing with empty result."""
        mock_client.call_api.return_value = {'EventSubscriptionsList': []}

        result = manager.list_event_subscriptions()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['event_subscriptions'] == []


class TestEventManagerListEvents:
    """Test events listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_list_events_success(self, manager, mock_client):
        """Test successful events listing."""
        mock_client.call_api.return_value = {
            'Events': [{'EventId': 'event-1'}, {'EventId': 'event-2'}]
        }

        result = manager.list_events()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['events']) == 2

    def test_list_events_with_all_params(self, manager, mock_client):
        """Test events listing with all parameters."""
        mock_client.call_api.return_value = {'Events': []}

        filters = [{'Name': 'event-type', 'Values': ['failure']}]
        result = manager.list_events(
            source_identifier='instance-1',
            source_type='replication-instance',
            start_time='2024-01-01T00:00:00Z',
            end_time='2024-01-02T00:00:00Z',
            duration=60,
            event_categories=['failure'],
            filters=filters,
            max_results=100,
            marker='token',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SourceIdentifier'] == 'instance-1'
        assert call_args['SourceType'] == 'replication-instance'
        assert call_args['StartTime'] == '2024-01-01T00:00:00Z'
        assert call_args['EndTime'] == '2024-01-02T00:00:00Z'
        assert call_args['Duration'] == 60
        assert call_args['EventCategories'] == ['failure']

    def test_list_events_with_pagination(self, manager, mock_client):
        """Test events listing with pagination."""
        mock_client.call_api.return_value = {'Events': [], 'Marker': 'next-token'}

        result = manager.list_events()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_list_events_by_source_identifier(self, manager, mock_client):
        """Test events listing by source identifier."""
        mock_client.call_api.return_value = {'Events': []}

        result = manager.list_events(source_identifier='task-1')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SourceIdentifier'] == 'task-1'

    def test_list_events_by_time_range(self, manager, mock_client):
        """Test events listing by time range."""
        mock_client.call_api.return_value = {'Events': []}

        result = manager.list_events(
            start_time='2024-01-01T00:00:00Z', end_time='2024-01-02T00:00:00Z'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['StartTime'] == '2024-01-01T00:00:00Z'
        assert call_args['EndTime'] == '2024-01-02T00:00:00Z'

    def test_list_events_by_duration(self, manager, mock_client):
        """Test events listing by duration."""
        mock_client.call_api.return_value = {'Events': []}

        result = manager.list_events(duration=120)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Duration'] == 120


class TestEventManagerListEventCategories:
    """Test event categories listing."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_list_event_categories_success(self, manager, mock_client):
        """Test successful event categories listing."""
        mock_client.call_api.return_value = {
            'EventCategoryGroupList': [
                {'SourceType': 'replication-instance', 'EventCategories': ['failure']},
                {'SourceType': 'replication-task', 'EventCategories': ['deletion']},
            ]
        }

        result = manager.list_event_categories()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['event_category_groups']) == 2

    def test_list_event_categories_with_source_type(self, manager, mock_client):
        """Test event categories listing with source type."""
        mock_client.call_api.return_value = {'EventCategoryGroupList': []}

        result = manager.list_event_categories(source_type='replication-instance')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SourceType'] == 'replication-instance'

    def test_list_event_categories_with_filters(self, manager, mock_client):
        """Test event categories listing with filters."""
        mock_client.call_api.return_value = {'EventCategoryGroupList': []}

        filters = [{'Name': 'source-type', 'Values': ['replication-task']}]
        result = manager.list_event_categories(filters=filters)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters

    def test_list_event_categories_empty_result(self, manager, mock_client):
        """Test event categories listing with empty result."""
        mock_client.call_api.return_value = {'EventCategoryGroupList': []}

        result = manager.list_event_categories()

        assert result['success'] is True
        assert result['data']['count'] == 0


class TestEventManagerUpdateToEventBridge:
    """Test updating subscriptions to EventBridge."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_update_subscriptions_to_event_bridge_success(self, manager, mock_client):
        """Test successful update to EventBridge."""
        mock_client.call_api.return_value = {'Result': 'migration-completed'}

        result = manager.update_subscriptions_to_event_bridge()

        assert result['success'] is True
        assert result['data']['message'] == 'Successfully updated subscriptions to EventBridge'
        assert result['data']['result'] == 'migration-completed'

    def test_update_subscriptions_to_event_bridge_force_move(self, manager, mock_client):
        """Test update to EventBridge with force move."""
        mock_client.call_api.return_value = {'Result': 'forced-migration'}

        result = manager.update_subscriptions_to_event_bridge(force_move=True)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['ForceMove'] is True

    def test_update_subscriptions_to_event_bridge_without_force(self, manager, mock_client):
        """Test update to EventBridge without force move."""
        mock_client.call_api.return_value = {'Result': 'success'}

        result = manager.update_subscriptions_to_event_bridge(force_move=False)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'ForceMove' not in call_args


class TestEventManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_create_subscription_api_error(self, manager, mock_client):
        """Test API error during subscription creation."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.create_event_subscription('test-sub', 'arn:topic')

        assert 'API Error' in str(exc_info.value)

    def test_modify_subscription_api_error(self, manager, mock_client):
        """Test API error during subscription modification."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.modify_event_subscription('test-sub')

        assert 'Network error' in str(exc_info.value)

    def test_delete_subscription_api_error(self, manager, mock_client):
        """Test API error during subscription deletion."""
        mock_client.call_api.side_effect = Exception('Service error')

        with pytest.raises(Exception) as exc_info:
            manager.delete_event_subscription('test-sub')

        assert 'Service error' in str(exc_info.value)

    def test_list_events_api_error(self, manager, mock_client):
        """Test API error during events listing."""
        mock_client.call_api.side_effect = Exception('Timeout error')

        with pytest.raises(Exception) as exc_info:
            manager.list_events()

        assert 'Timeout error' in str(exc_info.value)


class TestEventManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create EventManager instance."""
        return EventManager(mock_client)

    def test_list_subscriptions_max_results_boundary(self, manager, mock_client):
        """Test subscriptions listing with maximum results."""
        mock_client.call_api.return_value = {'EventSubscriptionsList': []}

        result = manager.list_event_subscriptions(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_list_events_max_results_boundary(self, manager, mock_client):
        """Test events listing with maximum results."""
        mock_client.call_api.return_value = {'Events': []}

        result = manager.list_events(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_create_subscription_with_multiple_source_ids(self, manager, mock_client):
        """Test subscription creation with multiple source IDs."""
        mock_client.call_api.return_value = {'EventSubscription': {}}

        source_ids = ['id-1', 'id-2', 'id-3', 'id-4', 'id-5']
        result = manager.create_event_subscription('test-sub', 'arn:topic', source_ids=source_ids)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['SourceIds'] == source_ids
        assert len(call_args['SourceIds']) == 5

    def test_create_subscription_with_multiple_event_categories(self, manager, mock_client):
        """Test subscription creation with multiple event categories."""
        mock_client.call_api.return_value = {'EventSubscription': {}}

        categories = ['failure', 'creation', 'deletion', 'configuration change']
        result = manager.create_event_subscription(
            'test-sub', 'arn:topic', event_categories=categories
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['EventCategories'] == categories

    def test_list_events_empty_filters(self, manager, mock_client):
        """Test events listing with empty filters list."""
        mock_client.call_api.return_value = {'Events': []}

        result = manager.list_events(filters=[])

        assert result['success'] is True
        # Empty filters list is not passed to API (falsy value)
        call_args = mock_client.call_api.call_args[1]
        assert 'Filters' not in call_args
