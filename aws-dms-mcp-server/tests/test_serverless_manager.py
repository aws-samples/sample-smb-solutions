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

"""Comprehensive tests for ServerlessManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.serverless_manager import ServerlessManager
from unittest.mock import Mock


class TestServerlessManagerMigrationProjects:
    """Test migration project operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_create_migration_project_success(self, manager, mock_client):
        """Test successful migration project creation."""
        mock_client.call_api.return_value = {
            'MigrationProject': {
                'MigrationProjectArn': 'arn:aws:dms:us-east-1:123:migration-project:test',
                'MigrationProjectIdentifier': 'test-project',
            }
        }

        result = manager.create_migration_project(
            identifier='test-project',
            instance_profile_arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            source_data_provider_descriptors=[{'DataProviderArn': 'source-arn'}],
            target_data_provider_descriptors=[{'DataProviderArn': 'target-arn'}],
        )

        assert result['success'] is True
        assert 'migration_project' in result['data']
        assert result['data']['message'] == 'Migration project created successfully'
        mock_client.call_api.assert_called_once()

    def test_create_migration_project_with_all_params(self, manager, mock_client):
        """Test migration project creation with all optional parameters."""
        mock_client.call_api.return_value = {'MigrationProject': {}}

        result = manager.create_migration_project(
            identifier='test-project',
            instance_profile_arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            source_data_provider_descriptors=[{'DataProviderArn': 'source-arn'}],
            target_data_provider_descriptors=[{'DataProviderArn': 'target-arn'}],
            transformation_rules='{"rules": []}',
            description='Test project',
            schema_conversion_application_attributes={'setting': 'value'},
            tags=[{'Key': 'env', 'Value': 'test'}],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'TransformationRules' in call_args
        assert 'Description' in call_args
        assert 'SchemaConversionApplicationAttributes' in call_args
        assert 'Tags' in call_args

    def test_modify_migration_project_success(self, manager, mock_client):
        """Test successful migration project modification."""
        mock_client.call_api.return_value = {
            'MigrationProject': {
                'MigrationProjectArn': 'arn:aws:dms:us-east-1:123:migration-project:test'
            }
        }

        result = manager.modify_migration_project(
            arn='arn:aws:dms:us-east-1:123:migration-project:test',
            identifier='new-identifier',
            description='Updated description',
        )

        assert result['success'] is True
        assert 'migration_project' in result['data']
        mock_client.call_api.assert_called_once_with(
            'modify_migration_project',
            MigrationProjectArn='arn:aws:dms:us-east-1:123:migration-project:test',
            MigrationProjectIdentifier='new-identifier',
            Description='Updated description',
        )

    def test_delete_migration_project_success(self, manager, mock_client):
        """Test successful migration project deletion."""
        mock_client.call_api.return_value = {'MigrationProject': {}}

        result = manager.delete_migration_project(
            'arn:aws:dms:us-east-1:123:migration-project:test'
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Migration project deleted successfully'
        mock_client.call_api.assert_called_once_with(
            'delete_migration_project',
            MigrationProjectArn='arn:aws:dms:us-east-1:123:migration-project:test',
        )

    def test_list_migration_projects_success(self, manager, mock_client):
        """Test listing migration projects."""
        mock_client.call_api.return_value = {
            'MigrationProjects': [
                {'MigrationProjectIdentifier': 'project-1'},
                {'MigrationProjectIdentifier': 'project-2'},
            ]
        }

        result = manager.list_migration_projects()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'migration_projects' in result['data']

    def test_list_migration_projects_with_filters(self, manager, mock_client):
        """Test listing migration projects with filters."""
        mock_client.call_api.return_value = {'MigrationProjects': []}

        filters = [{'Name': 'migration-project-identifier', 'Values': ['test']}]
        result = manager.list_migration_projects(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_migration_projects_with_pagination(self, manager, mock_client):
        """Test listing migration projects with pagination."""
        mock_client.call_api.return_value = {'MigrationProjects': [], 'Marker': 'next-token'}

        result = manager.list_migration_projects()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'


class TestServerlessManagerDataProviders:
    """Test data provider operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_create_data_provider_success(self, manager, mock_client):
        """Test successful data provider creation."""
        mock_client.call_api.return_value = {
            'DataProvider': {'DataProviderArn': 'arn:aws:dms:us-east-1:123:data-provider:test'}
        }

        result = manager.create_data_provider(
            identifier='test-provider',
            engine='mysql',
            settings={'ServerName': 'localhost', 'Port': 3306},
        )

        assert result['success'] is True
        assert 'data_provider' in result['data']
        assert result['data']['message'] == 'Data provider created successfully'

    def test_create_data_provider_with_optional_params(self, manager, mock_client):
        """Test data provider creation with optional parameters."""
        mock_client.call_api.return_value = {'DataProvider': {}}

        result = manager.create_data_provider(
            identifier='test-provider',
            engine='postgres',
            settings={},
            description='Test data provider',
            tags=[{'Key': 'env', 'Value': 'test'}],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Description' in call_args
        assert 'Tags' in call_args

    def test_modify_data_provider_success(self, manager, mock_client):
        """Test successful data provider modification."""
        mock_client.call_api.return_value = {'DataProvider': {}}

        result = manager.modify_data_provider(
            arn='arn:aws:dms:us-east-1:123:data-provider:test',
            identifier='new-identifier',
            engine='postgresql',
            settings={'Port': 5432},
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Data provider modified successfully'

    def test_delete_data_provider_success(self, manager, mock_client):
        """Test successful data provider deletion."""
        mock_client.call_api.return_value = {'DataProvider': {}}

        result = manager.delete_data_provider('arn:aws:dms:us-east-1:123:data-provider:test')

        assert result['success'] is True
        assert result['data']['message'] == 'Data provider deleted successfully'

    def test_list_data_providers_success(self, manager, mock_client):
        """Test listing data providers."""
        mock_client.call_api.return_value = {
            'DataProviders': [{'DataProviderIdentifier': 'provider-1'}]
        }

        result = manager.list_data_providers()

        assert result['success'] is True
        assert result['data']['count'] == 1
        assert 'data_providers' in result['data']

    def test_list_data_providers_with_pagination(self, manager, mock_client):
        """Test listing data providers with pagination."""
        mock_client.call_api.return_value = {'DataProviders': [], 'Marker': 'next-token'}

        result = manager.list_data_providers(max_results=25)

        assert result['success'] is True
        assert 'next_marker' in result['data']


class TestServerlessManagerInstanceProfiles:
    """Test instance profile operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_create_instance_profile_success(self, manager, mock_client):
        """Test successful instance profile creation."""
        mock_client.call_api.return_value = {
            'InstanceProfile': {
                'InstanceProfileArn': 'arn:aws:dms:us-east-1:123:instance-profile:test'
            }
        }

        result = manager.create_instance_profile(identifier='test-profile')

        assert result['success'] is True
        assert 'instance_profile' in result['data']
        assert result['data']['message'] == 'Instance profile created successfully'

    def test_create_instance_profile_with_all_params(self, manager, mock_client):
        """Test instance profile creation with all parameters."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.create_instance_profile(
            identifier='test-profile',
            description='Test profile',
            kms_key_arn='arn:aws:kms:us-east-1:123:key/test',
            publicly_accessible=True,
            network_type='IPV4',
            subnet_group_identifier='subnet-group-1',
            vpc_security_groups=['sg-123'],
            tags=[{'Key': 'env', 'Value': 'test'}],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['PubliclyAccessible'] is True
        assert 'KmsKeyArn' in call_args
        assert 'Tags' in call_args

    def test_create_instance_profile_publicly_accessible_false(self, manager, mock_client):
        """Test instance profile creation with publicly_accessible explicitly False."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.create_instance_profile(
            identifier='test-profile', publicly_accessible=False
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'PubliclyAccessible' in call_args
        assert call_args['PubliclyAccessible'] is False

    def test_modify_instance_profile_success(self, manager, mock_client):
        """Test successful instance profile modification."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.modify_instance_profile(
            arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            identifier='new-identifier',
            description='Updated',
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Instance profile modified successfully'

    def test_modify_instance_profile_publicly_accessible(self, manager, mock_client):
        """Test modifying instance profile with publicly_accessible parameter."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.modify_instance_profile(
            arn='arn:aws:dms:us-east-1:123:instance-profile:test', publicly_accessible=False
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'PubliclyAccessible' in call_args

    def test_delete_instance_profile_success(self, manager, mock_client):
        """Test successful instance profile deletion."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.delete_instance_profile('arn:aws:dms:us-east-1:123:instance-profile:test')

        assert result['success'] is True
        assert result['data']['message'] == 'Instance profile deleted successfully'

    def test_list_instance_profiles_success(self, manager, mock_client):
        """Test listing instance profiles."""
        mock_client.call_api.return_value = {
            'InstanceProfiles': [{'InstanceProfileIdentifier': 'profile-1'}]
        }

        result = manager.list_instance_profiles()

        assert result['success'] is True
        assert result['data']['count'] == 1
        assert 'instance_profiles' in result['data']

    def test_list_instance_profiles_with_filters(self, manager, mock_client):
        """Test listing instance profiles with filters."""
        mock_client.call_api.return_value = {'InstanceProfiles': []}

        filters = [{'Name': 'instance-profile-identifier', 'Values': ['test']}]
        result = manager.list_instance_profiles(filters=filters)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters


class TestServerlessManagerDataMigrations:
    """Test data migration operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_create_data_migration_success(self, manager, mock_client):
        """Test successful data migration creation."""
        mock_client.call_api.return_value = {
            'DataMigration': {'DataMigrationArn': 'arn:aws:dms:us-east-1:123:data-migration:test'}
        }

        result = manager.create_data_migration(
            identifier='test-migration',
            migration_type='full-load',
            service_access_role_arn='arn:aws:iam::123:role/test',
            source_data_settings=[{'DataProviderArn': 'source-arn'}],
        )

        assert result['success'] is True
        assert 'data_migration' in result['data']
        assert result['data']['message'] == 'Data migration created successfully'

    def test_create_data_migration_with_optional_params(self, manager, mock_client):
        """Test data migration creation with optional parameters."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.create_data_migration(
            identifier='test-migration',
            migration_type='full-load-and-cdc',
            service_access_role_arn='arn:aws:iam::123:role/test',
            source_data_settings=[],
            data_migration_settings={'setting': 'value'},
            data_migration_name='My Migration',
            tags=[{'Key': 'env', 'Value': 'test'}],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'DataMigrationSettings' in call_args
        assert 'DataMigrationName' in call_args
        assert 'Tags' in call_args

    def test_modify_data_migration_success(self, manager, mock_client):
        """Test successful data migration modification."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.modify_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test',
            identifier='new-identifier',
            migration_type='cdc',
            number_of_jobs=4,
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Data migration modified successfully'
        call_args = mock_client.call_api.call_args[1]
        assert call_args['NumberOfJobs'] == 4

    def test_delete_data_migration_success(self, manager, mock_client):
        """Test successful data migration deletion."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.delete_data_migration('arn:aws:dms:us-east-1:123:data-migration:test')

        assert result['success'] is True
        assert result['data']['message'] == 'Data migration deleted successfully'

    def test_list_data_migrations_success(self, manager, mock_client):
        """Test listing data migrations."""
        mock_client.call_api.return_value = {
            'DataMigrations': [
                {'DataMigrationIdentifier': 'migration-1'},
                {'DataMigrationIdentifier': 'migration-2'},
            ]
        }

        result = manager.list_data_migrations()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'data_migrations' in result['data']

    def test_list_data_migrations_with_pagination(self, manager, mock_client):
        """Test listing data migrations with pagination."""
        mock_client.call_api.return_value = {'DataMigrations': [], 'Marker': 'next-token'}

        result = manager.list_data_migrations(max_results=10)

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_start_data_migration_success(self, manager, mock_client):
        """Test successful data migration start."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.start_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test', start_type='start-replication'
        )

        assert result['success'] is True
        assert 'Data migration started with type: start-replication' in result['data']['message']
        mock_client.call_api.assert_called_once_with(
            'start_data_migration',
            DataMigrationArn='arn:aws:dms:us-east-1:123:data-migration:test',
            StartType='start-replication',
        )

    def test_start_data_migration_resume_processing(self, manager, mock_client):
        """Test starting data migration with resume-processing type."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.start_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test', start_type='resume-processing'
        )

        assert result['success'] is True
        assert 'resume-processing' in result['data']['message']

    def test_stop_data_migration_success(self, manager, mock_client):
        """Test successful data migration stop."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.stop_data_migration('arn:aws:dms:us-east-1:123:data-migration:test')

        assert result['success'] is True
        assert result['data']['message'] == 'Data migration stop initiated'
        mock_client.call_api.assert_called_once_with(
            'stop_data_migration', DataMigrationArn='arn:aws:dms:us-east-1:123:data-migration:test'
        )


class TestServerlessManagerErrorHandling:
    """Test error handling across all operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_create_migration_project_api_error(self, manager, mock_client):
        """Test API error during migration project creation."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.create_migration_project(
                identifier='test',
                instance_profile_arn='arn',
                source_data_provider_descriptors=[],
                target_data_provider_descriptors=[],
            )

        assert 'API Error' in str(exc_info.value)

    def test_create_data_provider_api_error(self, manager, mock_client):
        """Test API error during data provider creation."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.create_data_provider(identifier='test', engine='mysql', settings={})

        assert 'Network error' in str(exc_info.value)

    def test_list_operations_empty_response(self, manager, mock_client):
        """Test list operations with empty response."""
        mock_client.call_api.return_value = {'MigrationProjects': []}

        result = manager.list_migration_projects()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['migration_projects'] == []


class TestServerlessManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_modify_with_no_optional_params(self, manager, mock_client):
        """Test modify operations with only required parameters."""
        mock_client.call_api.return_value = {'MigrationProject': {}}

        result = manager.modify_migration_project(
            arn='arn:aws:dms:us-east-1:123:migration-project:test'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        # Only ARN should be present
        assert 'MigrationProjectArn' in call_args
        assert len(call_args) == 1

    def test_list_with_max_results(self, manager, mock_client):
        """Test list operations with custom max_results."""
        mock_client.call_api.return_value = {'DataProviders': []}

        result = manager.list_data_providers(max_results=200)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 200

    def test_create_with_empty_tags(self, manager, mock_client):
        """Test creation with empty tags list - empty list not passed due to truthy check."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.create_data_migration(
            identifier='test',
            migration_type='full-load',
            service_access_role_arn='arn',
            source_data_settings=[],
            tags=[],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        # Empty list is not passed due to 'if tags:' check treating empty list as falsy
        assert 'Tags' not in call_args

    def test_list_all_resource_types_pagination(self, manager, mock_client):
        """Test pagination consistency across all list operations."""
        test_cases = [
            ('list_migration_projects', 'MigrationProjects'),
            ('list_data_providers', 'DataProviders'),
            ('list_instance_profiles', 'InstanceProfiles'),
            ('list_data_migrations', 'DataMigrations'),
        ]

        for method_name, response_key in test_cases:
            mock_client.call_api.return_value = {response_key: [], 'Marker': 'test-token'}
            method = getattr(manager, method_name)
            result = method()

            assert result['success'] is True
            assert 'next_marker' in result['data']
            assert result['data']['next_marker'] == 'test-token'
            mock_client.call_api.reset_mock()
