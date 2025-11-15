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

"""Enhanced tests for ServerlessManager - targeting missing coverage lines."""

import pytest
from awslabs.aws_dms_mcp_server.utils.serverless_manager import ServerlessManager
from unittest.mock import Mock


class TestServerlessManagerOptionalParameterCoverage:
    """Test optional parameter paths in ServerlessManager."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_modify_migration_project_all_optional_params(self, manager, mock_client):
        """Test modify_migration_project with all optional parameters."""
        mock_client.call_api.return_value = {'MigrationProject': {}}

        result = manager.modify_migration_project(
            arn='arn:aws:dms:us-east-1:123:migration-project:test',
            identifier='new-id',
            instance_profile_arn='arn:aws:dms:us-east-1:123:instance-profile:new',
            source_data_provider_descriptors=[{'DataProviderArn': 'source-arn'}],
            target_data_provider_descriptors=[{'DataProviderArn': 'target-arn'}],
            transformation_rules='{"rules": []}',
            description='Updated project',
            schema_conversion_application_attributes={'setting': 'value'},
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        # Verify all optional parameters are included
        assert 'MigrationProjectIdentifier' in call_args
        assert 'InstanceProfileArn' in call_args
        assert 'SourceDataProviderDescriptors' in call_args
        assert 'TargetDataProviderDescriptors' in call_args
        assert 'TransformationRules' in call_args
        assert 'Description' in call_args
        assert 'SchemaConversionApplicationAttributes' in call_args

    def test_modify_data_provider_all_optional_params(self, manager, mock_client):
        """Test modify_data_provider with all optional parameters."""
        mock_client.call_api.return_value = {'DataProvider': {}}

        result = manager.modify_data_provider(
            arn='arn:aws:dms:us-east-1:123:data-provider:test',
            identifier='new-id',
            engine='postgresql',
            settings={'Port': 5432},
            description='Updated provider',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        # Verify all optional parameters are included
        assert 'DataProviderIdentifier' in call_args
        assert 'Engine' in call_args
        assert 'Settings' in call_args
        assert 'Description' in call_args

    def test_list_data_providers_with_all_params(self, manager, mock_client):
        """Test list_data_providers with filters and marker."""
        mock_client.call_api.return_value = {'DataProviders': []}

        filters = [{'Name': 'engine', 'Values': ['mysql']}]
        result = manager.list_data_providers(filters=filters, max_results=50, marker='token123')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Filters' in call_args
        assert call_args['Filters'] == filters
        assert call_args['Marker'] == 'token123'

    def test_modify_instance_profile_all_optional_params(self, manager, mock_client):
        """Test modify_instance_profile with all optional parameters."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.modify_instance_profile(
            arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            identifier='new-id',
            description='Updated profile',
            kms_key_arn='arn:aws:kms:us-east-1:123:key/new',
            publicly_accessible=True,
            network_type='DUAL',
            subnet_group_identifier='subnet-group-2',
            vpc_security_groups=['sg-456', 'sg-789'],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        # Verify all optional parameters are included
        assert 'InstanceProfileIdentifier' in call_args
        assert 'Description' in call_args
        assert 'KmsKeyArn' in call_args
        assert 'PubliclyAccessible' in call_args
        assert 'NetworkType' in call_args
        assert 'SubnetGroupIdentifier' in call_args
        assert 'VpcSecurityGroups' in call_args

    def test_list_instance_profiles_with_marker(self, manager, mock_client):
        """Test list_instance_profiles with marker parameter."""
        mock_client.call_api.return_value = {'InstanceProfiles': []}

        result = manager.list_instance_profiles(max_results=75, marker='marker-xyz')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Marker' in call_args
        assert call_args['Marker'] == 'marker-xyz'

    def test_modify_data_migration_all_optional_params(self, manager, mock_client):
        """Test modify_data_migration with all optional parameters."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.modify_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test',
            identifier='new-migration-id',
            migration_type='cdc',
            data_migration_name='Updated Migration',
            data_migration_settings={'BatchSize': 1000},
            source_data_settings=[{'DataProviderArn': 'updated-source'}],
            number_of_jobs=8,
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        # Verify all optional parameters are included
        assert 'DataMigrationIdentifier' in call_args
        assert 'MigrationType' in call_args
        assert 'DataMigrationName' in call_args
        assert 'DataMigrationSettings' in call_args
        assert 'SourceDataSettings' in call_args
        assert 'NumberOfJobs' in call_args
        assert call_args['NumberOfJobs'] == 8

    def test_list_data_migrations_with_filters(self, manager, mock_client):
        """Test list_data_migrations with filters parameter."""
        mock_client.call_api.return_value = {'DataMigrations': []}

        filters = [{'Name': 'migration-type', 'Values': ['full-load']}]
        result = manager.list_data_migrations(filters=filters, max_results=20, marker='page2')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Filters' in call_args
        assert call_args['Filters'] == filters
        assert 'Marker' in call_args


class TestServerlessManagerEdgeCaseCoverage:
    """Test edge cases for comprehensive coverage."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create ServerlessManager instance."""
        return ServerlessManager(mock_client)

    def test_modify_migration_project_partial_optional_params(self, manager, mock_client):
        """Test modify_migration_project with some optional parameters."""
        mock_client.call_api.return_value = {'MigrationProject': {}}

        # Test with only identifier and description
        result = manager.modify_migration_project(
            arn='arn:aws:dms:us-east-1:123:migration-project:test',
            identifier='partial-id',
            description='Partial update',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'MigrationProjectIdentifier' in call_args
        assert 'Description' in call_args
        # Other optional params should not be present
        assert 'InstanceProfileArn' not in call_args
        assert 'SourceDataProviderDescriptors' not in call_args

    def test_modify_data_provider_partial_params(self, manager, mock_client):
        """Test modify_data_provider with partial optional parameters."""
        mock_client.call_api.return_value = {'DataProvider': {}}

        # Test with only settings
        result = manager.modify_data_provider(
            arn='arn:aws:dms:us-east-1:123:data-provider:test',
            settings={'ServerName': 'new-host'},
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Settings' in call_args
        assert 'DataProviderIdentifier' not in call_args
        assert 'Engine' not in call_args

    def test_modify_instance_profile_network_type_only(self, manager, mock_client):
        """Test modify_instance_profile with only network_type."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.modify_instance_profile(
            arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            network_type='IPV4',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'NetworkType' in call_args
        assert call_args['NetworkType'] == 'IPV4'

    def test_modify_instance_profile_kms_key_only(self, manager, mock_client):
        """Test modify_instance_profile with only kms_key_arn."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.modify_instance_profile(
            arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            kms_key_arn='arn:aws:kms:us-east-1:123:key/abc123',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'KmsKeyArn' in call_args

    def test_modify_instance_profile_subnet_group_only(self, manager, mock_client):
        """Test modify_instance_profile with only subnet_group_identifier."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.modify_instance_profile(
            arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            subnet_group_identifier='new-subnet-group',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'SubnetGroupIdentifier' in call_args

    def test_modify_instance_profile_vpc_security_groups_only(self, manager, mock_client):
        """Test modify_instance_profile with only vpc_security_groups."""
        mock_client.call_api.return_value = {'InstanceProfile': {}}

        result = manager.modify_instance_profile(
            arn='arn:aws:dms:us-east-1:123:instance-profile:test',
            vpc_security_groups=['sg-new1', 'sg-new2'],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'VpcSecurityGroups' in call_args

    def test_modify_data_migration_migration_type_only(self, manager, mock_client):
        """Test modify_data_migration with only migration_type."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.modify_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test',
            migration_type='full-load-and-cdc',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'MigrationType' in call_args

    def test_modify_data_migration_name_only(self, manager, mock_client):
        """Test modify_data_migration with only data_migration_name."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.modify_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test',
            data_migration_name='New Name',
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'DataMigrationName' in call_args

    def test_modify_data_migration_settings_only(self, manager, mock_client):
        """Test modify_data_migration with only data_migration_settings."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.modify_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test',
            data_migration_settings={'Timeout': 3600},
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'DataMigrationSettings' in call_args

    def test_modify_data_migration_source_settings_only(self, manager, mock_client):
        """Test modify_data_migration with only source_data_settings."""
        mock_client.call_api.return_value = {'DataMigration': {}}

        result = manager.modify_data_migration(
            arn='arn:aws:dms:us-east-1:123:data-migration:test',
            source_data_settings=[{'DataProviderArn': 'new-arn'}],
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'SourceDataSettings' in call_args
