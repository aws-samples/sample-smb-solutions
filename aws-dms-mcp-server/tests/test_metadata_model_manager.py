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

"""Comprehensive tests for MetadataModelManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.metadata_model_manager import MetadataModelManager
from unittest.mock import Mock


class TestMetadataModelManagerConversionConfiguration:
    """Test conversion configuration operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_conversion_configuration_success(self, manager, mock_client):
        """Test successful conversion configuration retrieval."""
        mock_client.call_api.return_value = {'ConversionConfiguration': {'option': 'value'}}

        result = manager.describe_conversion_configuration('arn:test')

        assert result['success'] is True
        assert 'conversion_configuration' in result['data']
        mock_client.call_api.assert_called_once_with(
            'describe_conversion_configuration', MigrationProjectArn='arn:test'
        )

    def test_describe_conversion_configuration_empty(self, manager, mock_client):
        """Test conversion configuration retrieval with empty response."""
        mock_client.call_api.return_value = {}

        result = manager.describe_conversion_configuration('arn:test')

        assert result['success'] is True
        assert result['data']['conversion_configuration'] == {}

    def test_modify_conversion_configuration_success(self, manager, mock_client):
        """Test successful conversion configuration modification."""
        mock_client.call_api.return_value = {'ConversionConfiguration': {'option': 'new_value'}}

        config = {'option': 'new_value'}
        result = manager.modify_conversion_configuration('arn:test', config)

        assert result['success'] is True
        assert result['data']['message'] == 'Conversion configuration modified'
        mock_client.call_api.assert_called_once_with(
            'modify_conversion_configuration',
            MigrationProjectArn='arn:test',
            ConversionConfiguration=config,
        )


class TestMetadataModelManagerExtensionPack:
    """Test extension pack operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_extension_pack_associations_success(self, manager, mock_client):
        """Test successful extension pack associations listing."""
        mock_client.call_api.return_value = {
            'ExtensionPackAssociations': [
                {'AssociationId': 'assoc-1'},
                {'AssociationId': 'assoc-2'},
            ]
        }

        result = manager.describe_extension_pack_associations('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['extension_pack_associations']) == 2

    def test_describe_extension_pack_associations_with_filters(self, manager, mock_client):
        """Test extension pack associations listing with filters."""
        mock_client.call_api.return_value = {'ExtensionPackAssociations': []}

        filters = [{'Name': 'status', 'Values': ['active']}]
        result = manager.describe_extension_pack_associations(
            'arn:test', filters=filters, marker='token', max_results=50
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['Marker'] == 'token'
        assert call_args['MaxRecords'] == 50

    def test_describe_extension_pack_associations_with_pagination(self, manager, mock_client):
        """Test extension pack associations with pagination."""
        mock_client.call_api.return_value = {
            'ExtensionPackAssociations': [],
            'Marker': 'next-token',
        }

        result = manager.describe_extension_pack_associations('arn:test')

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_start_extension_pack_association_success(self, manager, mock_client):
        """Test successful extension pack association start."""
        mock_client.call_api.return_value = {
            'ExtensionPackAssociation': {'AssociationId': 'assoc-1'}
        }

        result = manager.start_extension_pack_association('arn:test')

        assert result['success'] is True
        assert result['data']['message'] == 'Extension pack association started'
        assert 'extension_pack_association' in result['data']


class TestMetadataModelManagerAssessments:
    """Test metadata model assessment operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_metadata_model_assessments_success(self, manager, mock_client):
        """Test successful assessments listing."""
        mock_client.call_api.return_value = {
            'MetadataModelAssessments': [
                {'AssessmentId': 'assessment-1'},
                {'AssessmentId': 'assessment-2'},
            ]
        }

        result = manager.describe_metadata_model_assessments('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['metadata_model_assessments']) == 2

    def test_describe_metadata_model_assessments_with_params(self, manager, mock_client):
        """Test assessments listing with all parameters."""
        mock_client.call_api.return_value = {
            'MetadataModelAssessments': [],
            'Marker': 'next-token',
        }

        filters = [{'Name': 'status', 'Values': ['completed']}]
        result = manager.describe_metadata_model_assessments(
            'arn:test', filters=filters, marker='token', max_results=25
        )

        assert result['success'] is True
        assert 'next_marker' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters

    def test_start_metadata_model_assessment_success(self, manager, mock_client):
        """Test successful assessment start."""
        mock_client.call_api.return_value = {
            'MetadataModelAssessment': {'AssessmentId': 'assessment-1'}
        }

        selection_rules = '{"rules": []}'
        result = manager.start_metadata_model_assessment('arn:test', selection_rules)

        assert result['success'] is True
        assert result['data']['message'] == 'Metadata model assessment started'
        mock_client.call_api.assert_called_once_with(
            'start_metadata_model_assessment',
            MigrationProjectArn='arn:test',
            SelectionRules=selection_rules,
        )


class TestMetadataModelManagerConversions:
    """Test metadata model conversion operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_metadata_model_conversions_success(self, manager, mock_client):
        """Test successful conversions listing."""
        mock_client.call_api.return_value = {
            'MetadataModelConversions': [
                {'ConversionId': 'conv-1'},
                {'ConversionId': 'conv-2'},
            ]
        }

        result = manager.describe_metadata_model_conversions('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['metadata_model_conversions']) == 2

    def test_describe_metadata_model_conversions_empty(self, manager, mock_client):
        """Test conversions listing with empty result."""
        mock_client.call_api.return_value = {'MetadataModelConversions': []}

        result = manager.describe_metadata_model_conversions('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 0

    def test_describe_metadata_model_conversions_with_pagination(self, manager, mock_client):
        """Test conversions listing with pagination."""
        mock_client.call_api.return_value = {
            'MetadataModelConversions': [],
            'Marker': 'next-token',
        }

        result = manager.describe_metadata_model_conversions('arn:test', marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']

    def test_start_metadata_model_conversion_success(self, manager, mock_client):
        """Test successful conversion start."""
        mock_client.call_api.return_value = {'MetadataModelConversion': {'ConversionId': 'conv-1'}}

        selection_rules = '{"rules": []}'
        result = manager.start_metadata_model_conversion('arn:test', selection_rules)

        assert result['success'] is True
        assert result['data']['message'] == 'Metadata model conversion started'
        mock_client.call_api.assert_called_once_with(
            'start_metadata_model_conversion',
            MigrationProjectArn='arn:test',
            SelectionRules=selection_rules,
        )


class TestMetadataModelManagerExportsAsScript:
    """Test metadata model export as script operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_metadata_model_exports_as_script_success(self, manager, mock_client):
        """Test successful script exports listing."""
        mock_client.call_api.return_value = {
            'MetadataModelExportsAsScript': [
                {'ExportId': 'export-1'},
                {'ExportId': 'export-2'},
            ]
        }

        result = manager.describe_metadata_model_exports_as_script('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['metadata_model_exports']) == 2

    def test_describe_metadata_model_exports_as_script_with_filters(self, manager, mock_client):
        """Test script exports listing with filters."""
        mock_client.call_api.return_value = {'MetadataModelExportsAsScript': []}

        filters = [{'Name': 'origin', 'Values': ['source']}]
        result = manager.describe_metadata_model_exports_as_script(
            'arn:test', filters=filters, max_results=10
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 10

    def test_start_metadata_model_export_as_script_success(self, manager, mock_client):
        """Test successful script export start."""
        mock_client.call_api.return_value = {
            'MetadataModelExportAsScript': {'ExportId': 'export-1'}
        }

        selection_rules = '{"rules": []}'
        result = manager.start_metadata_model_export_as_script(
            'arn:test', selection_rules, 'source'
        )

        assert result['success'] is True
        assert result['data']['message'] == 'Metadata model export as script started'
        mock_client.call_api.assert_called_once()

    def test_start_metadata_model_export_as_script_with_filename(self, manager, mock_client):
        """Test script export start with filename."""
        mock_client.call_api.return_value = {'MetadataModelExportAsScript': {}}

        selection_rules = '{"rules": []}'
        result = manager.start_metadata_model_export_as_script(
            'arn:test', selection_rules, 'source', file_name='export.sql'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['FileName'] == 'export.sql'


class TestMetadataModelManagerExportsToTarget:
    """Test metadata model export to target operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_metadata_model_exports_to_target_success(self, manager, mock_client):
        """Test successful target exports listing."""
        mock_client.call_api.return_value = {
            'MetadataModelExportsToTarget': [
                {'ExportId': 'export-1'},
                {'ExportId': 'export-2'},
            ]
        }

        result = manager.describe_metadata_model_exports_to_target('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['metadata_model_exports']) == 2

    def test_describe_metadata_model_exports_to_target_with_pagination(self, manager, mock_client):
        """Test target exports listing with pagination."""
        mock_client.call_api.return_value = {
            'MetadataModelExportsToTarget': [],
            'Marker': 'next-token',
        }

        result = manager.describe_metadata_model_exports_to_target('arn:test', marker='token')

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_start_metadata_model_export_to_target_success(self, manager, mock_client):
        """Test successful target export start."""
        mock_client.call_api.return_value = {
            'MetadataModelExportToTarget': {'ExportId': 'export-1'}
        }

        selection_rules = '{"rules": []}'
        result = manager.start_metadata_model_export_to_target('arn:test', selection_rules)

        assert result['success'] is True
        assert result['data']['message'] == 'Metadata model export to target started'

    def test_start_metadata_model_export_to_target_with_overwrite(self, manager, mock_client):
        """Test target export start with overwrite flag."""
        mock_client.call_api.return_value = {'MetadataModelExportToTarget': {}}

        selection_rules = '{"rules": []}'
        result = manager.start_metadata_model_export_to_target(
            'arn:test', selection_rules, overwrite_extension_pack=True
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['OverwriteExtensionPack'] is True


class TestMetadataModelManagerImports:
    """Test metadata model import operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_metadata_model_imports_success(self, manager, mock_client):
        """Test successful imports listing."""
        mock_client.call_api.return_value = {
            'MetadataModelImports': [{'ImportId': 'import-1'}, {'ImportId': 'import-2'}]
        }

        result = manager.describe_metadata_model_imports('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert len(result['data']['metadata_model_imports']) == 2

    def test_describe_metadata_model_imports_with_params(self, manager, mock_client):
        """Test imports listing with all parameters."""
        mock_client.call_api.return_value = {
            'MetadataModelImports': [],
            'Marker': 'next-token',
        }

        filters = [{'Name': 'origin', 'Values': ['source']}]
        result = manager.describe_metadata_model_imports(
            'arn:test', filters=filters, marker='token', max_results=20
        )

        assert result['success'] is True
        assert 'next_marker' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 20

    def test_start_metadata_model_import_success(self, manager, mock_client):
        """Test successful import start."""
        mock_client.call_api.return_value = {'MetadataModelImport': {'ImportId': 'import-1'}}

        selection_rules = '{"rules": []}'
        result = manager.start_metadata_model_import('arn:test', selection_rules, 'source')

        assert result['success'] is True
        assert result['data']['message'] == 'Metadata model import started'
        mock_client.call_api.assert_called_once_with(
            'start_metadata_model_import',
            MigrationProjectArn='arn:test',
            SelectionRules=selection_rules,
            Origin='source',
        )


class TestMetadataModelManagerExportAssessment:
    """Test metadata model assessment export operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_export_metadata_model_assessment_success(self, manager, mock_client):
        """Test successful assessment export."""
        mock_client.call_api.return_value = {
            'MetadataModelAssessmentExport': {'ExportId': 'export-1'}
        }

        selection_rules = '{"rules": []}'
        result = manager.export_metadata_model_assessment('arn:test', selection_rules)

        assert result['success'] is True
        assert result['data']['message'] == 'Metadata model assessment exported'
        mock_client.call_api.assert_called_once()

    def test_export_metadata_model_assessment_with_filename(self, manager, mock_client):
        """Test assessment export with filename."""
        mock_client.call_api.return_value = {'MetadataModelAssessmentExport': {}}

        selection_rules = '{"rules": []}'
        result = manager.export_metadata_model_assessment(
            'arn:test', selection_rules, file_name='assessment.pdf'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['FileName'] == 'assessment.pdf'

    def test_export_metadata_model_assessment_with_report_types(self, manager, mock_client):
        """Test assessment export with report types."""
        mock_client.call_api.return_value = {'MetadataModelAssessmentExport': {}}

        selection_rules = '{"rules": []}'
        report_types = ['summary', 'detailed']
        result = manager.export_metadata_model_assessment(
            'arn:test', selection_rules, assessment_report_types=report_types
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['AssessmentReportTypes'] == report_types

    def test_export_metadata_model_assessment_with_all_params(self, manager, mock_client):
        """Test assessment export with all parameters."""
        mock_client.call_api.return_value = {'MetadataModelAssessmentExport': {}}

        selection_rules = '{"rules": []}'
        report_types = ['summary']
        result = manager.export_metadata_model_assessment(
            'arn:test',
            selection_rules,
            file_name='report.pdf',
            assessment_report_types=report_types,
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['FileName'] == 'report.pdf'
        assert call_args['AssessmentReportTypes'] == report_types


class TestMetadataModelManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_conversion_configuration_api_error(self, manager, mock_client):
        """Test API error during configuration retrieval."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.describe_conversion_configuration('arn:test')

        assert 'API Error' in str(exc_info.value)

    def test_start_assessment_api_error(self, manager, mock_client):
        """Test API error during assessment start."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.start_metadata_model_assessment('arn:test', '{}')

        assert 'Network error' in str(exc_info.value)

    def test_start_conversion_api_error(self, manager, mock_client):
        """Test API error during conversion start."""
        mock_client.call_api.side_effect = Exception('Service error')

        with pytest.raises(Exception) as exc_info:
            manager.start_metadata_model_conversion('arn:test', '{}')

        assert 'Service error' in str(exc_info.value)


class TestMetadataModelManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create MetadataModelManager instance."""
        return MetadataModelManager(mock_client)

    def test_describe_assessments_empty_result(self, manager, mock_client):
        """Test assessments listing with empty result."""
        mock_client.call_api.return_value = {'MetadataModelAssessments': []}

        result = manager.describe_metadata_model_assessments('arn:test')

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['metadata_model_assessments'] == []

    def test_describe_conversions_max_results_boundary(self, manager, mock_client):
        """Test conversions listing with maximum results."""
        mock_client.call_api.return_value = {'MetadataModelConversions': []}

        result = manager.describe_metadata_model_conversions('arn:test', max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_export_assessment_without_optional_params(self, manager, mock_client):
        """Test assessment export without optional parameters."""
        mock_client.call_api.return_value = {'MetadataModelAssessmentExport': {}}

        selection_rules = '{"rules": []}'
        result = manager.export_metadata_model_assessment('arn:test', selection_rules)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'FileName' not in call_args
        assert 'AssessmentReportTypes' not in call_args
