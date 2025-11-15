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

"""Comprehensive tests for CertificateManager module."""

import pytest
from awslabs.aws_dms_mcp_server.utils.certificate_manager import CertificateManager
from unittest.mock import Mock


class TestCertificateManagerImportOperations:
    """Test certificate import operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create CertificateManager instance."""
        return CertificateManager(mock_client)

    def test_import_certificate_with_pem(self, manager, mock_client):
        """Test importing certificate with PEM data."""
        mock_client.call_api.return_value = {
            'Certificate': {
                'CertificateIdentifier': 'test-cert',
                'CertificateArn': 'arn:aws:dms:us-east-1:123:cert:test',
            }
        }

        pem_data = """-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQCKz...
-----END CERTIFICATE-----"""

        result = manager.import_certificate('test-cert', certificate_pem=pem_data)

        assert result['success'] is True
        assert result['data']['message'] == 'Certificate imported successfully'
        assert 'certificate' in result['data']
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CertificatePem'] == pem_data

    def test_import_certificate_with_wallet(self, manager, mock_client):
        """Test importing certificate with Oracle wallet."""
        mock_client.call_api.return_value = {'Certificate': {'CertificateIdentifier': 'test-cert'}}

        wallet_data = b'binary wallet data'
        result = manager.import_certificate('test-cert', certificate_wallet=wallet_data)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CertificateWallet'] == wallet_data

    def test_import_certificate_with_tags(self, manager, mock_client):
        """Test importing certificate with tags."""
        mock_client.call_api.return_value = {'Certificate': {}}

        tags = [{'Key': 'Environment', 'Value': 'Production'}, {'Key': 'Owner', 'Value': 'Team'}]
        result = manager.import_certificate('test-cert', certificate_pem='pem-data', tags=tags)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Tags'] == tags

    def test_import_certificate_without_tags(self, manager, mock_client):
        """Test importing certificate without tags."""
        mock_client.call_api.return_value = {'Certificate': {}}

        result = manager.import_certificate('test-cert', certificate_pem='pem-data')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'Tags' not in call_args

    def test_import_certificate_with_both_pem_and_wallet(self, manager, mock_client):
        """Test importing certificate with both PEM and wallet."""
        mock_client.call_api.return_value = {'Certificate': {}}

        result = manager.import_certificate(
            'test-cert', certificate_pem='pem-data', certificate_wallet=b'wallet-data'
        )

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert 'CertificatePem' in call_args
        assert 'CertificateWallet' in call_args

    def test_import_certificate_identifier_only(self, manager, mock_client):
        """Test importing certificate with identifier only."""
        mock_client.call_api.return_value = {'Certificate': {}}

        result = manager.import_certificate('test-cert')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CertificateIdentifier'] == 'test-cert'
        assert 'CertificatePem' not in call_args
        assert 'CertificateWallet' not in call_args


class TestCertificateManagerListOperations:
    """Test certificate listing operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create CertificateManager instance."""
        return CertificateManager(mock_client)

    def test_list_certificates_success(self, manager, mock_client):
        """Test successful certificate listing."""
        mock_client.call_api.return_value = {
            'Certificates': [
                {'CertificateIdentifier': 'cert-1', 'CertificateArn': 'arn:1'},
                {'CertificateIdentifier': 'cert-2', 'CertificateArn': 'arn:2'},
            ]
        }

        result = manager.list_certificates()

        assert result['success'] is True
        assert result['data']['count'] == 2
        assert 'certificates' in result['data']

    def test_list_certificates_with_filters(self, manager, mock_client):
        """Test listing certificates with filters."""
        mock_client.call_api.return_value = {'Certificates': []}

        filters = [{'Name': 'certificate-id', 'Values': ['test-cert']}]
        result = manager.list_certificates(filters=filters, max_results=50, marker='token')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['Filters'] == filters
        assert call_args['MaxRecords'] == 50
        assert call_args['Marker'] == 'token'

    def test_list_certificates_with_pagination(self, manager, mock_client):
        """Test listing certificates with pagination."""
        mock_client.call_api.return_value = {'Certificates': [], 'Marker': 'next-token'}

        result = manager.list_certificates()

        assert result['success'] is True
        assert 'next_marker' in result['data']
        assert result['data']['next_marker'] == 'next-token'

    def test_list_certificates_empty(self, manager, mock_client):
        """Test listing certificates with empty result."""
        mock_client.call_api.return_value = {'Certificates': []}

        result = manager.list_certificates()

        assert result['success'] is True
        assert result['data']['count'] == 0
        assert result['data']['certificates'] == []

    def test_list_certificates_without_optional_params(self, manager, mock_client):
        """Test listing certificates without optional parameters."""
        mock_client.call_api.return_value = {'Certificates': []}

        result = manager.list_certificates()

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 100
        assert 'Filters' not in call_args
        assert 'Marker' not in call_args


class TestCertificateManagerDeleteOperations:
    """Test certificate deletion operations."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create CertificateManager instance."""
        return CertificateManager(mock_client)

    def test_delete_certificate_success(self, manager, mock_client):
        """Test successful certificate deletion."""
        mock_client.call_api.return_value = {
            'Certificate': {
                'CertificateIdentifier': 'test-cert',
                'CertificateArn': 'arn:aws:dms:us-east-1:123:cert:test',
            }
        }

        result = manager.delete_certificate('arn:aws:dms:us-east-1:123:cert:test')

        assert result['success'] is True
        assert result['data']['message'] == 'Certificate deleted successfully'
        assert 'certificate' in result['data']
        mock_client.call_api.assert_called_once_with(
            'delete_certificate', CertificateArn='arn:aws:dms:us-east-1:123:cert:test'
        )

    def test_delete_certificate_returns_empty_dict(self, manager, mock_client):
        """Test certificate deletion with empty response."""
        mock_client.call_api.return_value = {'Certificate': {}}

        result = manager.delete_certificate('arn:aws:dms:us-east-1:123:cert:test')

        assert result['success'] is True
        assert result['data']['certificate'] == {}


class TestCertificateManagerErrorHandling:
    """Test error handling."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create CertificateManager instance."""
        return CertificateManager(mock_client)

    def test_import_certificate_api_error(self, manager, mock_client):
        """Test API error during certificate import."""
        mock_client.call_api.side_effect = Exception('API Error')

        with pytest.raises(Exception) as exc_info:
            manager.import_certificate('test-cert', certificate_pem='pem-data')

        assert 'API Error' in str(exc_info.value)

    def test_list_certificates_api_error(self, manager, mock_client):
        """Test API error during certificate listing."""
        mock_client.call_api.side_effect = Exception('Network error')

        with pytest.raises(Exception) as exc_info:
            manager.list_certificates()

        assert 'Network error' in str(exc_info.value)

    def test_delete_certificate_api_error(self, manager, mock_client):
        """Test API error during certificate deletion."""
        mock_client.call_api.side_effect = Exception('Delete failed')

        with pytest.raises(Exception) as exc_info:
            manager.delete_certificate('arn:cert')

        assert 'Delete failed' in str(exc_info.value)


class TestCertificateManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def mock_client(self):
        """Create mock DMS client."""
        return Mock()

    @pytest.fixture
    def manager(self, mock_client):
        """Create CertificateManager instance."""
        return CertificateManager(mock_client)

    def test_import_certificate_long_pem(self, manager, mock_client):
        """Test importing certificate with long PEM data."""
        mock_client.call_api.return_value = {'Certificate': {}}

        long_pem = '-----BEGIN CERTIFICATE-----\n' + 'A' * 10000 + '\n-----END CERTIFICATE-----'
        result = manager.import_certificate('test-cert', certificate_pem=long_pem)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['CertificatePem']) > 10000

    def test_import_certificate_large_wallet(self, manager, mock_client):
        """Test importing certificate with large wallet data."""
        mock_client.call_api.return_value = {'Certificate': {}}

        large_wallet = b'X' * 50000
        result = manager.import_certificate('test-cert', certificate_wallet=large_wallet)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['CertificateWallet']) == 50000

    def test_list_certificates_with_max_results_boundary(self, manager, mock_client):
        """Test list certificates with maximum results."""
        mock_client.call_api.return_value = {'Certificates': []}

        result = manager.list_certificates(max_results=1000)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['MaxRecords'] == 1000

    def test_list_certificates_multiple_pages(self, manager, mock_client):
        """Test listing certificates across multiple pages."""
        # First call
        mock_client.call_api.return_value = {
            'Certificates': [{'CertificateIdentifier': 'cert-1'}],
            'Marker': 'token-1',
        }

        result1 = manager.list_certificates()

        assert result1['success'] is True
        assert result1['data']['count'] == 1
        assert result1['data']['next_marker'] == 'token-1'

        # Second call with token
        mock_client.call_api.return_value = {'Certificates': [{'CertificateIdentifier': 'cert-2'}]}

        result2 = manager.list_certificates(marker='token-1')

        assert result2['success'] is True
        assert result2['data']['count'] == 1
        assert 'next_marker' not in result2['data']

    def test_import_certificate_with_multiple_tags(self, manager, mock_client):
        """Test importing certificate with multiple tags."""
        mock_client.call_api.return_value = {'Certificate': {}}

        tags = [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Owner', 'Value': 'Team'},
            {'Key': 'Project', 'Value': 'DMS'},
            {'Key': 'CostCenter', 'Value': 'Engineering'},
        ]
        result = manager.import_certificate('test-cert', certificate_pem='pem', tags=tags)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['Tags']) == 4

    def test_delete_certificate_with_special_characters(self, manager, mock_client):
        """Test deleting certificate with special characters in ARN."""
        mock_client.call_api.return_value = {'Certificate': {}}

        arn = 'arn:aws:dms:us-east-1:123456789012:cert:test-cert_123'
        result = manager.delete_certificate(arn)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CertificateArn'] == arn

    def test_list_certificates_with_multiple_filters(self, manager, mock_client):
        """Test listing certificates with multiple filters."""
        mock_client.call_api.return_value = {'Certificates': []}

        filters = [
            {'Name': 'certificate-id', 'Values': ['cert-1', 'cert-2']},
            {'Name': 'certificate-owner', 'Values': ['team-a']},
        ]
        result = manager.list_certificates(filters=filters)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert len(call_args['Filters']) == 2

    def test_import_certificate_empty_identifier(self, manager, mock_client):
        """Test importing certificate with empty identifier."""
        mock_client.call_api.return_value = {'Certificate': {}}

        result = manager.import_certificate('', certificate_pem='pem-data')

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert call_args['CertificateIdentifier'] == ''

    def test_import_certificate_with_multiline_pem(self, manager, mock_client):
        """Test importing certificate with properly formatted multiline PEM."""
        mock_client.call_api.return_value = {'Certificate': {}}

        pem_data = """-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKpvR4O8JTp2MA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRAwDgYDVQQHDAdTYW4gSm9zZTEP
MA0GA1UECgwGQW1hem9uMB4XDTE5MDExMDAwMDAwMFoXDTI5MDExMDAwMDAwMFow
-----END CERTIFICATE-----"""

        result = manager.import_certificate('test-cert', certificate_pem=pem_data)

        assert result['success'] is True
        call_args = mock_client.call_api.call_args[1]
        assert '-----BEGIN CERTIFICATE-----' in call_args['CertificatePem']
        assert '-----END CERTIFICATE-----' in call_args['CertificatePem']

    def test_delete_certificate_empty_arn(self, manager, mock_client):
        """Test deleting certificate with empty ARN."""
        mock_client.call_api.return_value = {'Certificate': {}}

        result = manager.delete_certificate('')

        assert result['success'] is True
