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
#

"""Tests for config.py module."""

import pytest
from awslabs.aws_dms_mcp_server.config import DMSServerConfig
from pydantic import ValidationError


class TestDMSServerConfigDefaults:
    """Test DMSServerConfig default values."""

    def test_default_aws_region(self):
        """Test default AWS region."""
        config = DMSServerConfig()
        assert config.aws_region == 'us-east-1'

    def test_default_aws_profile(self):
        """Test default AWS profile is None."""
        config = DMSServerConfig()
        assert config.aws_profile is None

    def test_default_read_only_mode(self):
        """Test default read-only mode is False."""
        config = DMSServerConfig()
        assert config.read_only_mode is False

    def test_default_timeout(self):
        """Test default timeout is 300 seconds."""
        config = DMSServerConfig()
        assert config.default_timeout == 300

    def test_default_max_results(self):
        """Test default max_results is 100."""
        config = DMSServerConfig()
        assert config.max_results == 100

    def test_default_log_level(self):
        """Test default log level is INFO."""
        config = DMSServerConfig()
        assert config.log_level == 'INFO'

    def test_default_enable_structured_logging(self):
        """Test default structured logging is True."""
        config = DMSServerConfig()
        assert config.enable_structured_logging is True

    def test_default_enable_connection_caching(self):
        """Test default connection caching is True."""
        config = DMSServerConfig()
        assert config.enable_connection_caching is True

    def test_default_validate_table_mappings(self):
        """Test default table mappings validation is True."""
        config = DMSServerConfig()
        assert config.validate_table_mappings is True


class TestDMSServerConfigCustomValues:
    """Test DMSServerConfig with custom values."""

    def test_custom_aws_region(self):
        """Test custom AWS region."""
        config = DMSServerConfig(aws_region='us-west-2')
        assert config.aws_region == 'us-west-2'

    def test_custom_aws_profile(self):
        """Test custom AWS profile."""
        config = DMSServerConfig(aws_profile='my-profile')
        assert config.aws_profile == 'my-profile'

    def test_custom_read_only_mode(self):
        """Test custom read-only mode."""
        config = DMSServerConfig(read_only_mode=True)
        assert config.read_only_mode is True

    def test_custom_timeout(self):
        """Test custom timeout."""
        config = DMSServerConfig(default_timeout=600)
        assert config.default_timeout == 600

    def test_custom_max_results(self):
        """Test custom max_results."""
        config = DMSServerConfig(max_results=50)
        assert config.max_results == 50

    def test_custom_log_level_debug(self):
        """Test custom log level DEBUG."""
        config = DMSServerConfig(log_level='DEBUG')
        assert config.log_level == 'DEBUG'

    def test_custom_log_level_warning(self):
        """Test custom log level WARNING."""
        config = DMSServerConfig(log_level='WARNING')
        assert config.log_level == 'WARNING'

    def test_custom_log_level_error(self):
        """Test custom log level ERROR."""
        config = DMSServerConfig(log_level='ERROR')
        assert config.log_level == 'ERROR'

    def test_custom_enable_structured_logging(self):
        """Test custom structured logging disabled."""
        config = DMSServerConfig(enable_structured_logging=False)
        assert config.enable_structured_logging is False

    def test_custom_enable_connection_caching(self):
        """Test custom connection caching disabled."""
        config = DMSServerConfig(enable_connection_caching=False)
        assert config.enable_connection_caching is False

    def test_custom_validate_table_mappings(self):
        """Test custom table mappings validation disabled."""
        config = DMSServerConfig(validate_table_mappings=False)
        assert config.validate_table_mappings is False

    def test_all_custom_values(self):
        """Test config with all custom values."""
        config = DMSServerConfig(
            aws_region='eu-west-1',
            aws_profile='test-profile',
            read_only_mode=True,
            default_timeout=450,
            max_results=75,
            log_level='DEBUG',
            enable_structured_logging=False,
            enable_connection_caching=False,
            validate_table_mappings=False,
        )
        assert config.aws_region == 'eu-west-1'
        assert config.aws_profile == 'test-profile'
        assert config.read_only_mode is True
        assert config.default_timeout == 450
        assert config.max_results == 75
        assert config.log_level == 'DEBUG'
        assert config.enable_structured_logging is False
        assert config.enable_connection_caching is False
        assert config.validate_table_mappings is False


class TestRegionValidator:
    """Test AWS region validation."""

    def test_valid_regions(self):
        """Test all valid regions."""
        valid_regions = [
            'us-east-1',
            'us-east-2',
            'us-west-1',
            'us-west-2',
            'eu-west-1',
            'eu-west-2',
            'eu-central-1',
            'ap-southeast-1',
            'ap-southeast-2',
            'ap-northeast-1',
            'sa-east-1',
            'ca-central-1',
        ]
        for region in valid_regions:
            config = DMSServerConfig(aws_region=region)
            assert config.aws_region == region

    def test_invalid_region_format(self):
        """Test validation error for invalid region format."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(aws_region='invalid-region')
        assert 'Invalid AWS region' in str(exc_info.value)

    def test_invalid_region_empty(self):
        """Test validation error for empty region."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(aws_region='')
        assert 'Invalid AWS region' in str(exc_info.value)

    def test_invalid_region_wrong_format(self):
        """Test validation error for wrong region format."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(aws_region='us_east_1')
        assert 'Invalid AWS region' in str(exc_info.value)

    def test_invalid_region_partial(self):
        """Test validation error for partial region name."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(aws_region='us-east')
        assert 'Invalid AWS region' in str(exc_info.value)


class TestTimeoutValidator:
    """Test timeout field validation."""

    def test_valid_timeout_minimum(self):
        """Test minimum valid timeout (30 seconds)."""
        config = DMSServerConfig(default_timeout=30)
        assert config.default_timeout == 30

    def test_valid_timeout_maximum(self):
        """Test maximum valid timeout (3600 seconds)."""
        config = DMSServerConfig(default_timeout=3600)
        assert config.default_timeout == 3600

    def test_valid_timeout_middle(self):
        """Test valid timeout in middle range."""
        config = DMSServerConfig(default_timeout=300)
        assert config.default_timeout == 300

    def test_invalid_timeout_too_low(self):
        """Test validation error for timeout below minimum."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(default_timeout=29)
        assert 'greater than or equal to 30' in str(exc_info.value)

    def test_invalid_timeout_too_high(self):
        """Test validation error for timeout above maximum."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(default_timeout=3601)
        assert 'less than or equal to 3600' in str(exc_info.value)

    def test_invalid_timeout_zero(self):
        """Test validation error for timeout of zero."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(default_timeout=0)
        assert 'greater than or equal to 30' in str(exc_info.value)

    def test_invalid_timeout_negative(self):
        """Test validation error for negative timeout."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(default_timeout=-100)
        assert 'greater than or equal to 30' in str(exc_info.value)


class TestMaxResultsValidator:
    """Test max_results field validation."""

    def test_valid_max_results_minimum(self):
        """Test minimum valid max_results (1)."""
        config = DMSServerConfig(max_results=1)
        assert config.max_results == 1

    def test_valid_max_results_maximum(self):
        """Test maximum valid max_results (100)."""
        config = DMSServerConfig(max_results=100)
        assert config.max_results == 100

    def test_valid_max_results_middle(self):
        """Test valid max_results in middle range."""
        config = DMSServerConfig(max_results=50)
        assert config.max_results == 50

    def test_invalid_max_results_too_low(self):
        """Test validation error for max_results below minimum."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(max_results=0)
        assert 'greater than or equal to 1' in str(exc_info.value)

    def test_invalid_max_results_too_high(self):
        """Test validation error for max_results above maximum."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(max_results=101)
        assert 'less than or equal to 100' in str(exc_info.value)

    def test_invalid_max_results_negative(self):
        """Test validation error for negative max_results."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(max_results=-5)
        assert 'greater than or equal to 1' in str(exc_info.value)


class TestLogLevelValidator:
    """Test log_level field validation."""

    def test_valid_log_levels(self):
        """Test all valid log levels."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']
        for level in valid_levels:
            config = DMSServerConfig(log_level=level)
            assert config.log_level == level

    def test_invalid_log_level(self):
        """Test validation error for invalid log level."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(log_level='INVALID')
        assert 'Input should be' in str(exc_info.value)

    def test_invalid_log_level_lowercase(self):
        """Test validation error for lowercase log level."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(log_level='debug')
        assert 'Input should be' in str(exc_info.value)

    def test_invalid_log_level_mixed_case(self):
        """Test validation error for mixed case log level."""
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig(log_level='Debug')
        assert 'Input should be' in str(exc_info.value)


class TestEnvironmentVariableLoading:
    """Test configuration loading from environment variables."""

    def test_env_aws_region(self, monkeypatch):
        """Test loading AWS region from environment."""
        monkeypatch.setenv('DMS_AWS_REGION', 'eu-west-1')
        config = DMSServerConfig()
        assert config.aws_region == 'eu-west-1'

    def test_env_aws_profile(self, monkeypatch):
        """Test loading AWS profile from environment."""
        monkeypatch.setenv('DMS_AWS_PROFILE', 'test-profile')
        config = DMSServerConfig()
        assert config.aws_profile == 'test-profile'

    def test_env_read_only_mode(self, monkeypatch):
        """Test loading read-only mode from environment."""
        monkeypatch.setenv('DMS_READ_ONLY_MODE', 'true')
        config = DMSServerConfig()
        assert config.read_only_mode is True

    def test_env_default_timeout(self, monkeypatch):
        """Test loading timeout from environment."""
        monkeypatch.setenv('DMS_DEFAULT_TIMEOUT', '600')
        config = DMSServerConfig()
        assert config.default_timeout == 600

    def test_env_max_results(self, monkeypatch):
        """Test loading max_results from environment."""
        monkeypatch.setenv('DMS_MAX_RESULTS', '50')
        config = DMSServerConfig()
        assert config.max_results == 50

    def test_env_log_level(self, monkeypatch):
        """Test loading log level from environment."""
        monkeypatch.setenv('DMS_LOG_LEVEL', 'DEBUG')
        config = DMSServerConfig()
        assert config.log_level == 'DEBUG'

    def test_env_enable_structured_logging(self, monkeypatch):
        """Test loading structured logging from environment."""
        monkeypatch.setenv('DMS_ENABLE_STRUCTURED_LOGGING', 'false')
        config = DMSServerConfig()
        assert config.enable_structured_logging is False

    def test_env_enable_connection_caching(self, monkeypatch):
        """Test loading connection caching from environment."""
        monkeypatch.setenv('DMS_ENABLE_CONNECTION_CACHING', 'false')
        config = DMSServerConfig()
        assert config.enable_connection_caching is False

    def test_env_validate_table_mappings(self, monkeypatch):
        """Test loading table mappings validation from environment."""
        monkeypatch.setenv('DMS_VALIDATE_TABLE_MAPPINGS', 'false')
        config = DMSServerConfig()
        assert config.validate_table_mappings is False

    def test_env_case_insensitive(self, monkeypatch):
        """Test that environment variables are case-insensitive."""
        monkeypatch.setenv('dms_aws_region', 'us-west-2')
        config = DMSServerConfig()
        assert config.aws_region == 'us-west-2'

    def test_env_override_defaults(self, monkeypatch):
        """Test that environment variables override defaults."""
        monkeypatch.setenv('DMS_AWS_REGION', 'ap-southeast-1')
        monkeypatch.setenv('DMS_READ_ONLY_MODE', 'true')
        config = DMSServerConfig()
        assert config.aws_region == 'ap-southeast-1'
        assert config.read_only_mode is True

    def test_env_with_explicit_values(self, monkeypatch):
        """Test that explicit values override environment variables."""
        monkeypatch.setenv('DMS_AWS_REGION', 'us-west-2')
        config = DMSServerConfig(aws_region='eu-west-1')
        assert config.aws_region == 'eu-west-1'

    def test_env_invalid_value_validation(self, monkeypatch):
        """Test that invalid environment values trigger validation errors."""
        monkeypatch.setenv('DMS_DEFAULT_TIMEOUT', '10')
        with pytest.raises(ValidationError) as exc_info:
            DMSServerConfig()
        assert 'greater than or equal to 30' in str(exc_info.value)


class TestConfigModelBehavior:
    """Test Pydantic model configuration behavior."""

    def test_extra_fields_ignored(self):
        """Test that extra fields are ignored."""
        # Should not raise an error
        config = DMSServerConfig(unknown_field='value')
        assert not hasattr(config, 'unknown_field')

    def test_validate_assignment(self):
        """Test that validate_assignment is enabled."""
        config = DMSServerConfig()
        # Should validate on assignment
        with pytest.raises(ValidationError):
            config.default_timeout = 10

    def test_config_immutability_attempt(self):
        """Test that changing validated fields triggers validation."""
        config = DMSServerConfig(default_timeout=300)
        assert config.default_timeout == 300

        # Valid assignment should work
        config.default_timeout = 600
        assert config.default_timeout == 600

        # Invalid assignment should fail
        with pytest.raises(ValidationError):
            config.default_timeout = 10


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_none_for_optional_profile(self):
        """Test that None is accepted for optional aws_profile."""
        config = DMSServerConfig(aws_profile=None)
        assert config.aws_profile is None

    def test_boundary_timeout_values(self):
        """Test boundary values for timeout."""
        # Minimum boundary
        config_min = DMSServerConfig(default_timeout=30)
        assert config_min.default_timeout == 30

        # Maximum boundary
        config_max = DMSServerConfig(default_timeout=3600)
        assert config_max.default_timeout == 3600

        # Just inside boundaries
        config_min_plus = DMSServerConfig(default_timeout=31)
        assert config_min_plus.default_timeout == 31

        config_max_minus = DMSServerConfig(default_timeout=3599)
        assert config_max_minus.default_timeout == 3599

    def test_boundary_max_results_values(self):
        """Test boundary values for max_results."""
        # Minimum boundary
        config_min = DMSServerConfig(max_results=1)
        assert config_min.max_results == 1

        # Maximum boundary
        config_max = DMSServerConfig(max_results=100)
        assert config_max.max_results == 100

        # Just inside boundaries
        config_min_plus = DMSServerConfig(max_results=2)
        assert config_min_plus.max_results == 2

        config_max_minus = DMSServerConfig(max_results=99)
        assert config_max_minus.max_results == 99

    def test_boolean_string_conversion(self, monkeypatch):
        """Test that boolean strings are converted properly."""
        monkeypatch.setenv('DMS_READ_ONLY_MODE', 'True')
        config = DMSServerConfig()
        assert config.read_only_mode is True

    def test_integer_string_conversion(self, monkeypatch):
        """Test that integer strings are converted properly."""
        monkeypatch.setenv('DMS_DEFAULT_TIMEOUT', '450')
        config = DMSServerConfig()
        assert config.default_timeout == 450


class TestTypeValidation:
    """Test type validation for configuration fields."""

    def test_invalid_region_type(self):
        """Test validation error for non-string region."""
        with pytest.raises(ValidationError):
            DMSServerConfig(aws_region=123)

    def test_invalid_timeout_type(self):
        """Test validation error for non-integer timeout."""
        with pytest.raises(ValidationError):
            DMSServerConfig(default_timeout='invalid')

    def test_invalid_max_results_type(self):
        """Test validation error for non-integer max_results."""
        with pytest.raises(ValidationError):
            DMSServerConfig(max_results='invalid')

    def test_invalid_read_only_mode_type(self):
        """Test validation error for non-boolean read_only_mode."""
        with pytest.raises(ValidationError):
            DMSServerConfig(read_only_mode=['invalid'])

    def test_invalid_log_level_type(self):
        """Test validation error for non-string log_level."""
        with pytest.raises(ValidationError):
            DMSServerConfig(log_level=123)
