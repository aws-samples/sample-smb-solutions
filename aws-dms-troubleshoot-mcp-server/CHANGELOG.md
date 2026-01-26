# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2026-06-01

### Changed
- Published to PyPI as `aws-dms-troubleshoot-mcp`; documentation now uses `uvx` for installation and MCP client configuration
- `get_troubleshooting_recommendations` now augments static guidance with live AWS documentation search results
- `list_replication_tasks` paginates through all results instead of truncating at 100 tasks
- boto3 clients are cached per region/profile to avoid recreating sessions on every call

### Fixed
- `analyze_endpoint` no longer issues a guaranteed-to-fail connection test; the test now runs only when a `replication_instance_arn` is supplied

## [1.0.0] - 2025-11-03

### Added
- Initial release of AWS DMS Troubleshooting MCP Server
- `list_replication_tasks` tool to list all DMS replication tasks with status filtering
- `get_replication_task_details` tool for comprehensive task information
- `get_task_cloudwatch_logs` tool to retrieve and analyze CloudWatch logs
- `analyze_endpoint` tool to validate endpoint configurations
- `diagnose_replication_issue` tool for comprehensive Root Cause Analysis (RCA)
- `get_troubleshooting_recommendations` tool for pattern-based troubleshooting guidance
- Support for post-migration troubleshooting workflows
- CloudWatch Logs integration with error pattern detection
- Endpoint configuration analysis and validation
- AWS DMS best practices recommendations
- Documentation links and troubleshooting guides
- Comprehensive test suite
- Docker support with health checks
- Full documentation and usage examples

### Features
- **Replication Task Management**: Query and monitor DMS replication tasks
- **CloudWatch Integration**: Retrieve and analyze logs with error detection
- **Endpoint Analysis**: Validate source and target configurations
- **RCA Automation**: Automated root cause analysis for failed tasks
- **Pattern Recognition**: Identify common error patterns and provide solutions
- **AWS Best Practices**: Built-in recommendations based on AWS documentation

### Documentation
- Comprehensive README with usage examples
- API reference for all tools
- Troubleshooting guide
- Docker deployment instructions
- IAM permissions requirements

### Testing
- Unit tests for all major functions
- Mock-based testing for AWS services
- Test coverage for error scenarios
- Async test support

[1.0.1]: https://pypi.org/project/aws-dms-troubleshoot-mcp/1.0.1/
[1.0.0]: https://pypi.org/project/aws-dms-troubleshoot-mcp/1.0.0/
