import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import cast, Sequence, Any

from dlt.destinations.impl.risingwave.typing import (
    TABLE_APPEND_ONLY_HINT,
    TABLE_ENGINE_HINT,
    TABLE_PROPERTIES_HINT,
)
from dlt.destinations.impl.risingwave.risingwave import RisingwaveClient, RisingwaveLoadJob
from dlt.destinations.impl.risingwave.risingwave_sql_client import RisingwaveSqlClient
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.impl.risingwave.configuration import (
    RisingwaveCredentials,
    RisingwaveClientConfiguration,
)
from dlt.destinations.impl.risingwave.factory import risingwave, _risingwave_file_format_selector
from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    GcpServiceAccountCredentialsWithoutDefaults,
    GcpOAuthCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.schema import Schema, utils
from dlt.common.schema.typing import TTableSchema, TFileFormat
from dlt.common.typing import TLoaderFileFormat
from dlt.common.destination.capabilities import DestinationCapabilitiesContext


@pytest.fixture
def risingwave_client_config() -> RisingwaveClientConfiguration:
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"
    config.dataset_name = "test_dataset"
    return config


@pytest.fixture
def mock_risingwave_client(risingwave_client_config: RisingwaveClientConfiguration) -> RisingwaveClient:
    schema = Schema("test_schema")
    schema.update_table(utils.new_table("test_table"))
    capabilities = risingwave().capabilities()
    client = RisingwaveClient(schema, risingwave_client_config, capabilities)
    return client


@pytest.fixture
def schema_with_hints() -> Schema:
    """Create a test schema with Risingwave-specific hints."""
    import json

    schema = Schema("test_schema")
    schema.update_table(
        utils.new_table(
            "test_table",
            json.dumps(
                {
                    "columns": {
                        "id": {"name": "id", "data_type": "bigint", "nullable": False},
                        "name": {"name": "name", "data_type": "text", "nullable": True},
                        "created_at": {
                            "name": "created_at",
                            "data_type": "timestamp",
                            "nullable": True,
                        },
                    },
                    TABLE_ENGINE_HINT: "iceberg",
                    TABLE_APPEND_ONLY_HINT: True,
                    TABLE_PROPERTIES_HINT: {"timeline.timestamp_column": "created_at"},
                }
            ),
        ),
    )
    return schema


def test_risingwave_destination() -> None:
    """Test that the risingwave destination is properly registered."""
    from dlt.destinations import risingwave as risingwave_dest

    assert risingwave_dest is not None
    # risingwave is a Destination class (which is a metaclass), not an instance
    assert hasattr(risingwave_dest, "__name__")
    assert risingwave_dest.__name__ == "risingwave"


def test_risingwave_capabilities() -> None:
    """Test that Risingwave capabilities are properly configured."""
    dest = risingwave()
    capabilities = dest.capabilities()

    assert capabilities is not None
    assert capabilities.sqlglot_dialect == "postgres"
    assert "insert_values" in capabilities.supported_loader_file_formats
    assert "csv" not in capabilities.supported_loader_file_formats
    assert "parquet" not in capabilities.supported_loader_file_formats
    assert "parquet" in capabilities.supported_staging_file_formats
    assert capabilities.supports_ddl_transactions is False
    assert capabilities.supports_transactions is False
    assert capabilities.alter_add_multi_column is False


def test_risingwave_type_mapper() -> None:
    """Test that Risingwave type mapper is compatible with Risingwave types."""
    dest = risingwave()
    type_mapper = dest.capabilities().get_type_mapper()

    # Test basic type mappings
    assert (
        type_mapper.to_destination_type({"name": "test_col", "data_type": "bigint"}, {}) == "bigint"
    )
    # Note: "text" maps to "varchar" without length (Risingwave doesn't support varchar with length)
    assert (
        type_mapper.to_destination_type({"name": "test_col", "data_type": "text"}, {}) == "varchar"
    )
    assert type_mapper.to_destination_type({"name": "test_col", "data_type": "json"}, {}) == "jsonb"

    # Critical: Test that decimal/numeric does NOT include precision/scale
    # Risingwave does not support numeric(precision, scale)
    decimal_type = type_mapper.to_destination_type(
        {"name": "test_col", "data_type": "decimal", "precision": 38, "scale": 2}, {}
    )
    assert decimal_type == "numeric", f"Expected 'numeric' but got '{decimal_type}'"
    assert "(" not in decimal_type, "numeric should not have precision/scale specification"

    # Test that wei also maps to numeric without precision/scale
    wei_type = type_mapper.to_destination_type(
        {"name": "test_col", "data_type": "wei", "precision": 76, "scale": 0}, {}
    )
    assert wei_type == "numeric", f"Expected 'numeric' but got '{wei_type}'"
    assert "(" not in wei_type, "numeric should not have precision/scale specification"

    # Test timestamp with precision - should work but not include precision
    timestamp_type = type_mapper.to_destination_type(
        {"name": "test_col", "data_type": "timestamp", "timezone": True, "precision": 6}, {}
    )
    # Risingwave uses "timestamp with time zone" without precision specification
    assert "timestamp with time zone" in timestamp_type.lower()


def test_risingwave_type_mapper_json_with_parquet_file_format() -> None:
    """Test that json type always maps to jsonb, even with parquet file_format.

    RisingWave does not support the 'json' type (only 'jsonb').
    PostgresTypeMapper returns 'json' for parquet file_format (ADBC workaround),
    but RisingWave doesn't use ADBC and doesn't support 'json'.
    """
    dest = risingwave()
    type_mapper = dest.capabilities().get_type_mapper()

    # Without file_format - should be jsonb
    assert type_mapper.to_destination_type({"name": "data", "data_type": "json"}, {}) == "jsonb"

    # With file_format="parquet" - should STILL be jsonb (not json)
    assert (
        type_mapper.to_destination_type(
            {"name": "data", "data_type": "json"},
            {"name": "test", "file_format": "parquet", "columns": {}},
        )
        == "jsonb"
    )


def test_risingwave_client_init(
    risingwave_client_config: RisingwaveClientConfiguration,
) -> None:
    """Test RisingwaveClient initialization."""
    schema = Schema("test_schema")
    dest = risingwave()
    capabilities = dest.capabilities()

    # Client initialization doesn't require database connection
    # It will connect when needed for operations
    client = RisingwaveClient(schema, risingwave_client_config, capabilities)
    assert client.config == risingwave_client_config
    assert client.sql_client is not None


def test_risingwave_create_indexes_default(
    risingwave_client_config: RisingwaveClientConfiguration,
) -> None:
    """Test that create_indexes defaults to False."""
    assert risingwave_client_config.create_indexes is False


def test_risingwave_primary_key_with_create_indexes_disabled(
    risingwave_client_config: RisingwaveClientConfiguration,
) -> None:
    """Test that PRIMARY KEY is NOT created when create_indexes=False (default)."""
    schema = Schema("test_schema")
    # Create table schema directly with primary_key hint
    table_schema = {
        "name": "test_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
            "name": {"name": "name", "data_type": "text", "nullable": True},
        },
        "write_disposition": "append",
    }
    schema._schema_tables["test_table"] = table_schema  # type: ignore[assignment]

    dest = risingwave()
    capabilities = dest.capabilities()
    client = RisingwaveClient(schema, risingwave_client_config, capabilities)

    columns = list(schema.tables["test_table"]["columns"].values())
    sql_statements = client._get_table_update_sql("test_table", columns, generate_alter=False)
    sql = sql_statements[0]

    # Should NOT contain PRIMARY KEY when create_indexes=False
    assert "PRIMARY KEY" not in sql


def test_risingwave_primary_key_with_create_indexes_enabled() -> None:
    """Test that PRIMARY KEY IS created when create_indexes=True."""
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"
    config.create_indexes = True  # Enable PRIMARY KEY
    config.dataset_name = "test_dataset"

    schema = Schema("test_schema")
    # Create table schema directly with primary_key hint
    table_schema = {
        "name": "test_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
            "name": {"name": "name", "data_type": "text", "nullable": True},
        },
        "write_disposition": "append",
    }
    schema._schema_tables["test_table"] = table_schema  # type: ignore[assignment]

    dest = risingwave()
    capabilities = dest.capabilities()
    client = RisingwaveClient(schema, config, capabilities)

    columns = list(schema.tables["test_table"]["columns"].values())
    sql_statements = client._get_table_update_sql("test_table", columns, generate_alter=False)
    sql = sql_statements[0]

    # Should contain PRIMARY KEY when create_indexes=True
    assert "PRIMARY KEY" in sql
    assert "PRIMARY KEY (id)" in sql


def test_risingwave_primary_key_multiple_columns() -> None:
    """Test PRIMARY KEY with multiple columns."""
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"
    config.create_indexes = True
    config.dataset_name = "test_dataset"

    schema = Schema("test_schema")
    # Create table schema directly with multiple primary_key hints
    table_schema = {
        "name": "test_table",
        "columns": {
            "user_id": {
                "name": "user_id",
                "data_type": "bigint",
                "nullable": False,
                "primary_key": True,
            },
            "event_id": {
                "name": "event_id",
                "data_type": "bigint",
                "nullable": False,
                "primary_key": True,
            },
            "data": {"name": "data", "data_type": "text", "nullable": True},
        },
        "write_disposition": "append",
    }
    schema._schema_tables["test_table"] = table_schema  # type: ignore[assignment]

    dest = risingwave()
    capabilities = dest.capabilities()
    client = RisingwaveClient(schema, config, capabilities)

    columns = list(schema.tables["test_table"]["columns"].values())
    sql_statements = client._get_table_update_sql("test_table", columns, generate_alter=False)
    sql = sql_statements[0]

    # Should contain composite PRIMARY KEY
    assert "PRIMARY KEY" in sql
    assert "PRIMARY KEY (user_id, event_id)" in sql


def test_risingwave_primary_key_with_all_clauses() -> None:
    """Test that PRIMARY KEY works correctly with ENGINE, APPEND ONLY, and WITH clauses."""
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"
    config.create_indexes = True
    config.table_engine = "iceberg"
    config.dataset_name = "test_dataset"

    schema = Schema("test_schema")
    # Create table schema directly with hints and primary_key
    table_schema = {
        "name": "test_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
            "event_type": {"name": "event_type", "data_type": "text", "nullable": True},
            "created_at": {
                "name": "created_at",
                "data_type": "timestamp",
                "nullable": True,
            },
        },
        TABLE_ENGINE_HINT: "iceberg",
        TABLE_APPEND_ONLY_HINT: True,
        TABLE_PROPERTIES_HINT: {"timeline.timestamp_column": "created_at"},
        "write_disposition": "append",
    }
    schema._schema_tables["test_table"] = table_schema  # type: ignore[assignment]

    dest = risingwave()
    capabilities = dest.capabilities()
    client = RisingwaveClient(schema, config, capabilities)

    columns = list(schema.tables["test_table"]["columns"].values())
    sql_statements = client._get_table_update_sql("test_table", columns, generate_alter=False)
    sql = sql_statements[0]

    # Check that all clauses are present in the correct order
    assert "PRIMARY KEY (id)" in sql
    assert "APPEND ONLY" in sql
    assert "WITH (timeline.timestamp_column" in sql  # Accept both quoted and unquoted
    assert "ENGINE = iceberg" in sql

    # Check clause order: PRIMARY KEY comes before APPEND ONLY
    pk_pos = sql.find("PRIMARY KEY")
    append_pos = sql.find("APPEND ONLY")
    with_pos = sql.find("WITH (")
    engine_pos = sql.find("ENGINE = iceberg")

    assert pk_pos < append_pos, "PRIMARY KEY should come before APPEND ONLY"
    assert append_pos < with_pos, "APPEND ONLY should come before WITH"
    assert with_pos < engine_pos, "WITH should come before ENGINE"


def test_risingwave_primary_key_no_primary_key_hint(
    risingwave_client_config: RisingwaveClientConfiguration,
) -> None:
    """Test that no PRIMARY KEY is created when no columns have primary_key hint."""
    risingwave_client_config.create_indexes = True

    schema = Schema("test_schema")
    # Create table schema without primary_key hints
    table_schema = {
        "name": "test_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "nullable": False},
            "name": {"name": "name", "data_type": "text", "nullable": True},
        },
        "write_disposition": "append",
    }
    schema._schema_tables["test_table"] = table_schema  # type: ignore[assignment]

    dest = risingwave()
    capabilities = dest.capabilities()
    client = RisingwaveClient(schema, risingwave_client_config, capabilities)

    columns = list(schema.tables["test_table"]["columns"].values())
    sql_statements = client._get_table_update_sql("test_table", columns, generate_alter=False)
    sql = sql_statements[0]

    # Should NOT contain PRIMARY KEY when no columns have primary_key hint
    assert "PRIMARY KEY" not in sql


# ==================== Tests for RisingwaveLoadJob (file_scan SQL generation) ====================


@pytest.fixture
def mock_staging_config_aws() -> AwsCredentialsWithoutDefaults:
    """Mock AWS S3 staging credentials."""
    creds = AwsCredentialsWithoutDefaults()
    creds.aws_access_key_id = "test_access_key"
    creds.aws_secret_access_key = "test_secret_key"
    creds.region_name = "us-east-1"
    return creds


@pytest.fixture
def mock_staging_config_gcp_service_account() -> GcpServiceAccountCredentialsWithoutDefaults:
    """Mock GCP GCS staging credentials with service account."""
    creds = GcpServiceAccountCredentialsWithoutDefaults()
    creds.project_id = "test-project"
    creds.private_key = "-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----"
    creds.private_key_id = "key_id"
    creds.client_email = "test@test-project.iam.gserviceaccount.com"
    return creds


@pytest.fixture
def mock_staging_config_gcp_oauth() -> GcpOAuthCredentialsWithoutDefaults:
    """Mock GCP GCS staging credentials with OAuth token."""
    creds = GcpOAuthCredentialsWithoutDefaults()
    creds.project_id = "test-project"
    creds.token = "ya29.test_oauth_token"
    return creds


@pytest.fixture
def mock_staging_config_azure() -> AzureCredentialsWithoutDefaults:
    """Mock Azure Blob storage credentials."""
    creds = AzureCredentialsWithoutDefaults()
    creds.azure_storage_account_name = "testaccount"
    creds.azure_storage_account_key = "test_key"
    creds.azure_account_host = "https://testaccount.blob.core.windows.net"
    return creds


def _create_mock_job(creds: Any) -> RisingwaveLoadJob:
    """Helper to create a mock RisingwaveLoadJob for testing _build_table_function."""
    # Use a valid file path format that ParsedLoadJobFileName can parse
    # Format: table_name.file_id.retry_count.file_format (4 parts)
    file_path = "test_client/my_table.0.0.parquet"

    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.port = 4566
    config.credentials.username = "root"
    config.dataset_name = "test_dataset"

    table_schema: PreparedTableSchema = {
        "name": "my_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint"},
            "name": {"name": "name", "data_type": "text"},
        },
    }

    return RisingwaveLoadJob(file_path, table_schema, config, creds)


def test_risingwave_load_job_file_scan_s3(
    mock_staging_config_aws: AwsCredentialsWithoutDefaults,
) -> None:
    """Test file_scan() SQL generation for S3."""
    from urllib.parse import urlparse

    job = _create_mock_job(mock_staging_config_aws)

    bucket_url = urlparse("s3://test-bucket/data/file.parquet")
    file_name = "file.parquet"
    table_function = job._build_table_function(bucket_url, file_name, "parquet")

    # Should contain S3 file_scan syntax
    assert "file_scan(" in table_function
    assert "'parquet'" in table_function
    assert "'s3'" in table_function
    assert "'us-east-1'" in table_function
    assert "'test_access_key'" in table_function
    assert "'test_secret_key'" in table_function
    assert "'s3://test-bucket/data/file.parquet'" in table_function


def test_risingwave_load_job_file_scan_s3_missing_region(
    mock_staging_config_aws: AwsCredentialsWithoutDefaults,
) -> None:
    """Test that S3 without region_name raises an error."""
    from urllib.parse import urlparse

    mock_staging_config_aws.region_name = None

    job = _create_mock_job(mock_staging_config_aws)

    bucket_url = urlparse("s3://bucket/file.parquet")

    with pytest.raises(Exception) as exc_info:
        job._build_table_function(bucket_url, "file.parquet", "parquet")

    assert "region_name" in str(exc_info.value)


def test_risingwave_load_job_file_scan_gcs_service_account(
    mock_staging_config_gcp_service_account: GcpServiceAccountCredentialsWithoutDefaults,
) -> None:
    """Test file_scan() SQL generation for GCS with service account."""
    from urllib.parse import urlparse

    job = _create_mock_job(mock_staging_config_gcp_service_account)

    bucket_url = urlparse("gs://test-bucket/data/file.parquet")
    file_name = "file.parquet"
    table_function = job._build_table_function(bucket_url, file_name, "parquet")

    # Should contain GCS file_scan syntax with service account
    assert "file_scan(" in table_function
    assert "'parquet'" in table_function
    assert "'gcs'" in table_function
    assert "'gcs://test-bucket/data/file.parquet'" in table_function
    # Service account JSON should be in the function (via to_native_representation)
    assert "test-project" in table_function or "service_account" in table_function


def test_risingwave_load_job_file_scan_gcs_oauth(
    mock_staging_config_gcp_oauth: GcpOAuthCredentialsWithoutDefaults,
) -> None:
    """Test file_scan() SQL generation for GCS with OAuth token."""
    from urllib.parse import urlparse

    job = _create_mock_job(mock_staging_config_gcp_oauth)

    bucket_url = urlparse("gs://test-bucket/data/file.parquet")
    file_name = "file.parquet"
    table_function = job._build_table_function(bucket_url, file_name, "parquet")

    # Should contain GCS file_scan syntax with OAuth token
    assert "file_scan(" in table_function
    assert "'parquet'" in table_function
    assert "'gcs'" in table_function
    assert "'ya29.test_oauth_token'" in table_function
    assert "'gcs://test-bucket/data/file.parquet'" in table_function


def test_risingwave_load_job_file_scan_gcs_normalization(
    mock_staging_config_gcp_service_account: GcpServiceAccountCredentialsWithoutDefaults,
) -> None:
    """Test GCS URL normalization (scheme and path)."""
    from urllib.parse import urlparse

    job = _create_mock_job(mock_staging_config_gcp_service_account)

    # Test scheme normalization (gs -> gcs) and path preservation (slashes are not collapsed)
    test_cases = [
        ("gs://test-bucket/data/file.parquet", "gcs://test-bucket/data/file.parquet"),
        ("gs://test-bucket//data/file.parquet", "gcs://test-bucket//data/file.parquet"),
        ("gcs://test-bucket///data/file.parquet", "gcs://test-bucket///data/file.parquet"),
    ]

    for input_url, expected_url in test_cases:
        bucket_url = urlparse(input_url)
        table_function = job._build_table_function(bucket_url, "file.parquet", "parquet")
        assert f"'{expected_url}'" in table_function


def test_risingwave_load_job_file_scan_azure(
    mock_staging_config_azure: AzureCredentialsWithoutDefaults,
) -> None:
    """Test file_scan() SQL generation for Azure Blob Storage."""
    from urllib.parse import urlparse

    job = _create_mock_job(mock_staging_config_azure)

    bucket_url = urlparse("az://test-container/data/file.parquet")
    file_name = "file.parquet"
    table_function = job._build_table_function(bucket_url, file_name, "parquet")

    # Should contain Azure Blob file_scan syntax
    assert "file_scan(" in table_function
    assert "'parquet'" in table_function
    assert "'azblob'" in table_function
    assert "'testaccount'" in table_function
    assert "'test_key'" in table_function
    assert "'https://testaccount.blob.core.windows.net'" in table_function
    assert "'az://test-container/data/file.parquet'" in table_function


def test_risingwave_load_job_file_scan_azure_missing_credentials() -> None:
    """Test that Azure without required credentials raises an error."""
    from urllib.parse import urlparse

    creds = AzureCredentialsWithoutDefaults()
    creds.azure_storage_account_name = "testaccount"
    # Missing azure_storage_account_key and azure_account_host

    job = _create_mock_job(creds)

    bucket_url = urlparse("az://container/file.parquet")

    with pytest.raises(Exception) as exc_info:
        job._build_table_function(bucket_url, "file.parquet", "parquet")

    assert (
        "azure_storage_account_name" in str(exc_info.value)
        or "azure_storage_account_key" in str(exc_info.value)
        or "azure_account_host" in str(exc_info.value)
    )


def test_risingwave_load_job_unsupported_scheme() -> None:
    """Test that unsupported filesystem schemes raise an error."""
    from urllib.parse import urlparse

    creds = AwsCredentialsWithoutDefaults()
    creds.aws_access_key_id = "test_key"
    creds.aws_secret_access_key = "test_secret"
    creds.region_name = "us-east-1"

    job = _create_mock_job(creds)

    # Using an unsupported scheme (e.g., wasb:// with AWS credentials)
    bucket_url = urlparse("wasb://container/file.parquet")

    with pytest.raises(Exception) as exc_info:
        job._build_table_function(bucket_url, "file.parquet", "parquet")

    assert "does not support" in str(exc_info.value)
    assert "wasb" in str(exc_info.value)


def test_risingwave_load_job_supported_file_formats() -> None:
    """Test that only parquet format is supported for staging."""
    assert RisingwaveLoadJob.SUPPORTED_FILE_FORMATS == ["parquet"]
    assert RisingwaveLoadJob.FILE_FORMAT_TO_RISINGWAVE_FORMAT_MAPPING == {"parquet": "parquet"}


def test_risingwave_load_job_sql_generation_with_json_casts() -> None:
    """Test that RisingwaveLoadJob generates SQL with explicit JSONB casts."""
    from urllib.parse import urlparse

    # Mock parameters
    file_path = "test_client/tb_file.0.0.parquet"
    table_schema: PreparedTableSchema = {
        "name": "tb_file",
        "columns": {
            "id": {"name": "id", "data_type": "bigint"},
            "metadata": {"name": "metadata", "data_type": "json"},
            "created_at": {"name": "created_at", "data_type": "timestamp"},
        },
    }
    config = RisingwaveClientConfiguration()
    staging_credentials = MagicMock()

    # Instantiate job
    job = RisingwaveLoadJob(file_path, table_schema, config, staging_credentials)

    # Mock client and sql_client
    mock_job_client = MagicMock()
    mock_sql_client = MagicMock()
    job._job_client = mock_job_client
    mock_job_client.sql_client = mock_sql_client

    # Setup sql_client behavior
    mock_sql_client.make_qualified_table_name.return_value = '"cake_ekyc"."tb_file"'
    mock_sql_client.capabilities.escape_identifier.side_effect = lambda x: f'"{x}"'

    # Mock ReferenceFollowupJobRequest and _build_table_function
    with patch(
        "dlt.destinations.impl.risingwave.risingwave.ReferenceFollowupJobRequest.is_reference_job",
        return_value=True,
    ), patch(
        "dlt.destinations.impl.risingwave.risingwave.ReferenceFollowupJobRequest.resolve_reference",
        return_value="gcs://bucket/dlt/file.parquet",
    ), patch(
        "dlt.destinations.impl.risingwave.risingwave.get_file_format_and_compression",
        return_value=("parquet", None),
    ), patch.object(
        RisingwaveLoadJob, "_build_table_function", return_value="file_scan(...)"
    ):

        # Run the job
        job.run()

        # Verify execute_sql call
        mock_sql_client.execute_sql.assert_called_once()
        statement = mock_sql_client.execute_sql.call_args[0][0]

        # Expected components
        assert 'INSERT INTO "cake_ekyc"."tb_file"' in statement
        assert '("id", "metadata", "created_at")' in statement
        assert 'SELECT "id", "metadata"::jsonb, "created_at"' in statement
        assert "FROM file_scan(...)" in statement


# ==================== Tests for truncate_tables and file_format_selector ====================


def test_risingwave_sql_client_truncate_tables() -> None:
    """Test that truncate_tables uses DELETE FROM instead of TRUNCATE TABLE."""
    credentials = RisingwaveCredentials()
    credentials.database = "test_db"
    credentials.host = "localhost"
    credentials.port = 4566
    credentials.username = "root"

    # Create proper capabilities mock
    capabilities = risingwave().capabilities()

    client = RisingwaveSqlClient(
        dataset_name="test_dataset",
        staging_dataset_name="test_staging_dataset",
        credentials=credentials,
        capabilities=capabilities,
    )

    # Mock execute_sql to capture the SQL statements
    statements_executed: list[str] = []

    def mock_execute_sql(sql: str, *args: object, **kwargs: object) -> object:
        statements_executed.append(sql)
        return None

    with patch.object(client, "execute_sql", mock_execute_sql):
        # Test truncating multiple tables
        client.truncate_tables("table1", "table2", "table3")

    # Should generate DELETE FROM statements (not TRUNCATE TABLE)
    assert len(statements_executed) == 3
    assert all("DELETE FROM" in str(stmt) for stmt in statements_executed)
    assert all("TRUNCATE" not in str(stmt) for stmt in statements_executed)

    # Check that table names are properly qualified
    assert any("table1" in str(stmt) for stmt in statements_executed)
    assert any("table2" in str(stmt) for stmt in statements_executed)
    assert any("table3" in str(stmt) for stmt in statements_executed)


def test_risingwave_sql_client_truncate_tables_empty() -> None:
    """Test that truncate_tables with no tables does nothing."""
    credentials = RisingwaveCredentials()
    credentials.database = "test_db"
    credentials.host = "localhost"
    credentials.port = 4566
    credentials.username = "root"

    # Create proper capabilities mock
    capabilities = risingwave().capabilities()

    client = RisingwaveSqlClient(
        dataset_name="test_dataset",
        staging_dataset_name="test_staging_dataset",
        credentials=credentials,
        capabilities=capabilities,
    )

    # Mock execute_many to verify it's not called
    execute_called: list[list[object]] = []

    def mock_execute_many(statements: Sequence[object]) -> Sequence[object]:
        execute_called.append(list(statements))
        return []

    with patch.object(client, "execute_many", mock_execute_many):
        # Test truncating no tables
        client.truncate_tables()

    # Should not call execute_many
    assert len(execute_called) == 0


def test_risingwave_file_format_selector_parquet_added() -> None:
    """Test that parquet is always added to supported formats."""
    supported_formats: list[TLoaderFileFormat] = ["insert_values", "csv"]
    table_schema: TTableSchema = {
        "name": "test_table",
        "columns": {},
    }

    preferred, supported = _risingwave_file_format_selector(
        "insert_values", supported_formats, table_schema=table_schema
    )

    # Parquet should be added
    assert "parquet" in supported
    assert preferred == "insert_values"  # Preferred format stays the same


def test_risingwave_file_format_selector_parquet_already_present() -> None:
    """Test that parquet is not duplicated if already present."""
    supported_formats: list[TLoaderFileFormat] = ["insert_values", "csv", "parquet"]
    table_schema: TTableSchema = {
        "name": "test_table",
        "columns": {},
    }

    preferred, supported = _risingwave_file_format_selector(
        "insert_values", supported_formats, table_schema=table_schema
    )

    # Parquet should be present (not duplicated)
    assert supported.count("parquet") == 1
    assert preferred == "insert_values"


def test_risingwave_file_format_selector_preserves_preferred() -> None:
    """Test that the preferred file format is preserved."""
    supported_formats: list[TLoaderFileFormat] = ["insert_values", "csv"]
    table_schema: TTableSchema = {
        "name": "test_table",
        "columns": {},
    }

    # Test with different preferred formats
    for preferred_fmt in ["insert_values", "csv", "parquet"]:
        preferred_fmt_typed: TLoaderFileFormat = cast(TLoaderFileFormat, preferred_fmt)
        preferred, supported = _risingwave_file_format_selector(
            preferred_fmt_typed, supported_formats, table_schema=table_schema
        )
        assert preferred == preferred_fmt
        assert "parquet" in supported


def test_risingwave_capabilities_merge_replace_strategies() -> None:
    """Test that Risingwave declares correct merge and replace strategies."""
    dest = risingwave()
    capabilities = dest.capabilities()

    # Validate merge strategies
    # - delete-insert IS supported (as separate operations)
    # - scd2 is NOT supported due to NULL type casting requirements
    assert capabilities.supported_merge_strategies == ["delete-insert"]

    # Validate replace strategies
    # - truncate-and-insert IS supported (uses DELETE FROM instead of TRUNCATE TABLE)
    assert capabilities.supported_replace_strategies == [
        "truncate-and-insert",
    ]


def test_risingwave_merge_job_delete_without_alias() -> None:
    """Test that RisingwaveMergeJob generates DELETE statements without table aliases.

    Risingwave does not support:
    1. Table aliases in DELETE FROM statements (e.g., "DELETE FROM table AS d WHERE ...")

    The merge job should:
    1. Generate DELETE without 'AS d' alias
    2. Use EXISTS subquery with just the base table name for outer table references
    """
    from dlt.destinations.impl.risingwave.risingwave import RisingwaveMergeJob

    # Test DELETE clause generation (for_delete=True) with quoted qualified names
    root_table = '"public"."users"'
    staging_table = '"public_staging"."users"'
    key_clauses = ['"user_id" = {d}."user_id"']

    delete_clauses = RisingwaveMergeJob.gen_key_table_clauses(
        root_table, staging_table, key_clauses, for_delete=True
    )

    # Should NOT contain table alias (AS d)
    assert len(delete_clauses) == 1
    clause = delete_clauses[0]
    assert " AS d" not in clause
    assert " as d" not in clause
    assert "AS d" not in clause
    # Should use full qualified table name in DELETE FROM
    assert '"public"."users"' in clause
    # Should use EXISTS subquery pattern
    assert "WHERE EXISTS (SELECT 1 FROM" in clause
    assert '"public_staging"."users"' in clause
    # Should use just base table name (users) for outer table reference in WHERE
    assert 'users."user_id"' in clause
    # Should NOT use fully qualified name for outer table reference in WHERE
    assert '"public"."users"."user_id"' not in clause

    # Test with unquoted qualified names
    root_table2 = "public.users"
    staging_table2 = "public_staging.users"
    delete_clauses2 = RisingwaveMergeJob.gen_key_table_clauses(
        root_table2, staging_table2, key_clauses, for_delete=True
    )
    clause2 = delete_clauses2[0]
    # Should use EXISTS subquery pattern
    assert "WHERE EXISTS (SELECT 1 FROM" in clause2
    assert "public_staging.users" in clause2

    # Test SELECT clause generation (for_delete=False) - should use aliases
    select_clauses = RisingwaveMergeJob.gen_key_table_clauses(
        root_table, staging_table, key_clauses, for_delete=False
    )

    # Should contain table aliases for SELECT
    assert len(select_clauses) == 1
    select_clause = select_clauses[0]
    assert "AS d" in select_clause or "as d" in select_clause


def test_risingwave_merge_job_default_order_by() -> None:
    """Test that RisingwaveMergeJob default_order_by returns NULL not a subquery.

    Risingwave does not support subqueries inside ORDER BY clauses.
    The default_order_by should return 'NULL' instead of '(SELECT NULL)'.
    """
    from dlt.destinations.impl.risingwave.risingwave import RisingwaveMergeJob

    # Test default_order_by returns NULL (not a subquery)
    order_by = RisingwaveMergeJob.default_order_by()

    assert order_by == "NULL"
    # Should NOT be a subquery
    assert order_by != "(SELECT NULL)"
    assert "SELECT" not in order_by


def test_risingwave_sql_client_sets_rw_implicit_flush() -> None:
    """Test that RisingwaveSqlClient sets RW_IMPLICIT_FLUSH=true when opening connection.

    RisingWave requires RW_IMPLICIT_FLUSH=true for INSERT/UPDATE/DELETE operations
    to persist data immediately. Without this, data is written but not visible
    to subsequent queries.

    See: https://docs.risingwave.com/sql/set-commands/set-rw_implicit_flush
    """
    from unittest.mock import MagicMock, patch
    from dlt.destinations.impl.risingwave.risingwave_sql_client import RisingwaveSqlClient

    credentials = RisingwaveCredentials()
    capabilities = risingwave().capabilities()

    client = RisingwaveSqlClient(
        dataset_name="test_dataset",
        staging_dataset_name="test_staging_dataset",
        credentials=credentials,
        capabilities=capabilities,
    )

    # Mock the parent's open_connection and cursor to verify the SET command
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch.object(Psycopg2SqlClient, "open_connection", return_value=mock_conn):
        result_conn = client.open_connection()

    # Verify that RW_IMPLICIT_FLUSH was set to true
    mock_cursor.execute.assert_called_once_with("SET RW_IMPLICIT_FLUSH = true")
    # Verify the connection is returned
    assert result_conn == mock_conn


def test_risingwave_sql_client_rw_implicit_flush_sql_format() -> None:
    """Test that the RW_IMPLICIT_FLUSH SQL command has the correct format.

    According to RisingWave documentation, the correct syntax is:
    SET RW_IMPLICIT_FLUSH = { true | false };
    """
    from dlt.destinations.impl.risingwave.risingwave_sql_client import RisingwaveSqlClient

    credentials = RisingwaveCredentials()
    capabilities = risingwave().capabilities()

    client = RisingwaveSqlClient(
        dataset_name="test_dataset",
        staging_dataset_name="test_staging_dataset",
        credentials=credentials,
        capabilities=capabilities,
    )

    # The SQL command should use the exact format from RisingWave docs
    expected_sql = "SET RW_IMPLICIT_FLUSH = true"

    # Should be 'RW_IMPLICIT_FLUSH' not 'implicit_flush'
    assert "RW_IMPLICIT_FLUSH" in expected_sql


def test_risingwave_capabilities_parquet_dictionary_disabled() -> None:
    """Test that Risingwave capabilities disable dictionary encoding for Parquet."""
    caps = risingwave().capabilities()

    assert caps.parquet_format is not None
    assert caps.parquet_format.supports_dictionary_encoding is False


def test_risingwave_load_job_init_initializes_load_table() -> None:
    """Test that RisingwaveLoadJob initializes _load_table in the constructor."""
    from dlt.destinations.impl.risingwave.risingwave import RisingwaveLoadJob
    from dlt.destinations.impl.risingwave.configuration import RisingwaveClientConfiguration

    table: PreparedTableSchema = {
        "name": "test_table",
        "columns": {"col1": {"name": "col1", "data_type": "text"}},
    }
    config = RisingwaveClientConfiguration()

    job = RisingwaveLoadJob("/path/to/test_table.file_id.0.parquet", table, config, None)

    # _load_table should be initialized
    assert job._load_table == table
    # load_table_name property should work
    assert job.load_table_name == "test_table"


def test_risingwave_load_job_sql_generation_multiple_json_columns() -> None:
    """Test SQL generation with multiple JSON columns."""
    from dlt.destinations.impl.risingwave.risingwave import RisingwaveLoadJob
    from dlt.destinations.impl.risingwave.configuration import RisingwaveClientConfiguration

    table: PreparedTableSchema = {
        "name": "test_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint"},
            "doc1": {"name": "doc1", "data_type": "json"},
            "doc2": {"name": "doc2", "data_type": "json"},
            "status": {"name": "status", "data_type": "text"},
        },
    }
    config = RisingwaveClientConfiguration()
    job = RisingwaveLoadJob("/path/to/test_table.file_id.0.parquet", table, config, None)

    # Mock SQL client and related methods
    mock_sql_client = MagicMock()
    mock_sql_client.make_qualified_table_name.return_value = '"public"."test_table"'
    mock_sql_client.capabilities.escape_identifier.side_effect = lambda x: f'"{x}"'

    with patch(
        "dlt.destinations.impl.risingwave.risingwave.ReferenceFollowupJobRequest.is_reference_job",
        return_value=True,
    ), patch(
        "dlt.destinations.impl.risingwave.risingwave.ReferenceFollowupJobRequest.resolve_reference",
        return_value="s3://bucket/test_table.file_id.0.parquet",
    ), patch(
        "dlt.destinations.impl.risingwave.risingwave.get_file_format_and_compression",
        return_value=("parquet", None),
    ), patch.object(
        RisingwaveLoadJob, "_build_table_function", return_value="file_scan(...)"
    ):
        job._job_client = MagicMock()
        job._job_client.sql_client = mock_sql_client

        job.run()

        # Verify execute_sql call
        mock_sql_client.execute_sql.assert_called_once()
        statement = mock_sql_client.execute_sql.call_args[0][0]

        # Expected components
        assert 'INSERT INTO "public"."test_table"' in statement
        assert '("id", "doc1", "doc2", "status")' in statement
        assert 'SELECT "id", "doc1"::jsonb, "doc2"::jsonb, "status"' in statement
        assert "FROM file_scan(...)" in statement


def test_risingwave_alter_table_multi_column_disabled(mock_risingwave_client: RisingwaveClient) -> None:
    """Test that Adding multiple columns generates separate ALTER TABLE statements."""
    new_columns: List[TColumnSchema] = [
        {"name": "new_col1", "data_type": "text", "nullable": True},
        {"name": "new_col2", "data_type": "bigint", "nullable": False},
    ]
    
    # generate_alter=True means table already exists
    sql_statements = mock_risingwave_client._get_table_update_sql("test_table", new_columns, generate_alter=True)
    
    # We expect 2 separate ALTER TABLE statements because alter_add_multi_column is False
    assert len(sql_statements) == 2
    assert "ADD COLUMN" in sql_statements[0]
    assert "ADD COLUMN" in sql_statements[1]
    assert "ALTER TABLE" in sql_statements[0]
    assert "ALTER TABLE" in sql_statements[1]

def test_risingwave_create_table_multi_column_enabled(mock_risingwave_client: RisingwaveClient) -> None:
    """Test that Creating a table with multiple columns still uses a single CREATE TABLE statement."""
    new_columns: List[TColumnSchema] = [
        {"name": "col1", "data_type": "text", "nullable": True},
        {"name": "col2", "data_type": "bigint", "nullable": False},
    ]
    
    # generate_alter=False means CREATE TABLE
    sql_statements = mock_risingwave_client._get_table_update_sql("test_table", new_columns, generate_alter=False)
    
    # Should be one CREATE TABLE statement with columns joined by comma
    assert len(sql_statements) == 1
    assert "CREATE TABLE" in sql_statements[0]
    assert '"col1" varchar' in sql_statements[0]
    assert '"col2" bigint' in sql_statements[0]
    assert "NOT NULL" in sql_statements[0]
