from typing import cast
from unittest.mock import MagicMock, patch

from dlt.common.destination.client import PreparedTableSchema
from dlt.common.schema import TColumnSchema

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from dlt.destinations.impl.doris.factory import (
    doris,
    DorisTypeMapper,
    escape_doris_identifier,
    escape_doris_literal,
)
from dlt.destinations.impl.doris.doris import DorisBrokerLoadJob, DorisMergeJob, DorisSqlClient


# ============================================================
# Destination Registration & Capabilities Tests
# ============================================================


def test_doris_destination_importable() -> None:
    """Test that doris destination can be imported from dlt.destinations."""
    from dlt.destinations import doris as doris_dest

    assert doris_dest is not None


def test_doris_destination_capabilities() -> None:
    """Test that doris destination has correct capabilities."""
    dest = doris()
    caps = dest.capabilities()

    assert caps.preferred_loader_file_format == "insert_values"
    assert "insert_values" in caps.supported_loader_file_formats
    assert caps.preferred_staging_file_format == "parquet"
    assert "parquet" in caps.supported_staging_file_formats
    assert "csv" in caps.supported_staging_file_formats
    assert caps.supports_ddl_transactions is False
    assert caps.supports_transactions is True
    assert "delete-insert" in caps.supported_merge_strategies
    assert "upsert" in caps.supported_merge_strategies
    assert "scd2" not in caps.supported_merge_strategies
    assert "truncate-and-insert" in caps.supported_replace_strategies
    assert "insert-from-staging" in caps.supported_replace_strategies
    assert "staging-optimized" not in caps.supported_replace_strategies
    assert caps.has_case_sensitive_identifiers is False
    assert caps.max_identifier_length == 64
    assert caps.alter_add_multi_column is True
    assert caps.sqlglot_dialect == "doris"
    assert caps.wei_precision == (38, 0)


# ============================================================
# Escape Function Tests
# ============================================================


def test_escape_doris_identifier_simple() -> None:
    """Test backtick identifier escaping."""
    assert escape_doris_identifier("my_table") == "`my_table`"
    assert escape_doris_identifier("column_name") == "`column_name`"


def test_escape_doris_identifier_with_special_chars() -> None:
    """Test backtick identifier escaping with backticks in name."""
    result = escape_doris_identifier("my`table")
    assert "`" in result  # Should be escaped


def test_escape_doris_literal_string() -> None:
    """Test string literal escaping."""
    result = escape_doris_literal("hello")
    assert result == "'hello'"


def test_escape_doris_literal_string_with_quotes() -> None:
    """Test string literal escaping with single quotes."""
    result = escape_doris_literal("it's")
    assert "''" in result  # Single quote should be escaped


def test_escape_doris_literal_none() -> None:
    """Test NULL literal."""
    assert escape_doris_literal(None) == "NULL"


def test_escape_doris_literal_bool() -> None:
    """Test boolean literal escaping (Doris uses 0/1)."""
    assert escape_doris_literal(True) == "1"
    assert escape_doris_literal(False) == "0"


def test_escape_doris_literal_int() -> None:
    """Test integer literal."""
    assert escape_doris_literal(42) == "42"


def test_escape_doris_literal_bytes() -> None:
    """Test bytes literal escaping."""
    result = escape_doris_literal(b"\x00\xff")
    assert result == "X'00ff'"


# ============================================================
# Type Mapper Tests
# ============================================================


def test_type_mapper_text() -> None:
    """Test text -> TEXT mapping (MYSQL equivalent of doris STRING)."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "text"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "TEXT"


def test_type_mapper_text_with_precision() -> None:
    """Test text with precision -> VARCHAR(n) mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "text", "precision": 255})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "VARCHAR(255)"


def test_type_mapper_bigint() -> None:
    """Test bigint -> BIGINT mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "bigint"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "BIGINT"


def test_type_mapper_double() -> None:
    """Test double -> DOUBLE mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "double"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "DOUBLE"


def test_type_mapper_bool() -> None:
    """Test bool -> BOOLEAN mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "bool"})
    result = mapper.to_destination_type(col)
    # SQLAlchemy Boolean might compile to BOOL or TINYINT(1) depending on MySQL version context
    assert result.compile(dialect=mysql.dialect()).upper() in ("BOOL", "BOOLEAN", "TINYINT(1)")


def test_type_mapper_timestamp() -> None:
    """Test timestamp -> DATETIME(6) mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "timestamp"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "DATETIME(6)"


def test_type_mapper_timestamp_with_precision() -> None:
    """Test timestamp with custom precision."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "timestamp", "precision": 3})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "DATETIME(3)"


def test_type_mapper_date() -> None:
    """Test date -> DATE mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "date"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "DATE"


def test_type_mapper_json() -> None:
    """Test json -> JSON mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "json"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "JSON"


def test_type_mapper_decimal() -> None:
    """Test decimal -> DECIMAL(p, s) mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "decimal", "precision": 18, "scale": 4})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper().replace(" ", "") == "DECIMAL(18,4)"


def test_type_mapper_decimal_default_precision() -> None:
    """Test decimal with default precision."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "decimal"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper().replace(" ", "").startswith("DECIMAL(")


def test_type_mapper_wei() -> None:
    """Test wei -> DECIMAL(38, 0) mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "wei"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper().replace(" ", "") == "DECIMAL(38,0)"


def test_type_mapper_binary() -> None:
    """Test binary -> LONGBLOB mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "binary"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper() == "LONGBLOB"


def test_type_mapper_time() -> None:
    """Test time -> VARCHAR(255) mapping."""
    caps = doris().capabilities()
    mapper = DorisTypeMapper(caps)
    col = cast(TColumnSchema, {"name": "col", "data_type": "time"})
    result = mapper.to_destination_type(col)
    assert result.compile(dialect=mysql.dialect()).upper().replace(" ", "") == "VARCHAR(255)"


# ============================================================
# DorisMergeJob Tests
# ============================================================


def test_doris_merge_job_default_order_by() -> None:
    """Test that DorisMergeJob uses the default ORDER BY."""
    assert DorisMergeJob.default_order_by() == "(SELECT NULL)"


def test_doris_merge_job_gen_key_table_clauses_for_delete() -> None:
    """Test that DorisMergeJob generates DELETE with USING syntax."""
    key_clauses = ["{d}.`id` = {s}.`id`"]
    result = DorisMergeJob.gen_key_table_clauses(
        '"schema"."table"', '"schema"."staging_table"', key_clauses, for_delete=True
    )

    assert len(result) == 1
    clause = result[0]
    # Should contain USING
    assert "USING" in clause
    # Should contain AS s
    assert "AS s" in clause
    # Should reference the staging table as s
    assert '"schema"."table".`id` = s.`id`'.lower() in clause.lower()

    # Multiple keys should be joined by AND
    key_clauses = ["{d}.`id` = {s}.`id`", "{d}.`name` = {s}.`name`"]
    result = DorisMergeJob.gen_key_table_clauses(
        '"schema"."table"', '"schema"."staging_table"', key_clauses, for_delete=True
    )
    assert len(result) == 1
    assert " AND ".lower() in result[0].lower()


def test_doris_sql_client_truncate_tables() -> None:
    """Test that DorisSqlClient uses TRUNCATE TABLE."""
    from dlt.destinations.impl.doris.configuration import DorisCredentials
    from dlt.common.destination import DestinationCapabilitiesContext

    # Use real capabilities and credentials
    caps = DestinationCapabilitiesContext()
    caps.escape_identifier = escape_doris_identifier
    caps.casefold_identifier = str.lower
    creds = DorisCredentials()
    creds.database = "dataset"  # Will be used as database_name

    client = DorisSqlClient("dataset", "staging_dataset", creds, caps)
    client.execute_sql = MagicMock()

    # Call truncate
    client.truncate_tables("table1", "table2")

    # Verify execute_sql calls
    assert client.execute_sql.call_count == 2
    # make_qualified_table_name will result in `dataset`.`table1`
    client.execute_sql.assert_any_call("TRUNCATE TABLE `dataset`.`table1`")
    client.execute_sql.assert_any_call("TRUNCATE TABLE `dataset`.`table2`")


def test_doris_client_unique_key_creation() -> None:
    """Test that DorisClient configures UNIQUE KEY model when primary keys are present."""
    from dlt.destinations.impl.doris.doris import DorisClient
    from dlt.destinations.impl.doris.configuration import DorisClientConfiguration, DorisCredentials
    from dlt.common.schema import Schema

    # Setup config with create_indexes=True
    config = DorisClientConfiguration()
    config.create_indexes = True
    config.credentials = DorisCredentials()
    config.credentials.database = "dataset"

    # Setup client
    schema = Schema("test")
    capabilities = doris().capabilities()
    client = DorisClient(schema, config, capabilities)

    # Mock SQL client
    client.sql_client = MagicMock()
    client.sql_client.dataset_name = "dataset"
    client.sql_client.metadata = sa.MetaData()

    # Create table schema with primary key
    table_schema: PreparedTableSchema = {
        "name": "test_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True, "nullable": False},
            "value": {"name": "value", "data_type": "text", "nullable": True},
        },
    }

    # Generate table
    table = client._to_table_object(table_schema)

    # Assert that mysql_engine was injected instead of PrimaryKeyConstraint
    assert "mysql_engine" in table.kwargs
    engine_str = table.kwargs["mysql_engine"]
    assert "olap" in engine_str
    assert "UNIQUE KEY(`id`)" in engine_str
    assert "DISTRIBUTED BY HASH(`id`)" in engine_str

    # Ensure no sa.PrimaryKeyConstraint was added to avoid MySQL compiler error
    for c in table.constraints:
        assert not isinstance(c, sa.PrimaryKeyConstraint)


def test_doris_merge_job_gen_key_table_clauses_for_select() -> None:
    """Test that DorisMergeJob delegates to parent for non-delete operations."""
    key_clauses = ["{d}.`id` = {s}.`id`"]
    result = DorisMergeJob.gen_key_table_clauses(
        '"schema"."table"',
        '"schema"."staging_table"',
        key_clauses,
        for_delete=False,
    )
    # Should return parent's implementation (non-empty list)
    assert len(result) > 0


# ============================================================
# DorisBrokerLoadJob SQL Generation Tests
# ============================================================


def test_doris_broker_load_generate_label() -> None:
    """Test unique label generation for Broker Load."""
    mock_table = {
        "name": "test_table",
        "columns": {"id": {"name": "id", "data_type": "bigint"}},
    }
    mock_config = MagicMock()
    mock_config.broker_load_timeout = 3600
    mock_config.broker_load_poll_interval = 5.0
    mock_config.broker_load_max_filter_ratio = 0.0

    job = DorisBrokerLoadJob.__new__(DorisBrokerLoadJob)
    job._load_table = cast(PreparedTableSchema, mock_table)
    job._config = mock_config
    job._file_path = "test_file.reference"

    label = job._generate_label()
    assert label.startswith("dlt_test_table_")
    assert len(label) <= 128


def test_doris_broker_load_build_with_clause_s3() -> None:
    """Test S3 WITH clause generation."""
    mock_config = MagicMock()
    mock_config.broker_load_timeout = 3600
    mock_config.broker_load_poll_interval = 5.0
    mock_config.broker_load_max_filter_ratio = 0.0
    mock_config.broker_load_access_key = None
    mock_config.broker_load_secret_key = None

    job = DorisBrokerLoadJob.__new__(DorisBrokerLoadJob)
    job._config = mock_config

    # Mock AWS credentials
    aws_creds = MagicMock()
    aws_creds.aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
    aws_creds.aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    aws_creds.region_name = "us-east-1"
    aws_creds.endpoint_url = None

    # Need to set it as AwsCredentialsWithoutDefaults
    from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults

    job._staging_credentials = aws_creds
    # Mock isinstance check by using a real spec
    job._staging_credentials.__class__ = AwsCredentialsWithoutDefaults  # type: ignore[assignment]

    with_clause = job._build_with_clause("s3")
    assert '"provider" = "S3"' in with_clause
    assert "AKIAIOSFODNN7EXAMPLE" in with_clause
    assert "us-east-1" in with_clause


def test_doris_broker_load_build_with_clause_gcp() -> None:
    """Test GCP WITH clause generation with service account credentials."""
    mock_config = MagicMock()
    mock_config.broker_load_timeout = 3600
    mock_config.broker_load_poll_interval = 5.0
    mock_config.broker_load_max_filter_ratio = 0.0
    mock_config.broker_load_access_key = None
    mock_config.broker_load_secret_key = None

    job = DorisBrokerLoadJob.__new__(DorisBrokerLoadJob)
    job._config = mock_config

    # Mock GCP service account credentials
    from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults

    gcp_creds = MagicMock(spec=GcpServiceAccountCredentialsWithoutDefaults)
    gcp_creds.project_id = "test-project"
    gcp_creds.private_key = "-----BEGIN RSA PRIVATE KEY-----\ntest_key\n-----END RSA PRIVATE KEY-----"
    gcp_creds.private_key_id = "test_key_id"
    gcp_creds.client_email = "test@test-project.iam.gserviceaccount.com"
    gcp_creds.client_id = "123456789"

    job._staging_credentials = gcp_creds

    with_clause = job._build_with_clause("gs")
    assert '"provider" = "GCP"' in with_clause
    assert '"s3.endpoint" = "https://storage.googleapis.com"' in with_clause
    assert '"s3.access_key" = "test-project"' in with_clause
    assert "test-project.iam.gserviceaccount.com" in with_clause
    assert '"s3.region" = "auto"' in with_clause


def test_doris_broker_load_into_table_unqualified() -> None:
    """Test that INTO TABLE clause uses unqualified table name (no database prefix).

    Doris LOAD statement does not accept database-qualified table names
    in the INTO TABLE clause - the database is inferred from LOAD LABEL.
    """
    mock_table = {
        "name": "test_table",
        "columns": {
            "id": {"name": "id", "data_type": "bigint"},
            "value": {"name": "value", "data_type": "text"},
        },
    }
    mock_config = MagicMock()
    mock_config.broker_load_timeout = 3600
    mock_config.broker_load_poll_interval = 5.0
    mock_config.broker_load_max_filter_ratio = 0.0
    mock_config.broker_load_access_key = None
    mock_config.broker_load_secret_key = None

    # Mock the job client with capabilities and sql_client
    mock_job_client = MagicMock()
    mock_job_client.capabilities = MagicMock()
    mock_job_client.capabilities.escape_identifier = escape_doris_identifier
    mock_job_client.sql_client = MagicMock()
    mock_job_client.sql_client.dataset_name = "my_dataset"

    job = DorisBrokerLoadJob.__new__(DorisBrokerLoadJob)
    job._load_table = cast(PreparedTableSchema, mock_table)
    job._config = mock_config
    job._job_client = mock_job_client

    # Mock AWS credentials
    aws_creds = MagicMock()
    aws_creds.aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
    aws_creds.aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    aws_creds.region_name = "us-east-1"
    aws_creds.endpoint_url = None
    from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults

    job._staging_credentials = aws_creds
    job._staging_credentials.__class__ = AwsCredentialsWithoutDefaults  # type: ignore[assignment]

    # Build the broker load SQL
    sql = job._build_broker_load_sql("test_label", "s3://bucket/file.parquet", "s3", "parquet")

    # Verify INTO TABLE clause uses unqualified table name
    assert "INTO TABLE `test_table`" in sql
    # Make sure it does NOT have database-qualified name
    assert "INTO TABLE `my_dataset`.`test_table`" not in sql
    # Verify column list is present
    assert "`id`" in sql
    assert "`value`" in sql
    # Verify LOAD LABEL has proper database context
    assert "LOAD LABEL `my_dataset`" in sql
    # Verify FORMAT clause
    assert 'FORMAT AS "PARQUET"' in sql


def test_doris_broker_load_with_clause_override_credentials_gcs() -> None:
    """Test Broker Load WITH clause with override credentials for GCS.

    When broker_load_access_key and broker_load_secret_key are set on the config,
    they should override the keys derived from staging credentials.
    """
    mock_config = MagicMock()
    mock_config.broker_load_timeout = 3600
    mock_config.broker_load_poll_interval = 5.0
    mock_config.broker_load_max_filter_ratio = 0.0
    # Set the override credentials
    mock_config.broker_load_access_key = "GOOG_HMAC_ACCESS_KEY"
    mock_config.broker_load_secret_key = "GOOG_HMAC_SECRET_KEY"

    job = DorisBrokerLoadJob.__new__(DorisBrokerLoadJob)
    job._config = mock_config
    job._file_path = "test_file.reference"

    # Mock GCP service account credentials (staging)
    from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults

    gcp_creds = MagicMock(spec=GcpServiceAccountCredentialsWithoutDefaults)
    gcp_creds.project_id = "test-project"
    gcp_creds.private_key = "-----BEGIN RSA PRIVATE KEY-----\ntest_key\n-----END RSA PRIVATE KEY-----"
    gcp_creds.private_key_id = "test_key_id"
    gcp_creds.client_email = "test@test-project.iam.gserviceaccount.com"
    gcp_creds.client_id = "123456789"

    job._staging_credentials = gcp_creds

    # Build WITH clause for GCS bucket
    with_clause = job._build_with_clause("gs")

    # Verify it uses GCP provider with GCS endpoint
    assert '"provider" = "GCP"' in with_clause
    assert '"s3.endpoint" = "https://storage.googleapis.com"' in with_clause
    # Verify it uses the OVERRIDE keys, not the service account keys
    assert '"s3.access_key" = "GOOG_HMAC_ACCESS_KEY"' in with_clause
    assert '"s3.secret_key" = "GOOG_HMAC_SECRET_KEY"' in with_clause
    # Should NOT contain the service account email
    assert "test-project.iam.gserviceaccount.com" not in with_clause
    assert '"s3.region" = "auto"' in with_clause


def test_doris_broker_load_with_clause_override_credentials_wrong_type() -> None:
    """Test Broker Load WITH clause with override when staging creds don't match bucket scheme.

    User has AWS credentials in .dlt/secrets.toml (for staging), but wants to use
    GCS with HMAC keys for Broker Load. Override credentials allow this.
    """
    mock_config = MagicMock()
    mock_config.broker_load_timeout = 3600
    mock_config.broker_load_poll_interval = 5.0
    mock_config.broker_load_max_filter_ratio = 0.0
    # Set the override HMAC credentials for GCS
    mock_config.broker_load_access_key = "GOOG_HMAC_ACCESS_KEY"
    mock_config.broker_load_secret_key = "GOOG_HMAC_SECRET_KEY"

    job = DorisBrokerLoadJob.__new__(DorisBrokerLoadJob)
    job._config = mock_config
    job._file_path = "test_file.reference"

    # Use AWS credentials for staging (wrong type for GCS)
    aws_creds = MagicMock()
    aws_creds.aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
    aws_creds.aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    aws_creds.region_name = "us-east-1"
    aws_creds.endpoint_url = None
    from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults

    job._staging_credentials = aws_creds
    job._staging_credentials.__class__ = AwsCredentialsWithoutDefaults  # type: ignore[assignment]

    # Build WITH clause for GCS bucket (mismatched credential type)
    # With overrides set, this should succeed
    with_clause = job._build_with_clause("gs")

    # Verify it uses GCP provider (from scheme, not from credentials type)
    assert '"provider" = "GCP"' in with_clause
    assert '"s3.endpoint" = "https://storage.googleapis.com"' in with_clause
    # Verify it uses the OVERRIDE keys
    assert '"s3.access_key" = "GOOG_HMAC_ACCESS_KEY"' in with_clause
    assert '"s3.secret_key" = "GOOG_HMAC_SECRET_KEY"' in with_clause
    assert '"s3.region" = "auto"' in with_clause
# ============================================================
# DorisMergeJob MERGE SQL Generation Tests
# ============================================================


def _extract_update_clause(merge_stmt: str) -> str:
    """Extract the UPDATE SET clause from a MERGE statement."""
    import re
    update_set_match = re.search(
        r'WHEN MATCHED\s+THEN UPDATE SET\s+(.+?)(?=\s+WHEN NOT MATCHED|$)',
        merge_stmt,
        re.DOTALL
    )
    if update_set_match:
        return update_set_match.group(1)
    return ""


def test_doris_merge_job_gen_upsert_merge_sql_excludes_primary_keys() -> None:
    """Test that gen_upsert_merge_sql excludes primary keys from UPDATE SET clause.

    Doris throws: Only value columns of unique table could be updated.
    This test ensures that primary key columns are NOT included in the UPDATE clause.
    """
    root_table_name = "`db`.`user_events_v5`"
    staging_root_table_name = "`db_staging`.`user_events_v5`"
    primary_keys = ["`user_id`"]
    root_table_column_names = ["`user_id`", "`event_type`", "`timestamp`", "`_dlt_load_id`", "`_dlt_id`"]
    hard_delete_col = None
    deleted_cond = None

    sql = DorisMergeJob.gen_upsert_merge_sql(
        root_table_name,
        staging_root_table_name,
        primary_keys,
        root_table_column_names,
        hard_delete_col,
        deleted_cond,
    )

    assert len(sql) == 1
    merge_stmt = sql[0]

    # Should contain MERGE INTO
    assert "MERGE INTO" in merge_stmt
    # Should use primary key in ON clause
    assert "ON d.`user_id` = s.`user_id`" in merge_stmt

    # Extract the UPDATE SET clause specifically
    update_clause = _extract_update_clause(merge_stmt)
    assert update_clause, "Should have UPDATE SET clause"

    # PRIMARY KEY should NOT be in UPDATE SET clause
    assert "`user_id` = s.`user_id`" not in update_clause, "user_id (primary key) should not be in UPDATE SET clause"

    # UPDATE SET should only contain non-primary key columns
    assert "`event_type` = s.`event_type`" in update_clause
    assert "`timestamp` = s.`timestamp`" in update_clause
    assert "`_dlt_load_id` = s.`_dlt_load_id`" in update_clause
    assert "`_dlt_id` = s.`_dlt_id`" in update_clause


def test_doris_merge_job_gen_upsert_merge_sql_multiple_primary_keys() -> None:
    """Test that gen_upsert_merge_sql handles multiple primary keys correctly."""
    root_table_name = "`db`.`events`"
    staging_root_table_name = "`db_staging`.`events`"
    primary_keys = ["`user_id`", "`event_id`"]
    root_table_column_names = [
        "`user_id`",
        "`event_id`",
        "`event_type`",
        "`timestamp`",
    ]
    hard_delete_col = None
    deleted_cond = None

    sql = DorisMergeJob.gen_upsert_merge_sql(
        root_table_name,
        staging_root_table_name,
        primary_keys,
        root_table_column_names,
        hard_delete_col,
        deleted_cond,
    )

    assert len(sql) == 1
    merge_stmt = sql[0]

    # Both primary keys should be in ON clause
    assert "d.`user_id` = s.`user_id`" in merge_stmt
    assert "d.`event_id` = s.`event_id`" in merge_stmt

    # Extract UPDATE SET clause
    update_clause = _extract_update_clause(merge_stmt)

    # Neither primary key should be in UPDATE SET
    assert "`user_id` = s.`user_id`" not in update_clause
    assert "`event_id` = s.`event_id`" not in update_clause

    # Only non-primary key columns should be in UPDATE SET
    assert "`event_type` = s.`event_type`" in update_clause
    assert "`timestamp` = s.`timestamp`" in update_clause


def test_doris_merge_job_gen_upsert_merge_sql_with_hard_delete() -> None:
    """Test that gen_upsert_merge_sql handles hard delete column correctly."""
    root_table_name = "`db`.`user_events_v5`"
    staging_root_table_name = "`db_staging`.`user_events_v5`"
    primary_keys = ["`user_id`"]
    root_table_column_names = ["`user_id`", "`event_type`", "`deleted_at`"]
    hard_delete_col = "`deleted_at`"
    deleted_cond = "`deleted_at` IS NOT NULL"

    sql = DorisMergeJob.gen_upsert_merge_sql(
        root_table_name,
        staging_root_table_name,
        primary_keys,
        root_table_column_names,
        hard_delete_col,
        deleted_cond,
    )

    assert len(sql) == 1
    merge_stmt = sql[0]

    # Should contain hard delete clause
    assert "WHEN MATCHED AND s.`deleted_at` IS NOT NULL THEN DELETE" in merge_stmt

    # Extract UPDATE SET clause
    update_clause = _extract_update_clause(merge_stmt)

    # PRIMARY KEY should NOT be in UPDATE SET clause
    assert "`user_id` = s.`user_id`" not in update_clause


def test_doris_merge_job_gen_upsert_sql_with_table_schema() -> None:
    """Test gen_upsert_sql with a realistic table schema.

    This is an integration-style test that verifies the full flow from
    table schema to SQL generation.
    """
    from dlt.destinations.impl.doris.doris import DorisClient
    from dlt.destinations.impl.doris.configuration import DorisClientConfiguration, DorisCredentials
    from dlt.common.schema import Schema

    # Setup config
    config = DorisClientConfiguration()
    config.credentials = DorisCredentials()
    config.credentials.database = "raw"
    config.credentials.host = "localhost"
    config.credentials.port = 9030
    config.credentials.username = "root"
    config.create_indexes = True

    # Setup schema
    schema = Schema("test")
    capabilities = doris().capabilities()
    client = DorisClient(schema, config, capabilities)

    # Mock SQL client
    mock_sql_client = MagicMock()
    mock_sql_client.dataset_name = "raw"
    mock_sql_client.staging_dataset_name = "raw_staging"
    mock_sql_client.escape_column_name = lambda x: f"`{x}`"
    mock_sql_client.capabilities.escape_literal = lambda x: f"'{x}'" if isinstance(x, str) else str(x)

    # get_qualified_table_names returns a tuple of (table_name, staging_table_name)
    # The method takes just a name parameter
    mock_sql_client.get_qualified_table_names = lambda name: (
        f"`raw`.`{name}`",
        f"`raw_staging`.`{name}`",
    )

    mock_sql_client.fully_qualified_dataset_name = lambda _self=None, staging=False: (
        "`raw_staging`" if staging else "`raw`"
    )

    # Create table schema with primary key
    table_schema: PreparedTableSchema = {
        "name": "user_events_v5",
        "columns": {
            "user_id": {"name": "user_id", "data_type": "bigint", "primary_key": True, "nullable": False},
            "event_type": {"name": "event_type", "data_type": "text", "nullable": True},
            "timestamp": {"name": "timestamp", "data_type": "timestamp", "nullable": True},
            "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": True},
            "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": True},
        },
        "write_disposition": "merge",
    }

    table_chain = [table_schema]

    # Generate SQL
    sql = DorisMergeJob.gen_upsert_sql(table_chain, mock_sql_client)

    assert len(sql) == 1
    merge_stmt = sql[0]

    # Verify the structure
    assert "MERGE INTO `raw`.`user_events_v5` d" in merge_stmt
    assert "USING `raw_staging`.`user_events_v5` s" in merge_stmt

    # Primary key should be in ON clause
    assert "ON d.`user_id` = s.`user_id`" in merge_stmt

    # Extract UPDATE SET clause
    update_clause = _extract_update_clause(merge_stmt)

    # Primary key should NOT be in UPDATE SET
    assert "`user_id` = s.`user_id`" not in update_clause

    # Non-primary key columns should be in UPDATE SET
    assert "`event_type` = s.`event_type`" in update_clause
    assert "`timestamp` = s.`timestamp`" in update_clause


def test_doris_merge_job_gen_upsert_merge_sql_no_update_columns() -> None:
    """Test that gen_upsert_merge_sql handles the case where all columns are primary keys.

    In this case, the UPDATE SET clause should be empty, and we should only have
    WHEN NOT MATCHED ... INSERT.
    """
    root_table_name = "`db`.`pk_only`"
    staging_root_table_name = "`db_staging`.`pk_only`"
    primary_keys = ["`id`", "`id2`"]
    root_table_column_names = ["`id`", "`id2`"]
    hard_delete_col = None
    deleted_cond = None

    sql = DorisMergeJob.gen_upsert_merge_sql(
        root_table_name,
        staging_root_table_name,
        primary_keys,
        root_table_column_names,
        hard_delete_col,
        deleted_cond,
    )

    assert len(sql) == 1
    merge_stmt = sql[0]

    # Should still have MERGE INTO
    assert "MERGE INTO" in merge_stmt
    assert "ON" in merge_stmt

    # Should NOT have WHEN MATCHED ... UPDATE (no columns to update)
    # Should only have WHEN NOT MATCHED ... INSERT
    assert "WHEN MATCHED" not in merge_stmt or "THEN UPDATE SET" not in merge_stmt
    assert "WHEN NOT MATCHED" in merge_stmt
