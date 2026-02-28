import secrets
import time
import warnings
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING, Union
from urllib.parse import urlparse

from dlt.common import logger

from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    GcpCredentials,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    FollowupJobRequest,
    HasFollowupJobs,
    PreparedTableSchema,
    RunnableLoadJob,
    LoadJob,
    SupportsStagingDestination,
)
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.schema.typing import TColumnType
from dlt.common.schema.utils import get_columns_names_with_prop, is_complete_column
from dlt.common.storages import FileStorage
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.doris.configuration import DorisClientConfiguration
from dlt.destinations.impl.doris.typing import (
    TABLE_MODEL_HINT,
    TABLE_DISTRIBUTED_BY_HINT,
    TABLE_BUCKETS_HINT,
    TABLE_PROPERTIES_HINT,
    TTableProperties,
)
from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.path_utils import get_file_format_and_compression
from dlt.destinations.sql_jobs import SqlMergeFollowupJob

if TYPE_CHECKING:
    pass

import sqlalchemy as sa

# Suppress SQLAlchemy warnings about unknown Doris-specific DDL during schema reflection
# Doris uses non-standard SQL extensions (DUPLICATE KEY, DISTRIBUTED BY, etc.) that
# SQLAlchemy's MySQL dialect doesn't recognize, but these are harmless.
warnings.filterwarnings(
    "ignore",
    message="Unknown schema content",
    category=Warning,
)


TStagingCredentials = Union[
    AwsCredentialsWithoutDefaults,
    GcpCredentials,
    AzureCredentialsWithoutDefaults,
]


class DorisBrokerLoadJob(RunnableLoadJob, HasFollowupJobs):
    """Load job for loading staged files into Doris via Broker Load.

    Broker Load is Apache Doris' native mechanism for pulling data from remote storage
    (S3, GCS, Azure, HDFS). It is asynchronous — submits a LOAD LABEL via SQL then polls
    SHOW LOAD until the job reaches FINISHED or CANCELLED state.

    Supported formats: parquet, csv.
    Supported providers: S3 (AWS, MinIO), GCS (GCP), Azure.
    """

    SUPPORTED_FILE_FORMATS = ["parquet", "csv"]

    def __init__(
        self,
        file_path: str,
        table: PreparedTableSchema,
        config: DorisClientConfiguration,
        staging_credentials: TStagingCredentials,
    ) -> None:
        super().__init__(file_path)
        self._job_client: Optional["DorisClient"] = None
        self._load_table = table
        self._config = config
        self._staging_credentials = staging_credentials

    def run(self) -> None:
        client = self._job_client.sql_client

        if not ReferenceFollowupJobRequest.is_reference_job(self._file_path):
            raise LoadJobTerminalException(
                self._file_path,
                "Doris Broker Load job only supports staged files from object storage",
            )

        bucket_path = ReferenceFollowupJobRequest.resolve_reference(self._file_path)
        file_name = FileStorage.get_file_name_from_file_path(bucket_path)
        bucket_url = urlparse(bucket_path)

        file_format, _ = get_file_format_and_compression(file_name)
        if file_format not in self.SUPPORTED_FILE_FORMATS:
            raise LoadJobTerminalException(
                self._file_path,
                f"Doris Broker Load does not support `{file_format}` format. "
                f"Supported formats: {self.SUPPORTED_FILE_FORMATS}",
            )

        # Generate a unique label for the Broker Load job
        label = self._generate_label()

        # Build the LOAD LABEL SQL
        load_sql = self._build_broker_load_sql(label, bucket_path, bucket_url.scheme, file_format)

        logger.info(f"Doris: submitting Broker Load job with label '{label}'")

        # Submit the async load job
        with client.begin_transaction():
            client.execute_sql(load_sql)

        # Poll for completion
        self._poll_broker_load(client, label)

    def _generate_label(self) -> str:
        """Generate a unique Broker Load label.

        Format: dlt_<table_name>_<random_hex>
        Doris label max length is 128 characters.
        """
        table_name = self.load_table_name
        random_suffix = secrets.token_hex(4)
        label = f"dlt_{table_name}_{random_suffix}"
        # Truncate to 128 chars if needed
        return label[:128]

    def _build_broker_load_sql(
        self, label: str, file_uri: str, scheme: str, file_format: str
    ) -> str:
        """Build the LOAD LABEL SQL statement for Broker Load."""
        client = self._job_client.sql_client
        db_name = client.dataset_name
        # Doris LOAD INTO TABLE does not accept database-qualified names, use table name only
        table_name = self._job_client.capabilities.escape_identifier(self.load_table_name)

        # Build column list
        column_names = []
        for col_name in self._load_table["columns"]:
            escaped_col = self._job_client.capabilities.escape_identifier(col_name)
            column_names.append(escaped_col)
        columns_clause = ", ".join(column_names)

        # Determine format clause
        format_clause = f'FORMAT AS "{file_format.upper()}"'

        # Build provider-specific WITH clause
        with_clause = self._build_with_clause(scheme)

        # Build properties
        timeout = self._config.broker_load_timeout
        max_filter_ratio = self._config.broker_load_max_filter_ratio

        sql = (
            f"LOAD LABEL `{db_name}`.`{label}`\n"
            "(\n"
            f'    DATA INFILE("{file_uri}")\n'
            f"    INTO TABLE {table_name}\n"
            f"    {format_clause}\n"
            f"    ({columns_clause})\n"
            ")\n"
            f"{with_clause}\n"
            "PROPERTIES (\n"
            f'    "timeout" = "{timeout}",\n'
            f'    "max_filter_ratio" = "{max_filter_ratio}"\n'
            ")"
        )
        return sql

    def _build_with_clause(self, scheme: str) -> str:
        """Build the WITH S3/HDFS clause based on the storage provider.

        Uses staging credentials configured in dlt's filesystem destination configuration.
        For example, users configure credentials in .dlt/secrets.toml:

            [destination.filesystem.credentials]
            aws_access_key_id = "..."
            aws_secret_access_key = "..."

        These same credentials are automatically used by Doris Broker Load.

        When broker_load_access_key and broker_load_secret_key are set on the config,
        they override the automatically-detected keys (useful when the staging credentials
        don't match the bucket scheme or when using different HMAC keys for Broker Load).
        """
        creds = self._staging_credentials
        override_access_key = getattr(self._config, "broker_load_access_key", None)
        override_secret_key = getattr(self._config, "broker_load_secret_key", None)

        # If both overrides are set, use them regardless of credential type
        if override_access_key and override_secret_key:
            if scheme == "s3":
                region = getattr(creds, "region_name", "us-east-1") or "us-east-1"
                endpoint = getattr(creds, "endpoint_url", None) or f"s3.{region}.amazonaws.com"
                return (
                    "WITH S3 (\n"
                    '    "provider" = "S3",\n'
                    f'    "s3.endpoint" = "https://{endpoint}",\n'
                    f'    "s3.access_key" = "{override_access_key}",\n'
                    f'    "s3.secret_key" = "{override_secret_key}",\n'
                    f'    "s3.region" = "{region}"\n'
                    ")"
                )

            elif scheme in ("gs", "gcs"):
                return (
                    "WITH S3 (\n"
                    '    "provider" = "GCP",\n'
                    '    "s3.endpoint" = "https://storage.googleapis.com",\n'
                    f'    "s3.access_key" = "{override_access_key}",\n'
                    f'    "s3.secret_key" = "{override_secret_key}",\n'
                    '    "s3.region" = "auto"\n'
                    ")"
                )

            elif scheme in ("az", "abfs"):
                account_name = getattr(creds, "azure_storage_account_name", "")
                endpoint = getattr(
                    creds,
                    "azure_account_host",
                    f"{account_name}.blob.core.windows.net",
                )
                return (
                    "WITH S3 (\n"
                    '    "provider" = "AZURE",\n'
                    f'    "s3.endpoint" = "https://{endpoint}",\n'
                    f'    "s3.access_key" = "{override_access_key}",\n'
                    f'    "s3.secret_key" = "{override_secret_key}",\n'
                    '    "s3.region" = "auto"\n'
                    ")"
                )

            else:
                raise LoadJobTerminalException(
                    self._file_path,
                    f"Doris Broker Load does not support `{scheme}` scheme with the provided"
                    " credentials. Supported: S3 (s3://), GCS (gs://), Azure (az://, abfs://).",
                )

        if scheme == "s3" and isinstance(creds, AwsCredentialsWithoutDefaults):
            access_key = creds.aws_access_key_id
            secret_key = creds.aws_secret_access_key
            region = getattr(creds, "region_name", "us-east-1") or "us-east-1"
            endpoint = getattr(creds, "endpoint_url", None)

            # Construct endpoint from region if not explicitly set
            if not endpoint:
                endpoint = f"s3.{region}.amazonaws.com"

            return (
                "WITH S3 (\n"
                '    "provider" = "S3",\n'
                f'    "s3.endpoint" = "https://{endpoint}",\n'
                f'    "s3.access_key" = "{access_key}",\n'
                f'    "s3.secret_key" = "{secret_key}",\n'
                f'    "s3.region" = "{region}"\n'
                ")"
            )

        elif scheme in ("gs", "gcs") and isinstance(creds, GcpCredentials):
            # GCS via GCP provider with service account credentials (Doris v2.0+ syntax)
            # For GCS, we need to pass the full service account JSON as the secret key
            from dlt.common.json import json

            project_id = getattr(creds, "project_id", "")

            # For GcpServiceAccountCredentialsWithoutDefaults, serialize the full credentials
            if hasattr(creds, "private_key") and hasattr(creds, "client_email"):
                # Service account credentials - serialize to JSON
                service_account_dict = {
                    "type": "service_account",
                    "project_id": project_id,
                    "private_key_id": getattr(creds, "private_key_id", ""),
                    "private_key": getattr(creds, "private_key", ""),
                    "client_email": getattr(creds, "client_email", ""),
                    "client_id": getattr(creds, "client_id", ""),
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
                # Remove empty values to keep the JSON clean
                service_account_dict = {k: v for k, v in service_account_dict.items() if v}
                secret_key = json.dumps(service_account_dict)
                # Escape double quotes for SQL string literal
                secret_key = secret_key.replace('"', '\\"')
            else:
                # OAuth or default credentials - only project ID is available
                secret_key = ""

            return (
                "WITH S3 (\n"
                '    "provider" = "GCP",\n'
                '    "s3.endpoint" = "https://storage.googleapis.com",\n'
                f'    "s3.access_key" = "{project_id}",\n'
                f'    "s3.secret_key" = "{secret_key}",\n'
                '    "s3.region" = "auto"\n'
                ")"
            )

        elif scheme in ("az", "abfs") and isinstance(creds, AzureCredentialsWithoutDefaults):
            account_name = getattr(creds, "azure_storage_account_name", "")
            account_key = getattr(creds, "azure_storage_account_key", "")
            endpoint = getattr(
                creds,
                "azure_account_host",
                f"{account_name}.blob.core.windows.net",
            )

            return (
                "WITH S3 (\n"
                '    "provider" = "AZURE",\n'
                f'    "s3.endpoint" = "https://{endpoint}",\n'
                f'    "s3.access_key" = "{account_name}",\n'
                f'    "s3.secret_key" = "{account_key}",\n'
                '    "s3.region" = "auto"\n'
                ")"
            )

        else:
            raise LoadJobTerminalException(
                self._file_path,
                f"Doris Broker Load does not support `{scheme}` scheme with the provided"
                " credentials. Supported: S3 (s3://), GCS (gs://), Azure (az://, abfs://).",
            )

    def _poll_broker_load(self, client: Any, label: str) -> None:
        """Poll SHOW LOAD until the Broker Load job finishes or is cancelled."""
        db_name = client.dataset_name
        poll_interval = self._config.broker_load_poll_interval
        timeout = self._config.broker_load_timeout
        start_time = time.monotonic()

        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > timeout:
                raise LoadJobTerminalException(
                    self._file_path,
                    f"Doris Broker Load job '{label}' timed out after {timeout}s",
                )

            show_sql = f'SHOW LOAD FROM `{db_name}` WHERE LABEL = "{label}"'
            with client.begin_transaction():
                result = client.execute_sql(show_sql)

            if result:
                # Result is a list of rows; take the first
                row = result[0] if isinstance(result, list) else result
                # The State column varies by Doris version but is typically index 2
                # We try to access it as a dict first, then as an index
                if isinstance(row, dict):
                    state = row.get("State", row.get("state", ""))
                    error_msg = row.get("ErrorMsg", row.get("errormsg", ""))
                else:
                    # Assume tuple format: (JobId, Label, State, ...)
                    state = row[2] if len(row) > 2 else ""
                    error_msg = row[7] if len(row) > 7 else ""

                if state == "FINISHED":
                    logger.info(f"Doris: Broker Load job '{label}' finished successfully")
                    return
                elif state == "CANCELLED":
                    raise LoadJobTerminalException(
                        self._file_path,
                        f"Doris Broker Load job '{label}' was cancelled: {error_msg}",
                    )
                else:
                    logger.debug(
                        f"Doris: Broker Load job '{label}' state: {state}, elapsed: {elapsed:.1f}s"
                    )

            time.sleep(poll_interval)


class DorisMergeJob(SqlMergeFollowupJob):
    """Merge job for Doris that generates DELETE statements without table aliases.

    Doris does not support:
    1. Table aliases in DELETE FROM statements (e.g., "DELETE FROM table AS d WHERE ...")
    2. Subqueries inside ORDER BY clauses
    """

    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        """Generate SQL clauses for selecting or deleting rows in root table.

        For DELETE operations, Doris 2.0+ supports DELETE FROM ... USING syntax which is more efficient
        than subqueries and avoids the "delete command must contain filter" error.
        This syntax requires the table to be a UNIQUE KEY model.
        """
        if for_delete:
            return [
                f"FROM {root_table_name} USING {staging_root_table_name} AS s WHERE"
                f" {' AND '.join([c.format(d=root_table_name, s='s') for c in key_clauses])}"
            ]

        return SqlMergeFollowupJob.gen_key_table_clauses(
            root_table_name, staging_root_table_name, key_clauses, for_delete
        )

    @classmethod
    def gen_upsert_sql(
        cls, table_chain: Sequence[PreparedTableSchema], sql_client: Any
    ) -> List[str]:
        """Override to exclude primary/merge keys from MERGE INTO ... UPDATE SET clause.
        Doris throws: Only value columns of unique table could be updated.
        """
        from dlt.common.schema.utils import get_columns_names_with_prop
        from dlt.common.destination import DestinationCapabilitiesContext

        sql: List[str] = []
        root_table = table_chain[0]
        root_table_name, staging_root_table_name = sql_client.get_qualified_table_names(
            root_table["name"]
        )
        escape_column_id = sql_client.escape_column_name
        escape_lit = sql_client.capabilities.escape_literal
        if escape_lit is None:
            escape_lit = DestinationCapabilitiesContext.generic_capabilities().escape_literal

        # process table hints
        primary_keys = cls._escape_list(
            get_columns_names_with_prop(root_table, "primary_key"),
            escape_column_id,
        )
        hard_delete_col, deleted_cond = cls._get_hard_delete_col_and_cond(
            root_table,
            escape_column_id,
            escape_lit,
        )

        # generate merge statement for root table
        root_table_column_names = list(map(escape_column_id, root_table["columns"]))
        sql.extend(
            cls.gen_upsert_merge_sql(
                root_table_name,
                staging_root_table_name,
                primary_keys,
                root_table_column_names,
                hard_delete_col,
                deleted_cond,
            )
        )

        # generate statements for nested tables if they exist
        nested_tables = table_chain[1:]
        if nested_tables:
            root_row_key_column = escape_column_id(
                cls.get_row_key_col(
                    table_chain,
                    root_table,
                    sql_client.fully_qualified_dataset_name(),
                    sql_client.fully_qualified_dataset_name(staging=True),
                )
            )
            for table in nested_tables:
                nested_row_key_column = escape_column_id(
                    cls.get_row_key_col(
                        table_chain,
                        table,
                        sql_client.fully_qualified_dataset_name(),
                        sql_client.fully_qualified_dataset_name(staging=True),
                    )
                )
                root_key_column = escape_column_id(
                    cls.get_root_key_col(
                        table_chain,
                        table,
                        sql_client.fully_qualified_dataset_name(),
                        sql_client.fully_qualified_dataset_name(staging=True),
                    )
                )
                table_name, staging_table_name = sql_client.get_qualified_table_names(table["name"])

                # delete records for elements no longer in the list
                sql.append(f"""
                    DELETE FROM {table_name}
                    WHERE {root_key_column} IN (SELECT {root_row_key_column} FROM {staging_root_table_name})
                    AND {nested_row_key_column} NOT IN (SELECT {nested_row_key_column} FROM {staging_table_name});
                """)

                # insert records for new elements in the list
                table_column_names = list(map(escape_column_id, table["columns"]))
                # Doris Cannot update nested_row_key_column because it's part of the UNIQUE KEY (primary key)
                update_columns = [c for c in table_column_names if c != nested_row_key_column]
                update_str = ", ".join([c + " = " + "s." + c for c in update_columns])
                col_str = ", ".join(["{alias}" + c for c in table_column_names])

                # If there are no columns to update, we must fallback to NOT MATCHED ... INSERT only
                when_matched = ""
                if update_str:
                    when_matched = f"WHEN MATCHED THEN UPDATE SET {update_str}"

                sql.append(f"""
                    MERGE INTO {table_name} d USING {staging_table_name} s
                    ON d.{nested_row_key_column} = s.{nested_row_key_column}
                    {when_matched}
                    WHEN NOT MATCHED
                        THEN INSERT ({col_str.format(alias="")}) VALUES ({col_str.format(alias="s.")});
                """)

                # delete hard-deleted records
                if hard_delete_col is not None:
                    sql.append(f"""
                        DELETE FROM {table_name}
                        WHERE {root_key_column} IN (
                            SELECT {root_row_key_column}
                            FROM {staging_root_table_name}
                            WHERE {deleted_cond}
                        );
                    """)
        return sql

    @classmethod
    def gen_upsert_merge_sql(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        primary_keys: Sequence[str],
        root_table_column_names: Sequence[str],
        hard_delete_col: Optional[str],
        deleted_cond: Optional[str],
    ) -> List[str]:
        """Generate MERGE statement for upsert on root table, excluding primary keys from UPDATE."""
        sql: List[str] = []
        on_str = " AND ".join([f"d.{c} = s.{c}" for c in primary_keys])

        # Doris throws: Only value columns of unique table could be updated.
        # So we filter out the primary keys from the update clause.
        update_columns = [c for c in root_table_column_names if c not in primary_keys]
        update_str = ", ".join([c + " = " + "s." + c for c in update_columns])
        col_str = ", ".join(["{alias}" + c for c in root_table_column_names])
        delete_str = (
            "" if hard_delete_col is None else f"WHEN MATCHED AND s.{deleted_cond} THEN DELETE"
        )

        when_matched = ""
        if update_str:
            when_matched = f"WHEN MATCHED THEN UPDATE SET {update_str}"

        sql.append(f"""
            MERGE INTO {root_table_name} d USING {staging_root_table_name} s
            ON {on_str}
            {delete_str}
            {when_matched}
            WHEN NOT MATCHED
                THEN INSERT ({col_str.format(alias="")}) VALUES ({col_str.format(alias="s.")});
        """)
        return sql


class DorisSqlClient(SqlalchemyClient):
    def truncate_tables(self, *tables: str) -> None:
        """Truncate tables in Doris using TRUNCATE TABLE command.

        Doris requires a filter in DELETE statements, so TRUNCATE is used instead for efficiency
        and compatibility.
        """
        from dlt.destinations.exceptions import DatabaseUndefinedRelation

        for table in tables:
            try:
                self.execute_sql(f"TRUNCATE TABLE {self.make_qualified_table_name(table)}")
            except DatabaseUndefinedRelation:
                pass


class DorisClient(InsertValuesJobClient, SqlalchemyJobClient, SupportsStagingDestination):
    """Apache Doris destination client.

    Uses SQLAlchemy with mysql+pymysql driver. Supports:
    - INSERT INTO VALUES for local file loading
    - Broker Load for staged file loading from S3/GCS/Azure
    - MERGE INTO and SCD2 merge strategies
    - Staging-optimized replace via ALTER TABLE REPLACE WITH TABLE
    """

    def __init__(
        self,
        schema: Schema,
        config: DorisClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: DorisClientConfiguration = config
        # Overwrite sql_client with Doris-specific one that uses TRUNCATE TABLE
        self.sql_client = DorisSqlClient(
            self.sql_client.dataset_name,
            self.sql_client.staging_dataset_name,
            config.credentials,
            capabilities,
        )

    def _to_column_object(
        self, column: TColumnSchema, schema_table: PreparedTableSchema
    ) -> sa.Column:
        """Override to remove primary_key flag to prevent SQLAlchemy auto-adding PrimaryKeyConstraint.
        Doris uses UNIQUE KEY model via mysql_engine suffix instead.
        """
        col = super()._to_column_object(column, schema_table)
        if self.config.create_indexes and getattr(col, "primary_key", False):
            # Doris UNIQUE KEY model handles the uniqueness/indexing.
            # We don't want standard PRIMARY KEY to be added.
            col.primary_key = False
        return col

    def _to_table_object(self, schema_table: PreparedTableSchema) -> sa.Table:
        """Convert a dlt schema table to a SQLAlchemy Table object.

        Handles Doris-specific requirements:
        - Primary key columns must appear before other columns
        - UNIQUE KEY model is configured via mysql_engine suffix
        - Autoincrement is disabled for all columns
        """
        existing = self.sql_client.get_existing_table(schema_table["name"])
        if existing is not None:
            existing_col_names = set(col.name for col in existing.columns)
            new_col_names = set(schema_table["columns"])
            if existing_col_names == new_col_names:
                return existing

        # Build Column objects
        table_columns = [
            self._to_column_object(col, schema_table)
            for col in schema_table["columns"].values()
            if is_complete_column(col)
        ]

        pk_columns = get_columns_names_with_prop(schema_table, "primary_key")

        table_kwargs = {}
        if pk_columns and self.config.create_indexes:
            # Doris requires primary key columns before other columns
            pk_col_objects = [
                self._to_column_object(schema_table["columns"][c], schema_table) for c in pk_columns
            ]
            other_col_objects = [c for c in table_columns if c.name not in pk_columns]
            table_columns = pk_col_objects + other_col_objects
            # Doris does not accept PRIMARY KEY (...) inside table columns.
            # Instead, we inject UNIQUE KEY(...) into the engine string to append it to the DDL.
            pk_str = ", ".join(f"`{c.name}`" for c in pk_col_objects)
            table_kwargs["mysql_engine"] = (
                f"olap\nUNIQUE KEY({pk_str})\nDISTRIBUTED BY HASH({pk_str}) BUCKETS AUTO"
            )

        # Disable autoincrement for all columns (not useful for Doris OLAP)
        for c in table_columns:
            if hasattr(c, "autoincrement"):
                c.autoincrement = False

        table = sa.Table(
            schema_table["name"],
            self.sql_client.metadata,
            *table_columns,
            extend_existing=True,
            schema=self.sql_client.dataset_name,
            **table_kwargs,
        )

        if pk_columns and self.config.create_indexes:
            # SQLAlchemy automatically adds a PrimaryKeyConstraint if any Column has primary_key=True.
            # We must remove it from the table's constraint list to prevent "PRIMARY KEY (col)" from
            # being generated in the DDL, as Doris only accepts "UNIQUE KEY(col)".
            table.constraints = set(
                [c for c in table.constraints if not isinstance(c, sa.PrimaryKeyConstraint)]
            )

        return table

    def update_stored_schema(
        self,
        only_tables: Any = None,
        expected_update: Any = None,
    ) -> Any:
        """Update schema in Doris, handling Doris-specific DDL requirements."""
        from dlt.common.destination.client import JobClientBase
        from dlt.common.schema.typing import TSchemaTables

        JobClientBase.update_stored_schema(self, only_tables, expected_update)

        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is not None:
            logger.info(
                "Schema with hash %s inserted at %s found in storage, no upgrade required",
                self.schema.stored_version_hash,
                schema_info.inserted_at,
            )
        else:
            logger.info(
                "Schema with hash %s not found in storage, upgrading",
                self.schema.stored_version_hash,
            )

        # Create all schema tables in metadata
        for table_name in only_tables or self.schema.tables:
            self._to_table_object(self.schema.tables[table_name])  # type: ignore[arg-type]

        schema_update: TSchemaTables = {}
        tables_to_create: List[sa.Table] = []
        columns_to_add: List[sa.Column] = []

        for table_name in only_tables or self.schema.tables:
            table = self.schema.tables[table_name]
            table_obj, new_columns, exists = self.sql_client.compare_storage_table(table["name"])
            if not new_columns:
                continue
            if not exists:
                logger.debug(f"Will create table {table_name} with {len(new_columns)} new columns")
                tables_to_create.append(table_obj)
            else:
                logger.debug(f"Will ALTER table {table_name} with {len(new_columns)} new columns")
                columns_to_add.extend(new_columns)
            partial_table = self.prepare_load_table(table_name)
            new_column_names = set(col.name for col in new_columns)
            partial_table["columns"] = {
                col_name: col_def
                for col_name, col_def in partial_table["columns"].items()
                if col_name in new_column_names
            }
            schema_update[table_name] = partial_table

        with self.sql_client.begin_transaction():
            for table_obj in tables_to_create:
                # Add Doris-specific UNIQUE KEY/DUPLICATE KEY to table creation
                # We do not set mysql_engine=None here as it causes a TypeError in sqlalchemy compiler
                self.sql_client.create_table(table_obj)
            self.sql_client.alter_table_add_columns(columns_to_add)
            self._update_schema_in_storage(self.schema)

        return schema_update

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        """Create a load job for the given file and table.

        For .reference files (staged), use DorisBrokerLoadJob.
        For all other files, fall back to SQLAlchemy base (insert_values).
        """
        job = None

        if file_path.endswith(".reference"):
            if self.config.staging_config:
                staging_credentials = self.config.staging_config.credentials
                if isinstance(
                    staging_credentials,
                    (
                        AwsCredentialsWithoutDefaults,
                        GcpCredentials,
                        AzureCredentialsWithoutDefaults,
                    ),
                ):
                    job = DorisBrokerLoadJob(file_path, table, self.config, staging_credentials)
        elif file_path.endswith(".parquet"):
            if self.config.staging_config:
                staging_credentials = self.config.staging_config.credentials
                if isinstance(
                    staging_credentials,
                    (
                        AwsCredentialsWithoutDefaults,
                        GcpCredentials,
                        AzureCredentialsWithoutDefaults,
                    ),
                ):
                    job = DorisBrokerLoadJob(file_path, table, self.config, staging_credentials)

        if job is not None:
            return job

        # Fall back to SQLAlchemy base (insert_values)
        return super().create_load_job(table, file_path, load_id, restore)

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List["FollowupJobRequest"]:
        """Create merge followup jobs using DorisMergeJob.

        Doris does not support table aliases in DELETE FROM statements,
        so we use a custom merge job.
        """
        return [DorisMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _create_append_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List["FollowupJobRequest"]:
        return []
