from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING, cast, Union
from urllib.parse import ParseResult, urlparse

from dlt.common import logger
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    GcpCredentials,
    AzureCredentialsWithoutDefaults,
    CredentialsWithDefault,
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
from dlt.common.schema.utils import get_columns_names_with_prop, is_nested_table
from dlt.common.storages import FileStorage
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.postgres.configuration import PostgresClientConfiguration
from dlt.destinations.impl.risingwave.configuration import RisingwaveClientConfiguration
from dlt.destinations.impl.risingwave.risingwave_sql_client import RisingwaveSqlClient
from dlt.destinations.impl.risingwave.typing import (
    TABLE_APPEND_ONLY_HINT,
    TABLE_ENGINE_HINT,
    TABLE_PROPERTIES_HINT,
    TTableProperties,
)
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.path_utils import get_file_format_and_compression
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.common.configuration.specs import (
    GcpCredentials,
    AzureCredentialsWithoutDefaults,
    CredentialsWithDefault,
)

if TYPE_CHECKING:
    from dlt.common.destination.capabilities import DestinationCapabilitiesContext


TStagingCredentials = Union[
    AwsCredentialsWithoutDefaults,
    GcpCredentials,
    AzureCredentialsWithoutDefaults,
]


class RisingwaveLoadJob(RunnableLoadJob, HasFollowupJobs):
    """Load job for reading data from object storage and inserting into Risingwave.

    Risingwave supports loading from multiple object storages via the `file_scan()` table function.
    Supported: S3, Google Cloud Storage (GCS), and Azure Blob Storage.

    Note: Risingwave's file_scan function only supports parquet format for staging loads.
    """

    SUPPORTED_FILE_FORMATS = ["parquet"]
    FILE_FORMAT_TO_RISINGWAVE_FORMAT_MAPPING: Dict[str, str] = {
        "parquet": "parquet",
    }

    def __init__(
        self,
        file_path: str,
        config: RisingwaveClientConfiguration,
        staging_credentials: TStagingCredentials,
    ) -> None:
        super().__init__(file_path)
        self._job_client: Optional["RisingwaveClient"] = None
        self._config = config
        self._staging_credentials = staging_credentials

    def run(self) -> None:
        client = self._job_client.sql_client

        bucket_path = None
        file_name = self._file_name

        if ReferenceFollowupJobRequest.is_reference_job(self._file_path):
            bucket_path = ReferenceFollowupJobRequest.resolve_reference(self._file_path)
            file_name = FileStorage.get_file_name_from_file_path(bucket_path)
            bucket_url = urlparse(bucket_path)
        else:
            raise LoadJobTerminalException(
                self._file_path,
                "Risingwave load job only supports staged files from object storage",
            )

        file_format, _ = get_file_format_and_compression(file_name)
        if file_format not in self.SUPPORTED_FILE_FORMATS:
            raise LoadJobTerminalException(
                self._file_path,
                f"Risingwave loader does not support `{file_format}` file format. "
                "Only parquet is supported for staging loads. "
                "Make sure your pipeline is configured to use parquet for staging.",
            )

        risingwave_format: str = self.FILE_FORMAT_TO_RISINGWAVE_FORMAT_MAPPING[file_format]

        # Build the appropriate table function based on storage provider
        table_function = self._build_table_function(bucket_url, file_name, risingwave_format)

        qualified_table_name = client.make_qualified_table_name(self.load_table_name)
        statement = f"INSERT INTO {qualified_table_name} SELECT * FROM {table_function}"

        with client.begin_transaction():
            client.execute_sql(statement)

    def _build_table_function(
        self, bucket_url: ParseResult, file_name: str, risingwave_format: str
    ) -> str:
        """Build the appropriate file_scan() table function based on storage provider.

        Sources:
        - S3: https://docs.risingwave.com/integrations/sources/s3
        - GCS: https://docs.risingwave.com/integrations/sources/google-cloud-storage
        - Azure Blob: https://docs.risingwave.com/integrations/sources/azure-blob
        """
        scheme = bucket_url.scheme

        if scheme == "s3" and isinstance(self._staging_credentials, AwsCredentialsWithoutDefaults):
            # S3: file_scan('parquet', 's3', region, access_key, secret_key, file_location)
            region = getattr(self._staging_credentials, "region_name", None)
            if not region:
                raise LoadJobTerminalException(
                    self._file_path,
                    "S3 credentials must include 'region_name' for Risingwave file_scan function",
                )
            access_key_id = self._staging_credentials.aws_access_key_id
            secret_access_key = self._staging_credentials.aws_secret_access_key

            s3_url = f"{bucket_url.scheme}://{bucket_url.netloc}{bucket_url.path}"
            return (
                f"file_scan('{risingwave_format}', 's3', '{region}', "
                f"'{access_key_id}', '{secret_access_key}', '{s3_url}')"
            )

        elif scheme == "gs" and isinstance(self._staging_credentials, GcpCredentials):
            # GCS: file_scan('parquet', 'gcs', credential, service_account, file_location)
            # Check for Application Default Credentials (ADC)
            uses_adc = (
                isinstance(self._staging_credentials, CredentialsWithDefault)
                and self._staging_credentials.has_default_credentials()
            )

            # For OAuth credentials, the token is in the 'token' field
            token = getattr(self._staging_credentials, "token", None)

            # For service account credentials, convert to JSON string
            # to_native_representation() returns json.dumps(dict(self))
            service_account_json = None
            if not uses_adc and not token:
                try:
                    service_account_json = self._staging_credentials.to_native_representation()
                except Exception:
                    service_account_json = None

            gcs_url = f"{bucket_url.scheme}://{bucket_url.netloc}{bucket_url.path}"
            parts = [f"'{risingwave_format}'", "'gcs'"]

            # Use ADC (empty string), OAuth token, or service account key
            if uses_adc:
                # Application Default Credentials - pass empty string
                parts.append("''")
            elif token:
                # OAuth token from GcpOAuthCredentials
                parts.append(f"'{token}'")
            elif service_account_json:
                # Service account key (JSON string)
                parts.append(f"'{service_account_json}'")
            else:
                raise LoadJobTerminalException(
                    self._file_path,
                    "GCS credentials: must use Application Default Credentials (ADC), provide"
                    " 'token' for OAuth, or service account JSON for authentication. Configure"
                    " with: gcloud auth application-default login or set"
                    " GOOGLE_APPLICATION_CREDENTIALS",
                )

            parts.append(f"'{gcs_url}'")
            return f"file_scan({', '.join(parts)})"

        elif (scheme == "az" or scheme == "abfs") and isinstance(
            self._staging_credentials, AzureCredentialsWithoutDefaults
        ):
            # Azure Blob: file_scan('parquet', 'azblob', account_name, account_key, endpoint, file_location)
            account_name = getattr(self._staging_credentials, "azure_storage_account_name", None)
            account_key = getattr(self._staging_credentials, "azure_storage_account_key", None)
            endpoint = getattr(self._staging_credentials, "azure_account_host", None)

            if not account_name or not account_key or not endpoint:
                raise LoadJobTerminalException(
                    self._file_path,
                    "Azure Blob credentials must include 'azure_storage_account_name',"
                    " 'azure_storage_account_key', and 'azure_account_host' for Risingwave"
                    " file_scan function",
                )

            azure_url = f"{bucket_url.scheme}://{bucket_url.netloc}{bucket_url.path}"
            return (
                f"file_scan('{risingwave_format}', 'azblob', '{account_name}', "
                f"'{account_key}', '{endpoint}', '{azure_url}')"
            )

        else:
            provider_name = (
                "AWS"
                if isinstance(self._staging_credentials, AwsCredentialsWithoutDefaults)
                else (
                    "GCP"
                    if isinstance(self._staging_credentials, GcpCredentials)
                    else (
                        "Azure"
                        if isinstance(self._staging_credentials, AzureCredentialsWithoutDefaults)
                        else "unknown"
                    )
                )
            )
            raise LoadJobTerminalException(
                self._file_path,
                f"Risingwave loader does not support `{scheme}` filesystem with `{provider_name}`"
                " credentials. Supported providers: S3 (aws://), GCS (gs://), and Azure Blob"
                " (az://, abfs://).",
            )


class RisingwaveMergeJob(SqlMergeFollowupJob):
    """Merge job for Risingwave that generates DELETE statements without table aliases.

    Risingwave does not support:
    1. Table aliases in DELETE FROM statements (e.g., "DELETE FROM table AS d WHERE ...")
    2. Subqueries inside ORDER BY clauses (e.g., "ORDER BY (SELECT NULL)")

    This class overrides the SQL generation to work around these limitations.
    """

    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table.

        For DELETE operations, Risingwave does not support table aliases, and also doesn't
        correctly resolve fully qualified table names (e.g., "schema"."table") in correlated
        subqueries. We must use just the base table name (without schema qualification)
        for the outer table reference in the WHERE clause.
        """
        if for_delete:
            # Risingwave doesn't support alias in DELETE FROM
            # Extract just the base table name (without schema) from the qualified name
            # root_table_name format: "schema"."table" - we need just "table"
            parts = root_table_name.split('"')
            if len(parts) > 1:
                # Format: "schema"."table" - extract the table name (last quoted part)
                base_table_name = parts[-2] if parts[-1] == "" else parts[-1]
            else:
                # Format: schema.table or table - extract last part
                base_table_name = root_table_name.split(".")[-1]

            return [
                f"FROM {root_table_name} WHERE EXISTS (SELECT 1 FROM"
                f" {staging_root_table_name} WHERE"
                f" {' OR '.join([c.format(d=base_table_name, s=staging_root_table_name) for c in key_clauses])})"
            ]
        return SqlMergeFollowupJob.gen_key_table_clauses(
            root_table_name, staging_root_table_name, key_clauses, for_delete
        )

    @classmethod
    def default_order_by(cls) -> str:
        """Return the ORDER BY clause for deduplication when no specific sort column is provided.

        Risingwave does not support subqueries in ORDER BY (e.g., "ORDER BY (SELECT NULL)"),
        so we use "NULL" directly which provides arbitrary but deterministic ordering.
        """
        return "NULL"


class RisingwaveClient(InsertValuesJobClient, SupportsStagingDestination):
    """Risingwave destination client.

    Risingwave is a Postgres-protocol compatible OLAP database that requires
    special table properties (ENGINE, WITH, APPEND ONLY).
    """

    def __init__(
        self,
        schema: Schema,
        config: RisingwaveClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        dataset_name, staging_dataset_name = SqlJobClientWithStagingDataset.create_dataset_names(
            schema, config
        )
        sql_client = RisingwaveSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
            staging_table_name_suffix=config.staging_table_name_suffix,
            dlt_tables_prefix=schema._dlt_tables_prefix,
        )
        super().__init__(schema, config, sql_client)
        self.config: RisingwaveClientConfiguration = config
        self.sql_client: RisingwaveSqlClient = sql_client
        self.type_mapper = self.capabilities.get_type_mapper()

    def _get_table_properties_clause(self, properties: TTableProperties) -> str:
        """Format table properties dict as a WITH clause string.

        Args:
            properties: Dictionary of table properties

        Returns:
            Formatted WITH clause string (e.g., "key1 = 'value1', key2 = 'value2'")
        """
        clauses = []
        for key, val in properties.items():
            if isinstance(val, str):
                escaped_val = self.capabilities.escape_literal(val)
                clauses.append(f"{key} = {escaped_val}")
            elif isinstance(val, bool):
                clauses.append(f"{key} = {str(val).lower()}")
            else:
                clauses.append(f"{key} = {val}")

        return ", ".join(clauses)

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        """Generate CREATE or ALTER TABLE SQL with Risingwave-specific table properties.

        This method extends the base implementation to add:
        - ENGINE clause (e.g., ENGINE = iceberg)
        - APPEND ONLY clause
        - WITH clause for table properties

        Table properties are only added on CREATE TABLE, not ALTER TABLE.
        """
        table = self.prepare_load_table(table_name)
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        # Only add table properties on CREATE, not ALTER
        if generate_alter:
            return sql

        if table.get(TABLE_APPEND_ONLY_HINT, False):
            sql[0] += "\nAPPEND ONLY"

        if properties := table.get(TABLE_PROPERTIES_HINT):
            properties = cast(TTableProperties, properties)
            properties_clause = self._get_table_properties_clause(properties)
            sql[0] += f"\nWITH ({properties_clause})"

        table_engine = table.get(TABLE_ENGINE_HINT) or getattr(self.config, "table_engine", None)
        if table_engine == "iceberg":
            sql[0] += "\nENGINE = iceberg"

        return sql

    def _get_constraints_sql(
        self,
        table_name: str,
        new_columns: Sequence[TColumnSchema],
        generate_alter: bool,
    ) -> str:
        """Generate PRIMARY KEY constraint SQL for Risingwave tables.

        Risingwave uses standard PostgreSQL PRIMARY KEY syntax.
        """
        if not self.config.create_indexes:
            return ""

        from dlt.common.schema.typing import TTableSchema

        partial: TTableSchema = {
            "name": table_name,
            "columns": {c["name"]: c for c in new_columns},
        }

        pk_columns = get_columns_names_with_prop(partial, "primary_key")

        if not pk_columns:
            return ""

        if generate_alter:
            logger.warning(
                f"PRIMARY KEY on {table_name} constraint cannot be added in ALTER TABLE and is"
                " ignored"
            )
            return ""

        # PRIMARY KEY column names should NOT be quoted
        pk_cols = ", ".join(pk_columns)
        return f",\nPRIMARY KEY ({pk_cols})"

    def _from_db_type(
        self, rw_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(rw_t, precision, scale)

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Create a load job for the given file and table.

        If object storage staging is configured, use Risingwave file_scan for better performance.
        Otherwise, use the parent's implementation (insert values or CSV/Parquet copy).
        """
        job = super().create_load_job(table, file_path, load_id, restore)

        # If no job was created and staging is configured, try staged file load job
        if not job and self.config.staging_config:
            staging_credentials = self.config.staging_config.credentials
            # Check if credentials are supported (AWS, GCP, or Azure)
            if isinstance(
                staging_credentials,
                (AwsCredentialsWithoutDefaults, GcpCredentials, AzureCredentialsWithoutDefaults),
            ):
                job = RisingwaveLoadJob(file_path, self.config, staging_credentials)

        return job

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List["FollowupJobRequest"]:
        """Create merge followup jobs using RisingwaveMergeJob.

        Risingwave does not support table aliases in DELETE FROM statements,
        so we use a custom merge job that generates correct SQL.
        """
        return [RisingwaveMergeJob.from_table_chain(table_chain, self.sql_client)]
