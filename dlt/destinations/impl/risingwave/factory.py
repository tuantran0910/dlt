from typing import Any, Dict, Optional, Type, Union, TYPE_CHECKING, Sequence

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_postgres_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema, TTableSchema
from dlt.common.typing import TLoaderFileFormat
from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.destinations.impl.postgres.factory import PostgresTypeMapper
from dlt.destinations.impl.risingwave.configuration import (
    RisingwaveClientConfiguration,
    RisingwaveCredentials,
)


def _risingwave_file_format_selector(
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    """Custom file format selector for Risingwave.

    Risingwave supports parquet via its native file_scan() function, not via ADBC.
    This selector ensures parquet is always available regardless of ADBC driver
    availability, since Risingwave's file_scan() can read parquet files directly
    from object storage (S3, GCS, Azure).

    For insert_values, csv, and model formats, we use the default behavior.
    """
    # Convert to list to allow modification
    supported_formats = list(supported_loader_file_formats)

    # Always ensure parquet is in the supported formats for Risingwave
    # file_scan() works natively without ADBC
    if "parquet" not in supported_formats:
        supported_formats.append("parquet")

    # Use the default preferred format (insert_values) for Risingwave
    # This ensures we don't force parquet when it's not appropriate
    # The staging system will use parquet separately
    return (preferred_loader_file_format, supported_formats)


if TYPE_CHECKING:
    from dlt.destinations.impl.risingwave.risingwave import RisingwaveClient


class RisingwaveTypeMapper(PostgresTypeMapper):
    """Risingwave type mapper that handles differences from PostgreSQL.

    Key differences from PostgreSQL:
    - varchar: Does NOT support length specification (only varchar, not varchar(255))
    - numeric/decimal: Does NOT support precision/scale specification
    - timestamp: Uses timestamptz and timestamp (does not support precision specification)
    - json: Uses JSONB type

    Reference: https://docs.risingwave.com/sql/data-types/overview
    """

    # Override sct_to_unbound_dbt for types that don't support precision/scale
    sct_to_unbound_dbt = dict(PostgresTypeMapper.sct_to_unbound_dbt)
    sct_to_unbound_dbt.update(
        {
            "decimal": "numeric",
            "wei": "numeric",
            "text": "varchar",
        }
    )

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        """Override to handle types that don't support precision/scale in Risingwave.

        Risingwave does NOT support:
        - varchar(n) - only varchar without length
        - numeric(p,s) - only numeric without precision/scale
        - timestamp(p) - only timestamp without precision specification
        """
        sc_t = column["data_type"]

        # For decimal and wei, always return numeric without precision/scale
        # Risingwave does not support specifying precision and scale
        if sc_t in ("decimal", "wei"):
            return "numeric"

        # For text, return varchar without length
        # Risingwave does not support specifying length for varchar
        if sc_t == "text":
            return "varchar"

        # For all other types, use the parent implementation
        return super().to_destination_type(column, table)

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> Optional[str]:
        """Override to return timestamp types without precision specification.

        Risingwave does not support specifying precision for timestamp types.

        Args:
            column (TColumnSchema): Column schema
            table (PreparedTableSchema): Table schema

        Returns:
            Optional[str]: "timestamp with time zone" if timezone is True or None,
            "timestamp without time zone" if timezone is False
        """
        timezone = column.get("timezone")
        if timezone is None or timezone:
            return "timestamp with time zone"
        else:
            return "timestamp without time zone"


class risingwave(Destination[RisingwaveClientConfiguration, "RisingwaveClient"]):
    spec = RisingwaveClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        # Risingwave is Postgres-protocol compatible, so capabilities are similar to Postgres
        # https://www.risingwave.com/docs/
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "csv", "parquet", "model"]
        # Use custom file format selector that keeps parquet available for file_scan()
        caps.loader_file_format_selector = _risingwave_file_format_selector
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet"]
        caps.type_mapper = RisingwaveTypeMapper
        caps.escape_identifier = escape_postgres_identifier
        # Risingwave has case sensitive identifiers but by default
        # it folds them to lower case which makes them case insensitive
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = True
        caps.escape_literal = escape_postgres_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (2 * EVM_DECIMAL_PRECISION, EVM_DECIMAL_PRECISION)
        caps.max_identifier_length = 63
        caps.max_column_identifier_length = 63
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]
        caps.sqlglot_dialect = "postgres"

        return caps

    @property
    def client_class(self) -> Type["RisingwaveClient"]:
        from dlt.destinations.impl.risingwave.risingwave import RisingwaveClient

        return RisingwaveClient

    def __init__(
        self,
        credentials: Union[RisingwaveCredentials, Dict[str, Any], str] = None,
        create_indexes: bool = False,
        csv_format: Optional[CsvFormatConfiguration] = None,
        table_engine: Optional[str] = None,
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Risingwave destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[RisingwaveCredentials, Dict[str, Any], str]): Credentials to connect to the risingwave database. Can be an instance of `RisingwaveCredentials` or
                a connection string in the format `risingwave://user:password@host:4566/database`
            create_indexes (bool): Should PRIMARY KEY constraints be created. Defaults to False.
            csv_format (Optional[CsvFormatConfiguration]): Formatting options for csv file format
            table_engine (Optional[str]): Default table engine to use (e.g., "iceberg"). Defaults to None.
            destination_name (Optional[str]): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (Optional[str]): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            csv_format=csv_format,
            table_engine=table_engine,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


risingwave.register()
