from typing import Any, Dict, Optional, Type, Union, TYPE_CHECKING, Sequence

from dlt.common.normalizers import NamingConvention

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import (
    escape_hive_identifier,
)
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.typing import TLoaderFileFormat
from dlt.destinations.impl.sqlalchemy.type_mapper import (
    SqlalchemyTypeMapper,
    MysqlVariantTypeMapper,
)
from sqlalchemy.sql import sqltypes
import sqlalchemy as sa
from dlt.destinations.impl.doris.configuration import (
    DorisClientConfiguration,
    DorisCredentials,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.doris.doris import DorisClient


# Doris uses MySQL-style backtick escaping for identifiers
escape_doris_identifier = escape_hive_identifier


def escape_doris_literal(v: Any) -> Any:
    """Escape literals for Apache Doris (MySQL-compatible)."""
    from datetime import date, datetime, time  # noqa: I251

    from dlt.common.json import json

    if isinstance(v, str):
        # MySQL-style single quote escaping: ' -> ''
        return "'" + v.replace("'", "''").replace("\\", "\\\\") + "'"
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return "'" + json.dumps(v).replace("'", "''").replace("\\", "\\\\") + "'"
    if isinstance(v, bytes):
        return f"X'{v.hex()}'"
    if isinstance(v, bool):
        return str(int(v))
    if v is None:
        return "NULL"
    return str(v)


class DorisTypeMapper(MysqlVariantTypeMapper):
    """Type mapper for Apache Doris.

    Maps dlt schema types to Doris SQL data types using SQLAlchemy types.
    Doris is MySQL-compatible, so we inherit from MysqlVariantTypeMapper.

    Key mappings:
    - text -> String(precision) or Text (renders as TEXT/LONGTEXT)
    - bigint -> BigInteger
    - double -> mysql.DOUBLE
    - bool -> Boolean
    - timestamp -> mysql.DATETIME(fsp=6)
    - date -> Date
    - decimal -> Numeric(p, s)
    - wei -> Numeric(38, 0)
    - json -> mysql.JSON
    - binary -> LargeBinary
    - time -> String (Doris has no TIME type)

    Reference: https://doris.apache.org/docs/dev/sql-manual/sql-data-types/overview
    """

    def db_type_from_text_type(
        self, column: TColumnSchema, table: PreparedTableSchema = None
    ) -> sqltypes.TypeEngine:
        precision = column.get("precision")
        if precision:
            return sa.String(length=precision)
        return sa.Text()

    def db_type_from_json_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        from sqlalchemy.dialects.mysql import JSON

        return JSON()

    def db_type_from_binary_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        from sqlalchemy.dialects.mysql import LONGBLOB

        return LONGBLOB()

    def db_type_from_decimal_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        from sqlalchemy.dialects.mysql import DECIMAL

        precision, scale = column.get("precision"), column.get("scale")
        if precision is None and scale is None:
            precision, scale = self.capabilities.decimal_precision
        return DECIMAL(precision, scale)

    def db_type_from_wei_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        from sqlalchemy.dialects.mysql import DECIMAL

        return DECIMAL(38, 0)

    def db_type_from_time_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        # Doris has no TIME type, using String
        return sa.String(length=255)


class doris(Destination[DorisClientConfiguration, "DorisClient"]):
    spec = DorisClientConfiguration

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: DorisClientConfiguration,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        """Adjust Doris capabilities. This hook is called by dlt to allow the destination
        to adjust its capabilities based on the configuration.
        """
        # Doris always uses MySQL dialect capabilities for SQLAlchemy
        from dlt.destinations.impl.sqlalchemy.dialect import MysqlDialectCapabilities

        caps.dialect_capabilities = MysqlDialectCapabilities("mysql")

        return super(doris, cls).adjust_capabilities(caps, config, naming)

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet", "csv"]
        caps.type_mapper = DorisTypeMapper
        caps.escape_identifier = escape_doris_identifier
        caps.escape_literal = escape_doris_literal
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (38, 0)
        caps.max_identifier_length = 64
        caps.max_column_identifier_length = 64
        caps.max_query_length = 1024 * 1024  # ~1MB
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 2147483643  # STRING type max
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supports_transactions = True
        caps.alter_add_multi_column = True
        caps.supported_merge_strategies = ["delete-insert", "upsert"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
        ]
        caps.sqlglot_dialect = "doris"
        caps.naming_convention = "dlt.destinations.impl.doris.naming"

        return caps

    @property
    def client_class(self) -> Type["DorisClient"]:
        from dlt.destinations.impl.doris.doris import DorisClient

        return DorisClient

    def __init__(
        self,
        credentials: Union[DorisCredentials, Dict[str, Any], str] = None,
        create_indexes: bool = False,
        broker_load_access_key: Optional[str] = None,
        broker_load_secret_key: Optional[str] = None,
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Apache Doris destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as
        environment variables and dlt config files.

        Args:
            credentials: Doris connection credentials. Can be a DorisCredentials instance,
                a dict, or a connection string like ``mysql+pymysql://user:pass@host:9030/db``.
            create_indexes: Whether UNIQUE KEY constraints should be created for primary
                key columns. Defaults to False.
            broker_load_access_key: Access key for Broker Load operations. When set,
                overrides the automatically-detected key from staging credentials.
                Required when using HMAC keys for GCS or when staging and Broker Load
                credentials differ. Must be set together with broker_load_secret_key.
            broker_load_secret_key: Secret key for Broker Load operations. Must be set
                together with broker_load_access_key to take effect.
            destination_name: Name of the destination.
            environment: Environment of the destination.
            **kwargs: Additional arguments passed to the destination config.
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            broker_load_access_key=broker_load_access_key,
            broker_load_secret_key=broker_load_secret_key,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


doris.register()
