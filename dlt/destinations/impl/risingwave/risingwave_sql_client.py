from typing import List, Optional, Tuple

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.utils import is_dlt_table_or_column
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.risingwave.configuration import RisingwaveCredentials
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient, psycopg2


class RisingwaveSqlClient(Psycopg2SqlClient):
    """SQL client for Risingwave that handles Risingwave-specific behaviors.

    Risingwave does not support TRUNCATE TABLE, so we override truncate_tables
    to use DELETE FROM instead.
    """

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: RisingwaveCredentials,
        capabilities: DestinationCapabilitiesContext,
        staging_table_name_suffix: Optional[str] = None,
        dlt_tables_prefix: str = "_dlt_",
    ) -> None:
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)
        self.staging_table_name_suffix = staging_table_name_suffix
        self.dlt_tables_prefix = dlt_tables_prefix

    def open_connection(self) -> "psycopg2.connection":
        """Open a connection to RisingWave with RW_IMPLICIT_FLUSH enabled.

        RisingWave requires RW_IMPLICIT_FLUSH=true for INSERT/UPDATE/DELETE operations
        to persist data immediately. Without this, data is written but not visible
        to subsequent queries until an explicit FLUSH is executed.

        See: https://docs.risingwave.com/sql/set-commands/set-rw_implicit_flush
        """
        # Call parent to establish the connection
        conn = super().open_connection()

        # CRITICAL: Enable RW_IMPLICIT_FLUSH for RisingWave to persist data immediately
        # Without this, INSERT/UPDATE/DELETE operations don't make data visible
        with conn.cursor() as cur:
            cur.execute("SET RW_IMPLICIT_FLUSH = true")

        return conn

    def truncate_tables(self, *tables: str) -> None:
        """Truncate tables using DELETE FROM instead of TRUNCATE TABLE.

        Risingwave does not support TRUNCATE TABLE statement, so we use
        DELETE FROM which has the same effect for our purposes.

        Note: self.dataset_name will be set to the staging dataset name when
        truncating staging tables via the with_staging_dataset context manager.
        """
        if not tables:
            return

        # Generate DELETE FROM statements for each table
        # make_qualified_table_name will use self.dataset_name which is set
        # to the staging dataset name when truncating staging tables
        statements = [
            f"DELETE FROM {self.make_qualified_table_name(table_name)}" for table_name in tables
        ]

        # Execute statements, silently skipping tables that don't exist
        for statement in statements:
            try:
                self.execute_sql(statement)
            except DatabaseUndefinedRelation:
                # Table doesn't exist yet - this is expected for first run with
                # staging_table_name_suffix
                pass

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        """Handle Risingwave-specific error codes.

        Risingwave raises UndefinedObject instead of UndefinedTable when a table
        is not found. We need to handle this to return DatabaseUndefinedRelation
        which is properly caught by the pipeline state sync logic.
        """
        if isinstance(
            ex,
            (
                psycopg2.errors.UndefinedTable,
                psycopg2.errors.InvalidSchemaName,
                psycopg2.errors.UndefinedObject,
            ),
        ):
            return DatabaseUndefinedRelation(ex)

        return super()._make_database_exception(ex)

    def make_qualified_table_name_path(
        self, table_name: Optional[str], quote: bool = True, casefold: bool = True
    ) -> List[str]:
        """Override to apply staging suffix to table names when in staging dataset.

        Internal dlt tables (_dlt_*) are NOT suffixed.
        """
        # Apply suffix to table_name BEFORE quoting/casefolding if:
        # - table_name is provided
        # - we're in staging dataset mode
        # - suffix is configured
        # - table is NOT an internal dlt table
        if table_name and self.is_staging_dataset_active and self.staging_table_name_suffix:
            # Check the table name (before casefolding) against dlt prefix
            if not is_dlt_table_or_column(table_name, self.dlt_tables_prefix):
                # Apply suffix to table_name before it gets quoted
                table_name = table_name + self.staging_table_name_suffix

        # Get the base path from parent (will apply quoting/casefolding)
        path = super().make_qualified_table_name_path(table_name, quote=quote, casefold=casefold)
        return path

    def get_qualified_table_names(
        self, table_name: str, quote: bool = True, casefold: bool = True
    ) -> Tuple[str, str]:
        """Returns qualified names for table and corresponding staging table as tuple.

        Example with suffix="_staging":
            main_table: "dataset"."table"
            staging_table: "dataset_staging"."table_staging"

        Internal dlt tables will NOT have the suffix applied.
        """
        main_table = self.make_qualified_table_name(table_name, quote=quote, casefold=casefold)

        with self.with_staging_dataset():
            staging_table = self.make_qualified_table_name(
                table_name, quote=quote, casefold=casefold
            )

        return (main_table, staging_table)
