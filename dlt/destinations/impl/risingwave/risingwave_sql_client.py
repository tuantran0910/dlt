from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient, psycopg2


class RisingwaveSqlClient(Psycopg2SqlClient):
    """SQL client for Risingwave that handles Risingwave-specific behaviors.

    Risingwave does not support TRUNCATE TABLE, so we override truncate_tables
    to use DELETE FROM instead.
    """

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
        self.execute_many(statements)

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
