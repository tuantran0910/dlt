import pytest
from dlt.common.schema import Schema
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.risingwave.risingwave_sql_client import RisingwaveSqlClient
from dlt.destinations.impl.risingwave.configuration import RisingwaveCredentials
from dlt.common.schema.typing import VERSION_TABLE_NAME


def test_risingwave_sql_client_custom_staging_naming() -> None:
    credentials = RisingwaveCredentials()
    capabilities = DestinationCapabilitiesContext()
    capabilities.casefold_identifier = lambda x: x
    capabilities.escape_identifier = lambda x: f'"{x}"'

    # 1. Test default behavior (no suffix)
    client = RisingwaveSqlClient(
        dataset_name="test_dataset",
        staging_dataset_name="test_dataset_staging",
        credentials=credentials,
        capabilities=capabilities,
        staging_table_name_suffix=None,
    )

    # Regular table, main dataset
    assert client.make_qualified_table_name("my_table") == '"test_dataset"."my_table"'

    # Regular table, staging dataset (identical name)
    with client.with_staging_dataset():
        assert client.make_qualified_table_name("my_table") == '"test_dataset_staging"."my_table"'

    # 2. Test with suffix
    client_suffix = RisingwaveSqlClient(
        dataset_name="test_dataset",
        staging_dataset_name="test_dataset_staging",
        credentials=credentials,
        capabilities=capabilities,
        staging_table_name_suffix="_staging",
    )

    # Regular table, main dataset
    assert client_suffix.make_qualified_table_name("my_table") == '"test_dataset"."my_table"'

    # Regular table, staging dataset (suffixed name)
    with client_suffix.with_staging_dataset():
        assert (
            client_suffix.make_qualified_table_name("my_table")
            == '"test_dataset_staging"."my_table_staging"'
        )

    # Internal table, staging dataset (should NOT be suffixed)
    with client_suffix.with_staging_dataset():
        assert (
            client_suffix.make_qualified_table_name(VERSION_TABLE_NAME)
            == f'"test_dataset_staging"."{VERSION_TABLE_NAME}"'
        )


def test_qualified_table_names_integration() -> None:
    credentials = RisingwaveCredentials()
    capabilities = DestinationCapabilitiesContext()
    capabilities.casefold_identifier = lambda x: x
    capabilities.escape_identifier = lambda x: f'"{x}"'

    client = RisingwaveSqlClient(
        dataset_name="test_dataset",
        staging_dataset_name="test_dataset_staging",
        credentials=credentials,
        capabilities=capabilities,
        staging_table_name_suffix="_stg",
    )

    qualified, staging_qualified = client.get_qualified_table_names("my_table")
    assert qualified == '"test_dataset"."my_table"'
    assert staging_qualified == '"test_dataset_staging"."my_table_stg"'
