import pytest

from dlt.destinations.impl.risingwave.configuration import (
    RisingwaveCredentials,
    RisingwaveClientConfiguration,
)


@pytest.fixture
def risingwave_config() -> RisingwaveClientConfiguration:
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"
    return config


def test_risingwave_credentials() -> None:
    """Test RisingwaveCredentials initialization."""
    credentials = RisingwaveCredentials()
    credentials.database = "test_db"
    credentials.host = "localhost"
    credentials.password = "password"
    credentials.port = 4566
    credentials.username = "root"

    assert credentials.database == "test_db"
    assert credentials.host == "localhost"
    assert credentials.port == 4566  # Risingwave default port
    assert credentials.username == "root"
    assert credentials.password == "password"
    assert (
        credentials.drivername == "postgresql"
    )  # Uses postgresql driver for psycopg2 compatibility


def test_risingwave_credentials_from_connection_string() -> None:
    """Test RisingwaveCredentials from connection string."""
    conn_str = "risingwave://root:password@localhost:4566/test_db"
    credentials = RisingwaveCredentials()
    credentials.parse_native_representation(conn_str)

    assert credentials.database == "test_db"
    assert credentials.host == "localhost"
    assert credentials.port == 4566
    assert credentials.username == "root"
    assert credentials.password == "password"


def test_risingwave_client_configuration(risingwave_config: RisingwaveClientConfiguration) -> None:
    """Test RisingwaveClientConfiguration initialization."""
    assert risingwave_config.destination_type == "risingwave"
    assert risingwave_config.credentials.database == "test_db"
    assert risingwave_config.credentials.port == 4566


def test_risingwave_client_configuration_table_engine() -> None:
    """Test RisingwaveClientConfiguration with table_engine."""
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"
    config.table_engine = "iceberg"

    assert config.table_engine == "iceberg"


def test_risingwave_client_configuration_create_indexes_default() -> None:
    """Test that create_indexes defaults to False."""
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"

    assert config.create_indexes is False


def test_risingwave_client_configuration_create_indexes_enabled() -> None:
    """Test RisingwaveClientConfiguration with create_indexes=True."""
    config = RisingwaveClientConfiguration()
    config.credentials = RisingwaveCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 4566
    config.credentials.username = "root"
    config.create_indexes = True

    assert config.create_indexes is True


def test_risingwave_config_fingerprint(risingwave_config: RisingwaveClientConfiguration) -> None:
    """Test RisingwaveClientConfiguration fingerprint."""
    fingerprint = risingwave_config.fingerprint()
    assert isinstance(fingerprint, str)
    assert len(fingerprint) > 0


def test_risingwave_config_is_resolved() -> None:
    """Test that configuration can be resolved from environment."""
    config = RisingwaveClientConfiguration()
    assert not config.is_resolved()

    # Partially resolved with credentials
    config.credentials = RisingwaveCredentials()
    assert not config.is_resolved()  # Still not fully resolved


def test_risingwave_credentials_to_native_representation() -> None:
    """Test that RisingwaveCredentials uses postgresql:// scheme for psycopg2 compatibility."""
    credentials = RisingwaveCredentials()
    credentials.database = "test_db"
    credentials.host = "localhost"
    credentials.password = "password"
    credentials.port = 4566
    credentials.username = "root"

    native = credentials.to_native_representation()
    # Risingwave is PostgreSQL-compatible, so it should use the postgresql:// scheme
    assert native.startswith("postgresql://")
    # Should contain the connection details
    assert "localhost:4566" in native or "localhost" in native
    assert "test_db" in native or "/test_db" in native
    # The password should be preserved
    assert ":password@" in native or "root:password" in native
