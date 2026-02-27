import pytest

from dlt.destinations.impl.doris.configuration import (
    DorisCredentials,
    DorisClientConfiguration,
)


@pytest.fixture
def doris_config() -> DorisClientConfiguration:
    config = DorisClientConfiguration()
    config.credentials = DorisCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 9030
    config.credentials.username = "root"
    return config


def test_doris_credentials() -> None:
    """Test DorisCredentials initialization with default values."""
    credentials = DorisCredentials()
    credentials.database = "test_db"
    credentials.host = "localhost"
    credentials.password = "password"
    credentials.port = 9030
    credentials.username = "root"

    assert credentials.database == "test_db"
    assert credentials.host == "localhost"
    assert credentials.port == 9030
    assert credentials.username == "root"
    assert credentials.password == "password"
    assert credentials.drivername == "mysql+pymysql"


def test_doris_credentials_default_port() -> None:
    """Test DorisCredentials default port is 9030 (MySQL protocol)."""
    credentials = DorisCredentials()
    assert credentials.port == 9030


def test_doris_credentials_default_username() -> None:
    """Test DorisCredentials default username."""
    credentials = DorisCredentials()
    assert credentials.username == "root"


def test_doris_client_configuration(doris_config: DorisClientConfiguration) -> None:
    """Test DorisClientConfiguration initialization."""
    assert doris_config.destination_type == "doris"
    assert doris_config.credentials.database == "test_db"
    assert doris_config.credentials.port == 9030


def test_doris_client_configuration_create_indexes_default() -> None:
    """Test that create_indexes defaults to False."""
    config = DorisClientConfiguration()
    config.credentials = DorisCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 9030
    config.credentials.username = "root"

    assert config.create_indexes is False


def test_doris_client_configuration_create_indexes_enabled() -> None:
    """Test DorisClientConfiguration with create_indexes=True."""
    config = DorisClientConfiguration()
    config.credentials = DorisCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.password = "password"
    config.credentials.port = 9030
    config.credentials.username = "root"
    config.create_indexes = True

    assert config.create_indexes is True


def test_doris_client_configuration_broker_load_defaults() -> None:
    """Test DorisClientConfiguration Broker Load default values."""
    config = DorisClientConfiguration()
    config.credentials = DorisCredentials()
    config.credentials.database = "test_db"
    config.credentials.host = "localhost"
    config.credentials.username = "root"

    assert config.broker_load_timeout == 3600
    assert config.broker_load_poll_interval == 5.0
    assert config.broker_load_max_filter_ratio == 0.0


def test_doris_config_fingerprint(doris_config: DorisClientConfiguration) -> None:
    """Test DorisClientConfiguration fingerprint."""
    fingerprint = doris_config.fingerprint()
    assert isinstance(fingerprint, str)
    assert len(fingerprint) > 0


def test_doris_config_fingerprint_empty() -> None:
    """Test DorisClientConfiguration fingerprint with no host."""
    config = DorisClientConfiguration()
    config.credentials = None
    assert config.fingerprint() == ""
