import dataclasses
from typing import ClassVar, Final, List, Optional

from dlt.common.configuration import configspec
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyCredentials,
    SqlalchemyClientConfiguration,
)


@configspec(init=False)
class DorisCredentials(SqlalchemyCredentials):
    drivername: str = "mysql+pymysql"
    host: str = "localhost"
    port: int = 9030
    username: str = "root"
    password: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = [
        "host",
        "port",
        "username",
        "password",
        "database",
    ]


@configspec
class DorisClientConfiguration(
    SqlalchemyClientConfiguration, DestinationClientDwhWithStagingConfiguration
):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="doris", init=False, repr=False, compare=False
    )
    credentials: DorisCredentials = None

    create_indexes: bool = True
    """Whether UNIQUE KEY constraints should be created for primary key columns. Defaults to True."""

    broker_load_timeout: int = 3600
    """Timeout in seconds for Broker Load jobs. Defaults to 3600 (1 hour)."""

    broker_load_poll_interval: float = 5.0
    """Polling interval in seconds when checking Broker Load job status. Defaults to 5.0."""

    broker_load_max_filter_ratio: float = 0.0
    """Maximum ratio of rows that can be filtered (errored) during Broker Load.
    0.0 means strict (no errors allowed). Defaults to 0.0."""

    broker_load_access_key: Optional[TSecretStrValue] = None
    """Access key used in the Broker Load WITH clause. When set, overrides the key
    derived automatically from staging credentials. Required when the staging filesystem
    uses credentials that don't match the expected type for the bucket scheme (e.g.
    HMAC keys for a gs:// bucket stored as aws_access_key_id)."""

    broker_load_secret_key: Optional[TSecretStrValue] = None
    """Secret key used in the Broker Load WITH clause. Must be set together with
    broker_load_access_key to take effect."""

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""
