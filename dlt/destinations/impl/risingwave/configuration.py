import dataclasses
from typing import ClassVar, Final, List, Optional

from dlt.common.configuration import configspec
from dlt.common.utils import digest128
from dlt.destinations.impl.postgres.configuration import (
    PostgresCredentials,
    PostgresClientConfiguration,
)


@configspec(init=False)
class RisingwaveCredentials(PostgresCredentials):
    drivername: Final[str] = dataclasses.field(  # type: ignore
        default="postgresql", init=False, repr=False, compare=False
    )
    port: int = 4566

    __config_gen_annotations__: ClassVar[List[str]] = ["port"]


@configspec
class RisingwaveClientConfiguration(PostgresClientConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="risingwave", init=False, repr=False, compare=False
    )
    credentials: RisingwaveCredentials = None

    table_engine: Optional[str] = None
    """Default table engine to use for Risingwave tables. Defaults to None (uses Risingwave default).
       Supported values: 'iceberg'."""

    create_indexes: bool = False
    """Whether PRIMARY KEY constraints should be created. Defaults to False."""

    staging_table_name_suffix: Optional[str] = None
    """Optional suffix to append to table names in staging dataset (e.g., '_staging').
       Internal dlt tables (_dlt_*) will NOT be suffixed."""

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)

        return ""
