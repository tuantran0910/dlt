from typing import Any, Dict, Optional

from dlt.destinations.impl.risingwave.typing import (
    TABLE_APPEND_ONLY_HINT,
    TABLE_ENGINE_HINT,
    TABLE_PROPERTIES_HINT,
    TTableEngine,
    TTableProperties,
)
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


def risingwave_adapter(
    data: Any,
    table_engine: Optional[TTableEngine] = None,
    append_only: bool = False,
    table_properties: Optional[TTableProperties] = None,
) -> DltResource:
    """Adapts the given data by applying Risingwave-specific hints.

    This adapter allows you to configure Risingwave-specific table properties
    such as the table engine (e.g., iceberg), append-only mode, and custom
    table properties for the WITH clause.

    Args:
        data: The data to be transformed. Can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        table_engine: Table engine type (e.g., "iceberg"). Defaults to None.
            Only "iceberg" is currently supported.
        append_only: Whether to add APPEND ONLY clause to the table.
            Defaults to False.
        table_properties: Dictionary of table properties for the WITH clause.
            These are engine-specific properties. For Iceberg, you might set
            properties like timeline.timestamp_column.

    Returns:
        DltResource: A resource with applied Risingwave-specific hints.

    Raises:
        ValueError: If table_engine is not "iceberg".

    Examples:
        Set table engine to iceberg:

        >>> data = [{"name": "Alice", "description": "Software Developer"}]
        >>> risingwave_adapter(data, table_engine="iceberg")

        Set table engine with append-only:

        >>> risingwave_adapter(data, table_engine="iceberg", append_only=True)

        Set table properties:

        >>> risingwave_adapter(
        >>>     data,
        >>>     table_engine="iceberg",
        >>>     table_properties={"timeline.timestamp_column": "timestamp"}
        >>> )

        Combine all options:

        >>> risingwave_adapter(
        >>>     data,
        >>>     table_engine="iceberg",
        >>>     append_only=True,
        >>>     table_properties={"timeline.timestamp_column": "created_at"}
        >>> )
    """
    resource = get_resource_for_adapter(data)
    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    # Validate table_engine
    if table_engine is not None:
        if table_engine != "iceberg":
            raise ValueError(
                f"table_engine must be 'iceberg'. Got: {table_engine}. "
                "Currently only 'iceberg' engine is supported."
            )
        additional_table_hints[TABLE_ENGINE_HINT] = table_engine

    # Set append_only
    if append_only:
        additional_table_hints[TABLE_APPEND_ONLY_HINT] = True

    # Set table_properties
    if table_properties:
        additional_table_hints[TABLE_PROPERTIES_HINT] = table_properties

    resource.apply_hints(additional_table_hints=additional_table_hints)
    return resource
