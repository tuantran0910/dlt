from typing import Any, Dict, List, Optional

from dlt.common.schema import TTableSchema
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate
from dlt.destinations.impl.doris.typing import (
    TABLE_MODEL_HINT,
    TABLE_DISTRIBUTED_BY_HINT,
    TABLE_BUCKETS_HINT,
    TABLE_PROPERTIES_HINT,
    TTableModel,
    TDistributedBy,
    TTableProperties,
)


def doris_adapter(
    data: DltResource,
    table_model: Optional[TTableModel] = None,
    distributed_by: Optional[TDistributedBy] = None,
    buckets: Optional[int] = None,
    table_properties: Optional[TTableProperties] = None,
) -> DltResource:
    """Set Doris-specific table properties on a dlt resource.

    Args:
        data: A dlt resource to which the Doris-specific hints will be added.
        table_model: The Doris table model. One of "duplicate", "unique", or "aggregate".
            Defaults to None (uses Doris default, which is DUPLICATE).
        distributed_by: List of column names for DISTRIBUTED BY HASH(...).
            Defaults to None (Doris will auto-select the first column).
        buckets: Number of hash buckets for distribution.
            Defaults to None (Doris will use AUTO).
        table_properties: Dictionary of key-value pairs for the PROPERTIES clause.
            Example: {"replication_num": "1"}.

    Returns:
        The modified dlt resource with Doris-specific hints applied.

    Raises:
        ValueError: If an invalid table_model is provided.

    Example:
        >>> from dlt.destinations.impl.doris.doris_adapter import doris_adapter
        >>> @dlt.resource
        ... def my_data():
        ...     yield {"id": 1, "name": "Alice"}
        >>> doris_adapter(my_data, table_model="unique", distributed_by=["id"], buckets=8)
    """
    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    if table_model is not None:
        valid_models = ("duplicate", "unique", "aggregate")
        if table_model not in valid_models:
            raise ValueError(f"Invalid table_model '{table_model}'. Must be one of: {valid_models}")
        additional_table_hints[TABLE_MODEL_HINT] = table_model

    if distributed_by is not None:
        additional_table_hints[TABLE_DISTRIBUTED_BY_HINT] = distributed_by

    if buckets is not None:
        additional_table_hints[TABLE_BUCKETS_HINT] = buckets

    if table_properties is not None:
        additional_table_hints[TABLE_PROPERTIES_HINT] = table_properties

    if additional_table_hints:
        data.apply_hints(additional_table_hints=additional_table_hints)

    return data
