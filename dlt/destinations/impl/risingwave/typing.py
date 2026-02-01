from typing import Dict, Literal, Union

# Table hint names for Risingwave-specific properties
TABLE_ENGINE_HINT: Literal["x-table-engine"] = "x-table-engine"
TABLE_PROPERTIES_HINT: Literal["x-table-properties"] = "x-table-properties"
TABLE_APPEND_ONLY_HINT: Literal["x-table-append-only"] = "x-table-append-only"

# Supported engine types (Iceberg only per user requirement)
TTableEngine = Literal["iceberg"]

# Table properties type (key-value pairs for WITH clause)
TTableProperties = Dict[str, Union[str, int, float, bool]]
