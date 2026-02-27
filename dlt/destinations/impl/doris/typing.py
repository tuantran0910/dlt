from typing import Dict, List, Literal, Union

# Table hint names for Doris-specific properties
TABLE_MODEL_HINT: Literal["x-table-model"] = "x-table-model"
TABLE_DISTRIBUTED_BY_HINT: Literal["x-distributed-by"] = "x-distributed-by"
TABLE_BUCKETS_HINT: Literal["x-buckets"] = "x-buckets"
TABLE_PROPERTIES_HINT: Literal["x-table-properties"] = "x-table-properties"

# Supported table models
TTableModel = Literal["duplicate", "unique", "aggregate"]

# Distributed by column names
TDistributedBy = List[str]

# Table properties type (key-value pairs for PROPERTIES clause)
TTableProperties = Dict[str, Union[str, int, float, bool]]
