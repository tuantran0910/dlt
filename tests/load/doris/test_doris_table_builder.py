from typing import Any, cast
import pytest

from dlt.destinations.impl.doris.doris_adapter import doris_adapter
from dlt.destinations.impl.doris.typing import (
    TABLE_MODEL_HINT,
    TABLE_DISTRIBUTED_BY_HINT,
    TABLE_BUCKETS_HINT,
    TABLE_PROPERTIES_HINT,
    TTableProperties,
)


class MockResource:
    """Mock dlt resource for testing doris_adapter."""

    def __init__(self) -> None:
        self._additional_table_hints: Any = {}

    def apply_hints(self, additional_table_hints: Any = None, **kwargs: Any) -> None:
        if additional_table_hints:
            self._additional_table_hints.update(additional_table_hints)


def test_doris_adapter_table_model_duplicate() -> None:
    """Test setting table model to duplicate."""
    resource = MockResource()
    doris_adapter(cast(Any, resource), table_model="duplicate")

    assert resource._additional_table_hints[TABLE_MODEL_HINT] == "duplicate"


def test_doris_adapter_table_model_unique() -> None:
    """Test setting table model to unique."""
    resource = MockResource()
    doris_adapter(cast(Any, resource), table_model="unique")

    assert resource._additional_table_hints[TABLE_MODEL_HINT] == "unique"


def test_doris_adapter_table_model_aggregate() -> None:
    """Test setting table model to aggregate."""
    resource = MockResource()
    doris_adapter(cast(Any, resource), table_model="aggregate")

    assert resource._additional_table_hints[TABLE_MODEL_HINT] == "aggregate"


def test_doris_adapter_invalid_table_model() -> None:
    """Test that invalid table model raises ValueError."""
    resource = MockResource()
    with pytest.raises(ValueError, match="Invalid table_model"):
        doris_adapter(cast(Any, resource), table_model=cast(Any, "invalid"))


def test_doris_adapter_distributed_by() -> None:
    """Test setting distributed by columns."""
    resource = MockResource()
    doris_adapter(cast(Any, resource), distributed_by=["id", "name"])

    assert resource._additional_table_hints[TABLE_DISTRIBUTED_BY_HINT] == ["id", "name"]


def test_doris_adapter_buckets() -> None:
    """Test setting number of buckets."""
    resource = MockResource()
    doris_adapter(cast(Any, resource), buckets=16)

    assert resource._additional_table_hints[TABLE_BUCKETS_HINT] == 16


def test_doris_adapter_table_properties() -> None:
    """Test setting table properties."""
    resource = MockResource()
    props = {"replication_num": "1", "storage_format": "V2"}
    doris_adapter(cast(Any, resource), table_properties=cast(TTableProperties, props))

    assert resource._additional_table_hints[TABLE_PROPERTIES_HINT] == props


def test_doris_adapter_combined_hints() -> None:
    """Test setting multiple hints at once."""
    resource = MockResource()
    doris_adapter(
        cast(Any, resource),
        table_model="unique",
        distributed_by=["id"],
        buckets=8,
        table_properties=cast(TTableProperties, {"replication_num": "1"}),
    )

    assert resource._additional_table_hints[TABLE_MODEL_HINT] == "unique"
    assert resource._additional_table_hints[TABLE_DISTRIBUTED_BY_HINT] == ["id"]
    assert resource._additional_table_hints[TABLE_BUCKETS_HINT] == 8
    assert resource._additional_table_hints[TABLE_PROPERTIES_HINT] == {"replication_num": "1"}


def test_doris_adapter_no_hints() -> None:
    """Test that no hints are applied when all args are None."""
    resource = MockResource()
    doris_adapter(cast(Any, resource))

    assert len(resource._additional_table_hints) == 0


def test_doris_adapter_returns_resource() -> None:
    """Test that doris_adapter returns the resource."""
    resource = MockResource()
    result = doris_adapter(cast(Any, resource), table_model="duplicate")

    assert result is resource
