import pytest

from dlt.destinations.impl.risingwave.typing import (
    TABLE_APPEND_ONLY_HINT,
    TABLE_ENGINE_HINT,
    TABLE_PROPERTIES_HINT,
    TTableProperties,
)
from dlt.destinations.impl.risingwave.risingwave_adapter import risingwave_adapter


def test_risingwave_adapter_with_engine() -> None:
    """Test risingwave_adapter sets table engine hint."""
    data = [{"name": "Alice", "age": 30}]
    resource = risingwave_adapter(data, table_engine="iceberg")

    hints = resource._hints.get("additional_table_hints", {})
    assert TABLE_ENGINE_HINT in hints
    assert hints[TABLE_ENGINE_HINT] == "iceberg"


def test_risingwave_adapter_with_append_only() -> None:
    """Test risingwave_adapter sets append-only hint."""
    data = [{"name": "Alice", "age": 30}]
    resource = risingwave_adapter(data, append_only=True)

    hints = resource._hints.get("additional_table_hints", {})
    assert TABLE_APPEND_ONLY_HINT in hints
    assert hints[TABLE_APPEND_ONLY_HINT] is True


def test_risingwave_adapter_with_properties() -> None:
    """Test risingwave_adapter sets table properties hint."""
    data = [{"name": "Alice", "age": 30}]
    properties: TTableProperties = {"timeline.timestamp_column": "created_at"}
    resource = risingwave_adapter(data, table_properties=properties)

    hints = resource._hints.get("additional_table_hints", {})
    assert TABLE_PROPERTIES_HINT in hints
    assert hints[TABLE_PROPERTIES_HINT] == properties


def test_risingwave_adapter_with_all_options() -> None:
    """Test risingwave_adapter with all options."""
    data = [{"name": "Alice", "age": 30}]
    properties: TTableProperties = {"timeline.timestamp_column": "created_at"}
    resource = risingwave_adapter(
        data, table_engine="iceberg", append_only=True, table_properties=properties
    )

    hints = resource._hints.get("additional_table_hints", {})
    assert hints[TABLE_ENGINE_HINT] == "iceberg"
    assert hints[TABLE_APPEND_ONLY_HINT] is True
    assert hints[TABLE_PROPERTIES_HINT] == properties


def test_risingwave_adapter_invalid_engine() -> None:
    """Test risingwave_adapter rejects invalid engine."""
    data = [{"name": "Alice", "age": 30}]

    with pytest.raises(ValueError, match="table_engine must be 'iceberg'"):
        risingwave_adapter(data, table_engine="invalid_engine")  # type: ignore[arg-type]


def test_risingwave_hint_constants() -> None:
    """Test that hint constants are correctly defined."""
    assert TABLE_ENGINE_HINT == "x-table-engine"
    assert TABLE_PROPERTIES_HINT == "x-table-properties"
    assert TABLE_APPEND_ONLY_HINT == "x-table-append-only"
