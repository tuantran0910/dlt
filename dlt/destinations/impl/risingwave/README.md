# Risingwave Destination

[Risingwave](https://risingwave.com/) is a Postgres-protocol compatible OLAP database that supports streaming analytics. This destination allows you to load data into Risingwave with support for special table properties like ENGINE, APPEND ONLY, custom table properties, and PRIMARY KEY constraints.

## Features

- **Postgres-compatible**: Leverages dlt's Postgres destination implementation with Risingwave-specific extensions
- **Table engine support**: Supports the Iceberg table engine (`ENGINE = iceberg`)
- **Append-only tables**: Configure tables as append-only for optimized write patterns
- **Custom table properties**: Pass custom properties via the WITH clause (e.g., timeline settings)
- **PRIMARY KEY support**: Optionally create PRIMARY KEY constraints (disabled by default)
- **Object storage staging**: Support for loading data from S3, GCS, and Azure Blob Storage via `file_scan()`

## Configuration

### Connection String

```python
import dlt
from dlt.destinations import risingwave

pipeline = dlt.pipeline(
    destination=risingwave(
        credentials="risingwave://user:password@localhost:4566/dev"
    )
)
```

**Note:** While the connection string uses `risingwave://` for destination identification, internally dlt converts this to `postgresql://` for psycopg2 compatibility (Risingwave is Postgres-protocol compatible).

### Full Configuration

```python
pipeline = dlt.pipeline(
    destination=risingwave(
        credentials={
            "host": "localhost",
            "port": 4566,
            "database": "dev",
            "username": "root",
            "password": "password"
        },
        table_engine="iceberg",   # Set default table engine
        create_indexes=True        # Enable PRIMARY KEY support (default: False)
    )
)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `credentials` | `RisingwaveCredentials` or `str` or `dict` | *required* | Connection credentials |
| `table_engine` | `str` | `None` | Default table engine (supported: `"iceberg"`) |
| `create_indexes` | `bool` | `False` | Whether to create PRIMARY KEY constraints |

## Table Properties

### Using the Adapter

The `risingwave_adapter` function allows you to configure table-specific properties:

```python
from dlt.destinations.impl.risingwave.risingwave_adapter import risingwave_adapter
import dlt

@dlt.resource
def user_events():
    yield {"user_id": 1, "event_type": "login", "timestamp": "2024-01-01"}

# Apply adapter with custom properties
resource = risingwave_adapter(
    user_events(),
    table_engine="iceberg",
    append_only=True,
    table_properties={"timeline.timestamp_column": "timestamp"}
)

pipeline.run(resource)
```

### Using apply_hints() Directly

```python
resource = user_events().apply_hints(
    additional_table_hints={
        "x-table-engine": "iceberg",
        "x-table-append-only": True,
        "x-table-properties": {"timeline.timestamp_column": "timestamp"}
    }
)
pipeline.run(resource)
```

## PRIMARY KEY Support

To create PRIMARY KEY constraints in Risingwave tables:

```python
pipeline = dlt.pipeline(
    destination=risingwave(
        credentials={...},
        create_indexes=True  # Must be enabled for PRIMARY KEY
    )
)

@dlt.resource(primary_key=["user_id"])
def user_events():
    yield {"user_id": 1, "event_type": "login", "timestamp": "2024-01-01"}

pipeline.run(user_events())
```

**Important:** PRIMARY KEY constraints are only created when:
1. `create_indexes=True` is set in the destination configuration
2. The resource has `primary_key` hint set

If `create_indexes=False` (default), the `primary_key` hint is ignored.

## Generated SQL Example

### With All Options

```sql
CREATE TABLE user_events (
    user_id BIGINT,
    event_type VARCHAR,
    timestamp TIMESTAMPTZ,
    PRIMARY KEY (user_id)
)
APPEND ONLY
WITH (timeline.timestamp_column = 'timestamp')
ENGINE = iceberg;
```

**Clause Order:** The Risingwave-specific clauses are added in the following order:
1. `APPEND ONLY` (if enabled)
2. `WITH (...)` (if properties are set)
3. `ENGINE = iceberg` (if engine is set)

### Minimal Table

```sql
CREATE TABLE user_events (
    user_id BIGINT,
    event_type VARCHAR,
    timestamp TIMESTAMPTZ
);
```

## Table Hints

| Hint | Type | Description |
|------|------|-------------|
| `x-table-engine` | `str` | Table engine type (supported: `"iceberg"`) |
| `x-table-append-only` | `bool` | Enable append-only mode |
| `x-table-properties` | `dict` | Table properties for the WITH clause |

## Data Type Mapping

Risingwave has some differences from standard PostgreSQL:

| dlt Type | Risingwave Type | Notes |
|----------|-----------------|-------|
| `text` | `varchar` | No length specification |
| `decimal` | `numeric` | No precision/scale specification |
| `wei` | `numeric` | No precision/scale specification |
| `timestamp` | `timestamp with time zone` | No precision specification |

## Object Storage Staging

For large datasets, you can configure object storage staging (S3, GCS, or Azure Blob):

```python
pipeline = dlt.pipeline(
    destination=risingwave(
        credentials="risingwave://user:password@localhost:4566/dev",
        staging_config="filesystem"  # Configure filesystem staging
    )
)
```

With staging configured, dlt will use Risingwave's `file_scan()` table function to load data directly from object storage, which is more efficient for large datasets.

**Supported storage providers:**
- **S3** (`aws://` URLs) - requires `region_name` in credentials
- **Google Cloud Storage** (`gs://` URLs) - supports service account keys or OAuth tokens
- **Azure Blob Storage** (`az://`, `abfs://` URLs) - requires `account_name`, `account_key`, and `endpoint_url`

**Supported file format for staging:** `parquet` only

The `file_scan()` function automatically detects compression for parquet files.

## Error Handling

The Risingwave destination handles Risingwave-specific errors:

- **`UndefinedObject`**: Properly converted to `DatabaseUndefinedRelation` for handling missing tables
- **First-run scenario**: State table creation is handled gracefully on initial pipeline runs

## References

- [Risingwave Documentation](https://docs.risingwave.com/)
- [Risingwave CREATE TABLE Syntax](https://docs.risingwave.com/sql/statements/create-table/)
- [Risingwave file_scan() Function](https://docs.risingwave.com/sql/functions/file-scan/)
- [S3 Integration](https://docs.risingwave.com/integrations/sources/s3)
- [Google Cloud Storage Integration](https://docs.risingwave.com/integrations/sources/google-cloud-storage)
- [Azure Blob Storage Integration](https://docs.risingwave.com/integrations/sources/azure-blob)
- [Risingwave Python SDK](https://docs.risingwave.com/sql/python-sdk/intro)
