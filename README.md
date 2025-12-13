# amplifier-module-storage-localfirst

Generic local-first storage with optional sync for Amplifier applications.

## Installation

```bash
pip install git+https://github.com/salil/amplifier-module-storage-localfirst
```

## Usage

### As an Amplifier Module

```python
# In your Amplifier config
modules:
  - name: storage-localfirst
    config:
      db_path: "~/.myapp/data.db"
      enable_vectors: false
      schemas:
        - name: todos
          fields:
            id: string
            text: string
            status: string
            created_at: datetime
          indexes:
            - status
```

### Direct Usage

```python
from amplifier_module_storage_localfirst import (
    SQLiteLocalFirstStorage,
    StorageConfig,
    Schema,
    FieldType,
)

# Configure storage
config = StorageConfig(
    db_path="~/.myapp/data.db",
    enable_vectors=False
)

# Define schema
todo_schema = Schema(
    name="todos",
    fields={
        "id": FieldType.STRING,
        "text": FieldType.STRING,
        "status": FieldType.STRING,
        "created_at": FieldType.DATETIME,
    },
    indexes=["status"]
)

# Initialize storage
storage = SQLiteLocalFirstStorage()
await storage.initialize(config)
await storage.register_collection(todo_schema)

# CRUD operations
todo_id = await storage.save("todos", {
    "text": "Buy groceries",
    "status": "pending"
})

todo = await storage.get("todos", todo_id)

await storage.update("todos", todo_id, {"status": "done"})

await storage.delete("todos", todo_id)
```

### Query Operations

```python
# Simple equality
results = await storage.query("todos", filter={"status": "pending"})

# Comparison operators
results = await storage.query("todos", filter={
    "created_at__gte": datetime(2024, 1, 1)
})

# List membership
results = await storage.query("todos", filter={
    "status__in": ["pending", "in_progress"]
})

# String contains
results = await storage.query("todos", filter={
    "text__contains": "groceries"
})

# Null check
results = await storage.query("todos", filter={
    "due_date__is_null": True
})

# Sorting and pagination
results = await storage.query(
    "todos",
    sort=[("created_at", "desc")],
    limit=10,
    offset=0
)

# Count
count = await storage.count("todos", filter={"status": "pending"})
```

## Schema Field Types

- `STRING` - Text values
- `INTEGER` - Whole numbers
- `FLOAT` - Decimal numbers
- `BOOLEAN` - True/False
- `DATETIME` - Date and time
- `DATE` - Date only
- `JSON` - Nested objects/arrays

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db_path` | str | required | Path to SQLite database file |
| `backend_url` | str | None | URL for backend sync (None = local only) |
| `auth_token` | str | None | Auth token for backend API |
| `conflict_strategy` | str | "last_write_wins" | How to handle sync conflicts |
| `auto_sync` | bool | True | Auto-sync on changes |
| `sync_interval` | int | 60 | Seconds between background syncs |
| `enable_vectors` | bool | False | Enable semantic search |
| `embedding_model` | str | "all-MiniLM-L6-v2" | Model for embeddings |

## License

MIT
