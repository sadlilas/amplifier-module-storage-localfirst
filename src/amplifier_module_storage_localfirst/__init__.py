"""amplifier-module-storage-localfirst - Generic local-first storage with optional sync.

This is a true Amplifier module that provides local-first storage with
schema-driven collections. Any application can use it for any entity type.

Example usage:
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

    # Use it
    todo_id = await storage.save("todos", {"text": "Buy groceries", "status": "pending"})
"""

from amplifier_module_storage_localfirst.errors import (
    ConflictError,
    NotFoundError,
    NotSupportedError,
    SchemaError,
    StorageError,
    SyncError,
)
from amplifier_module_storage_localfirst.protocol import LocalFirstStorage
from amplifier_module_storage_localfirst.sqlite import SQLiteLocalFirstStorage
from amplifier_module_storage_localfirst.types import (
    Change,
    Conflict,
    FieldType,
    Schema,
    StorageConfig,
    SyncResult,
)

__all__ = [
    # Main classes
    "LocalFirstStorage",
    "SQLiteLocalFirstStorage",
    # Configuration
    "StorageConfig",
    "Schema",
    "FieldType",
    # Data types
    "SyncResult",
    "Conflict",
    "Change",
    # Errors
    "StorageError",
    "NotFoundError",
    "SchemaError",
    "SyncError",
    "ConflictError",
    "NotSupportedError",
    # Mount
    "mount",
]


# Amplifier module type identifier
__amplifier_module_type__ = "storage"


async def mount(coordinator, config: dict):
    """Amplifier module entry point.

    Mounts LocalFirstStorage at the 'storage' slot.

    Args:
        coordinator: Amplifier coordinator instance.
        config: Configuration dict with keys:
            - db_path: Path to SQLite database
            - backend_url: Optional sync endpoint
            - enable_vectors: Enable semantic search (default: False)
            - auto_sync: Auto-sync on changes (default: True)
            - sync_interval: Seconds between background syncs (default: 60)
            - schemas: List of schema definitions
    """
    storage_config = StorageConfig(
        db_path=config.get("db_path", "data.db"),
        backend_url=config.get("backend_url"),
        auth_token=config.get("auth_token"),
        enable_vectors=config.get("enable_vectors", False),
        auto_sync=config.get("auto_sync", True),
        sync_interval=config.get("sync_interval", 60),
        conflict_strategy=config.get("conflict_strategy", "last_write_wins"),
    )

    storage = SQLiteLocalFirstStorage()
    await storage.initialize(storage_config)

    # Register schemas from config
    for schema_def in config.get("schemas", []):
        # Convert field type strings to FieldType enum
        fields = {}
        for field_name, field_type in schema_def.get("fields", {}).items():
            if isinstance(field_type, str):
                fields[field_name] = FieldType(field_type)
            else:
                fields[field_name] = field_type

        schema = Schema(
            name=schema_def["name"],
            fields=fields,
            primary_key=schema_def.get("primary_key", "id"),
            indexes=schema_def.get("indexes"),
            vector_field=schema_def.get("vector_field"),
        )
        await storage.register_collection(schema)

    # Mount at named slot
    await coordinator.mount("storage", storage)
