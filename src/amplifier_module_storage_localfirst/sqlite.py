"""SQLite implementation of LocalFirstStorage."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiosqlite

from amplifier_module_storage_localfirst.errors import (
    NotFoundError,
    NotSupportedError,
    SchemaError,
)
from amplifier_module_storage_localfirst.types import (
    Change,
    FieldType,
    Schema,
    StorageConfig,
    SyncResult,
)

if TYPE_CHECKING:
    pass


class SQLiteLocalFirstStorage:
    """SQLite-based implementation of LocalFirstStorage.

    This implementation provides:
    - Schema-driven table creation
    - Full CRUD operations
    - Rich query filtering
    - Change tracking for sync
    - Optional FTS (full-text search)

    Vector search requires sqlite-vec extension (not included by default).
    """

    def __init__(self):
        """Initialize storage (call `initialize()` before use)."""
        self._conn: aiosqlite.Connection | None = None
        self._config: StorageConfig | None = None
        self._schemas: dict[str, Schema] = {}

    async def initialize(self, config: StorageConfig) -> None:
        """Initialize storage with configuration."""
        self._config = config
        db_path = Path(config.db_path).expanduser()

        # Ensure directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = await aiosqlite.connect(db_path)
        self._conn.row_factory = aiosqlite.Row

        # Create system tables
        await self._create_system_tables()

    async def _create_system_tables(self) -> None:
        """Create internal system tables for tracking changes."""
        await self.conn.executescript("""
            -- Track pending changes for sync
            CREATE TABLE IF NOT EXISTS _pending_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                data TEXT,
                timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_pending_changes_collection
                ON _pending_changes(collection);

            -- Track sync state
            CREATE TABLE IF NOT EXISTS _sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """)
        await self.conn.commit()

    async def register_collection(self, schema: Schema) -> None:
        """Register a collection with its schema."""
        self._validate_schema(schema)
        self._schemas[schema.name] = schema
        await self._create_collection_table(schema)

    def _validate_schema(self, schema: Schema) -> None:
        """Validate schema definition."""
        if not schema.name:
            raise SchemaError("Schema name is required")
        if not schema.fields:
            raise SchemaError("Schema must have at least one field")
        if schema.primary_key not in schema.fields:
            raise SchemaError(f"Primary key '{schema.primary_key}' not in fields")
        if schema.vector_field and schema.vector_field not in schema.fields:
            raise SchemaError(f"Vector field '{schema.vector_field}' not in fields")

    async def _create_collection_table(self, schema: Schema) -> None:
        """Create table for a collection."""
        columns = []

        for field_name, field_type in schema.fields.items():
            sql_type = self._field_type_to_sql(field_type)
            if field_name == schema.primary_key:
                columns.append(f"{field_name} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"{field_name} {sql_type}")

        # Metadata columns
        columns.extend([
            "_created_at TEXT",
            "_updated_at TEXT",
            "_deleted INTEGER DEFAULT 0",
            "_version INTEGER DEFAULT 1",
        ])

        await self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema.name} (
                {', '.join(columns)}
            )
        """)

        # Create indexes
        for index_field in schema.indexes or []:
            await self.conn.execute(f"""
                CREATE INDEX IF NOT EXISTS
                idx_{schema.name}_{index_field}
                ON {schema.name}({index_field})
            """)

        await self.conn.commit()

    def _field_type_to_sql(self, field_type: FieldType) -> str:
        """Convert FieldType to SQLite type."""
        mapping = {
            FieldType.STRING: "TEXT",
            FieldType.INTEGER: "INTEGER",
            FieldType.FLOAT: "REAL",
            FieldType.BOOLEAN: "INTEGER",
            FieldType.DATETIME: "TEXT",
            FieldType.DATE: "TEXT",
            FieldType.JSON: "TEXT",
        }
        return mapping.get(field_type, "TEXT")

    async def close(self) -> None:
        """Clean up resources."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        """Get connection, raising if not initialized."""
        if self._conn is None:
            raise RuntimeError("Storage not initialized. Call initialize() first.")
        return self._conn

    def _ensure_collection(self, collection: str) -> Schema:
        """Ensure collection is registered and return schema."""
        if collection not in self._schemas:
            raise SchemaError(f"Collection '{collection}' not registered")
        return self._schemas[collection]

    # === CRUD Operations ===

    async def save(self, collection: str, entity: dict) -> str:
        """Save an entity (create or update)."""
        schema = self._ensure_collection(collection)
        pk = schema.primary_key

        # Generate ID if not present
        if pk not in entity or entity[pk] is None:
            entity = {**entity, pk: str(uuid.uuid4())}

        entity_id = entity[pk]
        now = datetime.now(timezone.utc).isoformat()

        # Check if exists
        existing = await self.get(collection, entity_id)

        if existing:
            # Update
            await self._update_entity(collection, entity_id, entity, schema)
        else:
            # Insert
            await self._insert_entity(collection, entity, schema, now)

        return entity_id

    async def _insert_entity(
        self,
        collection: str,
        entity: dict,
        schema: Schema,
        now: str,
    ) -> None:
        """Insert a new entity."""
        columns = []
        placeholders = []
        values = []

        for field_name in schema.fields:
            if field_name in entity:
                columns.append(field_name)
                placeholders.append("?")
                values.append(self._serialize_value(entity[field_name], schema.fields[field_name]))

        # Add metadata
        columns.extend(["_created_at", "_updated_at", "_deleted", "_version"])
        placeholders.extend(["?", "?", "?", "?"])
        values.extend([now, now, 0, 1])

        await self.conn.execute(
            f"INSERT INTO {collection} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})",
            values,
        )
        await self.conn.commit()

        # Track change for sync
        await self._track_change(collection, entity[schema.primary_key], "create", entity)

    async def _update_entity(
        self,
        collection: str,
        entity_id: str,
        entity: dict,
        schema: Schema,
    ) -> None:
        """Update an existing entity."""
        now = datetime.now(timezone.utc).isoformat()
        updates = []
        values = []

        for field_name in schema.fields:
            if field_name in entity and field_name != schema.primary_key:
                updates.append(f"{field_name} = ?")
                values.append(self._serialize_value(entity[field_name], schema.fields[field_name]))

        updates.append("_updated_at = ?")
        values.append(now)
        updates.append("_version = _version + 1")

        values.append(entity_id)

        await self.conn.execute(
            f"UPDATE {collection} SET {', '.join(updates)} WHERE {schema.primary_key} = ?",
            values,
        )
        await self.conn.commit()

        # Track change for sync
        await self._track_change(collection, entity_id, "update", entity)

    def _serialize_value(self, value: Any, field_type: FieldType) -> Any:
        """Serialize a value for storage."""
        if value is None:
            return None
        if field_type == FieldType.JSON:
            return json.dumps(value)
        if field_type == FieldType.BOOLEAN:
            return 1 if value else 0
        if field_type == FieldType.DATETIME:
            if isinstance(value, datetime):
                return value.isoformat()
            return value
        if field_type == FieldType.DATE:
            if hasattr(value, "isoformat"):
                return value.isoformat()
            return value
        return value

    def _deserialize_value(self, value: Any, field_type: FieldType) -> Any:
        """Deserialize a value from storage."""
        if value is None:
            return None
        if field_type == FieldType.JSON:
            return json.loads(value) if isinstance(value, str) else value
        if field_type == FieldType.BOOLEAN:
            return bool(value)
        if field_type == FieldType.INTEGER:
            return int(value) if value is not None else None
        if field_type == FieldType.FLOAT:
            return float(value) if value is not None else None
        return value

    async def get(self, collection: str, entity_id: str) -> dict | None:
        """Get an entity by ID."""
        schema = self._ensure_collection(collection)

        cursor = await self.conn.execute(
            f"SELECT * FROM {collection} WHERE {schema.primary_key} = ? AND _deleted = 0",
            (entity_id,),
        )
        row = await cursor.fetchone()

        if row is None:
            return None

        return self._row_to_entity(row, schema)

    def _row_to_entity(self, row: aiosqlite.Row, schema: Schema) -> dict:
        """Convert database row to entity dict."""
        entity = {}

        for field_name, field_type in schema.fields.items():
            if field_name in row.keys():
                entity[field_name] = self._deserialize_value(row[field_name], field_type)

        # Include metadata
        entity["_created_at"] = row["_created_at"]
        entity["_updated_at"] = row["_updated_at"]
        entity["_version"] = row["_version"]

        return entity

    async def update(self, collection: str, entity_id: str, changes: dict) -> dict:
        """Partial update of an entity."""
        schema = self._ensure_collection(collection)

        existing = await self.get(collection, entity_id)
        if existing is None:
            raise NotFoundError(collection, entity_id)

        # Merge changes
        updated = {**existing, **changes}
        await self.save(collection, updated)

        return await self.get(collection, entity_id)  # type: ignore

    async def delete(self, collection: str, entity_id: str) -> bool:
        """Delete an entity (soft delete)."""
        schema = self._ensure_collection(collection)

        existing = await self.get(collection, entity_id)
        if existing is None:
            return False

        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute(
            f"UPDATE {collection} SET _deleted = 1, _updated_at = ? WHERE {schema.primary_key} = ?",
            (now, entity_id),
        )
        await self.conn.commit()

        # Track change for sync
        await self._track_change(collection, entity_id, "delete", {})

        return True

    # === Query Operations ===

    async def query(
        self,
        collection: str,
        filter: dict | None = None,
        sort: list[tuple[str, str]] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Query entities with filtering, sorting, pagination."""
        schema = self._ensure_collection(collection)

        conditions = ["_deleted = 0"]
        values: list[Any] = []

        if filter:
            for key, value in filter.items():
                condition, vals = self._build_filter_condition(key, value)
                conditions.append(condition)
                values.extend(vals)

        where_clause = " AND ".join(conditions)

        # Build ORDER BY
        order_clause = ""
        if sort:
            order_parts = []
            for field, direction in sort:
                dir_sql = "DESC" if direction.lower() == "desc" else "ASC"
                order_parts.append(f"{field} {dir_sql}")
            order_clause = f"ORDER BY {', '.join(order_parts)}"

        query = f"""
            SELECT * FROM {collection}
            WHERE {where_clause}
            {order_clause}
            LIMIT ? OFFSET ?
        """
        values.extend([limit, offset])

        cursor = await self.conn.execute(query, values)
        rows = await cursor.fetchall()

        return [self._row_to_entity(row, schema) for row in rows]

    def _build_filter_condition(self, key: str, value: Any) -> tuple[str, list[Any]]:
        """Build SQL condition from filter key/value."""
        # Parse operator from key
        if "__" in key:
            field, op = key.rsplit("__", 1)
        else:
            field = key
            op = "eq"

        if op == "eq":
            return f"{field} = ?", [value]
        elif op == "ne":
            return f"{field} != ?", [value]
        elif op == "gt":
            return f"{field} > ?", [value]
        elif op == "gte":
            return f"{field} >= ?", [value]
        elif op == "lt":
            return f"{field} < ?", [value]
        elif op == "lte":
            return f"{field} <= ?", [value]
        elif op == "in":
            placeholders = ", ".join("?" * len(value))
            return f"{field} IN ({placeholders})", list(value)
        elif op == "not_in":
            placeholders = ", ".join("?" * len(value))
            return f"{field} NOT IN ({placeholders})", list(value)
        elif op == "contains":
            return f"{field} LIKE ?", [f"%{value}%"]
        elif op == "starts_with":
            return f"{field} LIKE ?", [f"{value}%"]
        elif op == "ends_with":
            return f"{field} LIKE ?", [f"%{value}"]
        elif op == "is_null":
            if value:
                return f"{field} IS NULL", []
            else:
                return f"{field} IS NOT NULL", []
        else:
            # Default to equality
            return f"{key} = ?", [value]

    async def count(self, collection: str, filter: dict | None = None) -> int:
        """Count entities matching filter."""
        self._ensure_collection(collection)

        conditions = ["_deleted = 0"]
        values: list[Any] = []

        if filter:
            for key, value in filter.items():
                condition, vals = self._build_filter_condition(key, value)
                conditions.append(condition)
                values.extend(vals)

        where_clause = " AND ".join(conditions)

        cursor = await self.conn.execute(
            f"SELECT COUNT(*) FROM {collection} WHERE {where_clause}",
            values,
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    # === Semantic Search ===

    @property
    def has_vector_search(self) -> bool:
        """Whether vector search is available."""
        return self._config is not None and self._config.enable_vectors

    async def semantic_search(
        self,
        collection: str,
        query: str,
        limit: int = 10,
        filter: dict | None = None,
    ) -> list[dict]:
        """Find semantically similar entities."""
        if not self.has_vector_search:
            raise NotSupportedError("Vector search not enabled. Set enable_vectors=True in config.")

        schema = self._ensure_collection(collection)
        if not schema.vector_field:
            raise SchemaError(f"Collection '{collection}' has no vector_field defined")

        # Note: Full vector search requires sqlite-vec extension
        # For now, fall back to text search
        raise NotSupportedError(
            "Vector search requires sqlite-vec extension. "
            "Use query() with contains filter as an alternative."
        )

    # === Sync Operations ===

    @property
    def supports_sync(self) -> bool:
        """Whether sync is available."""
        return self._config is not None and self._config.backend_url is not None

    async def _track_change(
        self,
        collection: str,
        entity_id: str,
        operation: str,
        data: dict,
    ) -> None:
        """Track a change for sync."""
        if not self.supports_sync:
            return

        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute(
            """
            INSERT INTO _pending_changes (collection, entity_id, operation, data, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (collection, entity_id, operation, json.dumps(data), now),
        )
        await self.conn.commit()

    async def sync(self) -> SyncResult:
        """Sync with backend."""
        if not self.supports_sync:
            raise NotSupportedError("Sync not available. Configure backend_url.")

        # Get pending changes
        changes = await self.get_pending_changes()

        # TODO: Implement actual sync protocol
        # For now, return empty result
        return SyncResult(
            pushed=0,
            pulled=0,
            conflicts=[],
            errors=["Sync not yet implemented"],
        )

    async def get_pending_changes(self) -> list[Change]:
        """Get changes not yet synced."""
        if not self.supports_sync:
            raise NotSupportedError("Sync not available. Configure backend_url.")

        cursor = await self.conn.execute(
            "SELECT * FROM _pending_changes ORDER BY timestamp"
        )
        rows = await cursor.fetchall()

        changes = []
        for row in rows:
            changes.append(
                Change(
                    collection=row["collection"],
                    entity_id=row["entity_id"],
                    operation=row["operation"],
                    data=json.loads(row["data"]) if row["data"] else {},
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                )
            )

        return changes

    async def force_push(self, collection: str, entity_id: str) -> None:
        """Force push a specific entity."""
        if not self.supports_sync:
            raise NotSupportedError("Sync not available. Configure backend_url.")

        entity = await self.get(collection, entity_id)
        if entity is None:
            raise NotFoundError(collection, entity_id)

        # TODO: Implement actual sync protocol
        raise NotSupportedError("Force push not yet implemented")

    async def force_pull(self, collection: str, entity_id: str) -> None:
        """Force pull a specific entity."""
        if not self.supports_sync:
            raise NotSupportedError("Sync not available. Configure backend_url.")

        # TODO: Implement actual sync protocol
        raise NotSupportedError("Force pull not yet implemented")
