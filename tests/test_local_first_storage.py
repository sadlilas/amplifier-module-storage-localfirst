"""Tests for LocalFirstStorage module."""

import pytest
import tempfile
import os
from datetime import datetime, timezone

from amplifier_module_storage_localfirst import (
    SQLiteLocalFirstStorage,
    StorageConfig,
    Schema,
    FieldType,
    NotFoundError,
    SchemaError,
    NotSupportedError,
)


@pytest.fixture
def storage_config():
    """Create a temporary storage config."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    yield StorageConfig(db_path=db_path)

    # Cleanup
    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture
def todo_schema():
    """Create a todo schema for testing."""
    return Schema(
        name="todos",
        fields={
            "id": FieldType.STRING,
            "text": FieldType.STRING,
            "status": FieldType.STRING,
            "priority": FieldType.INTEGER,
            "done": FieldType.BOOLEAN,
            "due_date": FieldType.DATE,
            "created_at": FieldType.DATETIME,
            "tags": FieldType.JSON,
        },
        primary_key="id",
        indexes=["status", "priority"],
    )


@pytest.fixture
async def storage(storage_config, todo_schema):
    """Create and initialize storage with todo schema."""
    store = SQLiteLocalFirstStorage()
    await store.initialize(storage_config)
    await store.register_collection(todo_schema)
    yield store
    await store.close()


class TestStorageInitialization:
    """Tests for storage initialization and configuration."""

    async def test_initialize_creates_database(self, storage_config):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)

        assert os.path.exists(storage_config.db_path)
        await store.close()

    async def test_register_collection(self, storage_config, todo_schema):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)
        await store.register_collection(todo_schema)

        # Should be able to query empty collection
        results = await store.query("todos")
        assert results == []

        await store.close()

    async def test_register_invalid_schema_no_name(self, storage_config):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)

        with pytest.raises(SchemaError, match="name is required"):
            await store.register_collection(
                Schema(name="", fields={"id": FieldType.STRING})
            )

        await store.close()

    async def test_register_invalid_schema_no_fields(self, storage_config):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)

        with pytest.raises(SchemaError, match="at least one field"):
            await store.register_collection(Schema(name="test", fields={}))

        await store.close()

    async def test_register_invalid_schema_missing_primary_key(self, storage_config):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)

        with pytest.raises(SchemaError, match="Primary key"):
            await store.register_collection(
                Schema(
                    name="test",
                    fields={"name": FieldType.STRING},
                    primary_key="id",  # Not in fields
                )
            )

        await store.close()

    async def test_query_unregistered_collection(self, storage_config):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)

        with pytest.raises(SchemaError, match="not registered"):
            await store.query("nonexistent")

        await store.close()


class TestCRUDOperations:
    """Tests for Create, Read, Update, Delete operations."""

    async def test_save_creates_entity(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Buy groceries",
            "status": "pending",
        })

        assert entity_id is not None
        assert len(entity_id) > 0

    async def test_save_with_explicit_id(self, storage):
        entity_id = await storage.save("todos", {
            "id": "my-custom-id",
            "text": "Buy groceries",
            "status": "pending",
        })

        assert entity_id == "my-custom-id"

    async def test_get_existing_entity(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Buy groceries",
            "status": "pending",
        })

        entity = await storage.get("todos", entity_id)

        assert entity is not None
        assert entity["id"] == entity_id
        assert entity["text"] == "Buy groceries"
        assert entity["status"] == "pending"

    async def test_get_nonexistent_entity(self, storage):
        entity = await storage.get("todos", "nonexistent-id")
        assert entity is None

    async def test_save_updates_existing(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Buy groceries",
            "status": "pending",
        })

        await storage.save("todos", {
            "id": entity_id,
            "text": "Buy groceries and milk",
            "status": "pending",
        })

        entity = await storage.get("todos", entity_id)
        assert entity["text"] == "Buy groceries and milk"

    async def test_update_partial(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Buy groceries",
            "status": "pending",
            "priority": 1,
        })

        updated = await storage.update("todos", entity_id, {"status": "active"})

        assert updated["status"] == "active"
        assert updated["text"] == "Buy groceries"  # Unchanged
        assert updated["priority"] == 1  # Unchanged

    async def test_update_nonexistent_raises(self, storage):
        with pytest.raises(NotFoundError):
            await storage.update("todos", "nonexistent", {"status": "active"})

    async def test_delete_entity(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Buy groceries",
            "status": "pending",
        })

        result = await storage.delete("todos", entity_id)

        assert result is True
        assert await storage.get("todos", entity_id) is None

    async def test_delete_nonexistent(self, storage):
        result = await storage.delete("todos", "nonexistent")
        assert result is False

    async def test_metadata_fields(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Buy groceries",
            "status": "pending",
        })

        entity = await storage.get("todos", entity_id)

        assert "_created_at" in entity
        assert "_updated_at" in entity
        assert "_version" in entity
        assert entity["_version"] == 1

    async def test_version_increments_on_update(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Buy groceries",
            "status": "pending",
        })

        await storage.update("todos", entity_id, {"status": "active"})
        entity = await storage.get("todos", entity_id)

        assert entity["_version"] == 2


class TestFieldTypes:
    """Tests for different field types."""

    async def test_boolean_field(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Test",
            "status": "pending",
            "done": True,
        })

        entity = await storage.get("todos", entity_id)
        assert entity["done"] is True

    async def test_integer_field(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Test",
            "status": "pending",
            "priority": 5,
        })

        entity = await storage.get("todos", entity_id)
        assert entity["priority"] == 5
        assert isinstance(entity["priority"], int)

    async def test_json_field(self, storage):
        entity_id = await storage.save("todos", {
            "text": "Test",
            "status": "pending",
            "tags": ["work", "urgent"],
        })

        entity = await storage.get("todos", entity_id)
        assert entity["tags"] == ["work", "urgent"]

    async def test_datetime_field(self, storage):
        now = datetime.now(timezone.utc)
        entity_id = await storage.save("todos", {
            "text": "Test",
            "status": "pending",
            "created_at": now,
        })

        entity = await storage.get("todos", entity_id)
        assert entity["created_at"] == now.isoformat()


class TestQueryOperations:
    """Tests for query functionality."""

    async def test_query_all(self, storage):
        await storage.save("todos", {"text": "Todo 1", "status": "pending"})
        await storage.save("todos", {"text": "Todo 2", "status": "pending"})

        results = await storage.query("todos")

        assert len(results) == 2

    async def test_query_with_equality_filter(self, storage):
        await storage.save("todos", {"text": "Todo 1", "status": "pending"})
        await storage.save("todos", {"text": "Todo 2", "status": "active"})

        results = await storage.query("todos", filter={"status": "pending"})

        assert len(results) == 1
        assert results[0]["status"] == "pending"

    async def test_query_with_comparison_filter(self, storage):
        await storage.save("todos", {"text": "Low", "status": "pending", "priority": 1})
        await storage.save("todos", {"text": "High", "status": "pending", "priority": 5})

        results = await storage.query("todos", filter={"priority__gte": 3})

        assert len(results) == 1
        assert results[0]["text"] == "High"

    async def test_query_with_in_filter(self, storage):
        await storage.save("todos", {"text": "Todo 1", "status": "pending"})
        await storage.save("todos", {"text": "Todo 2", "status": "active"})
        await storage.save("todos", {"text": "Todo 3", "status": "completed"})

        results = await storage.query("todos", filter={"status__in": ["pending", "active"]})

        assert len(results) == 2

    async def test_query_with_contains_filter(self, storage):
        await storage.save("todos", {"text": "Buy groceries", "status": "pending"})
        await storage.save("todos", {"text": "Call mom", "status": "pending"})

        results = await storage.query("todos", filter={"text__contains": "groceries"})

        assert len(results) == 1
        assert "groceries" in results[0]["text"]

    async def test_query_with_null_filter(self, storage):
        await storage.save("todos", {"text": "Has priority", "status": "pending", "priority": 5})
        await storage.save("todos", {"text": "No priority", "status": "pending"})

        results = await storage.query("todos", filter={"priority__is_null": True})

        assert len(results) == 1
        assert results[0]["text"] == "No priority"

    async def test_query_with_sort(self, storage):
        await storage.save("todos", {"text": "B", "status": "pending", "priority": 2})
        await storage.save("todos", {"text": "A", "status": "pending", "priority": 1})
        await storage.save("todos", {"text": "C", "status": "pending", "priority": 3})

        results = await storage.query("todos", sort=[("priority", "asc")])

        assert results[0]["priority"] == 1
        assert results[1]["priority"] == 2
        assert results[2]["priority"] == 3

    async def test_query_with_sort_desc(self, storage):
        await storage.save("todos", {"text": "B", "status": "pending", "priority": 2})
        await storage.save("todos", {"text": "A", "status": "pending", "priority": 1})

        results = await storage.query("todos", sort=[("priority", "desc")])

        assert results[0]["priority"] == 2
        assert results[1]["priority"] == 1

    async def test_query_with_limit(self, storage):
        for i in range(5):
            await storage.save("todos", {"text": f"Todo {i}", "status": "pending"})

        results = await storage.query("todos", limit=2)

        assert len(results) == 2

    async def test_query_with_offset(self, storage):
        for i in range(5):
            await storage.save("todos", {"text": f"Todo {i}", "status": "pending", "priority": i})

        results = await storage.query("todos", sort=[("priority", "asc")], offset=2, limit=2)

        assert len(results) == 2
        assert results[0]["priority"] == 2

    async def test_count_all(self, storage):
        await storage.save("todos", {"text": "Todo 1", "status": "pending"})
        await storage.save("todos", {"text": "Todo 2", "status": "active"})

        count = await storage.count("todos")

        assert count == 2

    async def test_count_with_filter(self, storage):
        await storage.save("todos", {"text": "Todo 1", "status": "pending"})
        await storage.save("todos", {"text": "Todo 2", "status": "active"})

        count = await storage.count("todos", filter={"status": "pending"})

        assert count == 1


class TestVectorSearch:
    """Tests for vector search (semantic search) functionality."""

    async def test_has_vector_search_disabled(self, storage_config, todo_schema):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)
        await store.register_collection(todo_schema)

        assert store.has_vector_search is False

        await store.close()

    async def test_semantic_search_raises_when_disabled(self, storage):
        with pytest.raises(NotSupportedError, match="not enabled"):
            await storage.semantic_search("todos", "test query")


class TestSyncOperations:
    """Tests for sync functionality."""

    async def test_supports_sync_without_backend(self, storage_config, todo_schema):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)
        await store.register_collection(todo_schema)

        assert store.supports_sync is False

        await store.close()

    async def test_supports_sync_with_backend(self, storage_config, todo_schema):
        config = StorageConfig(
            db_path=storage_config.db_path,
            backend_url="https://api.example.com/sync",
        )
        store = SQLiteLocalFirstStorage()
        await store.initialize(config)
        await store.register_collection(todo_schema)

        assert store.supports_sync is True

        await store.close()

    async def test_sync_raises_when_disabled(self, storage):
        with pytest.raises(NotSupportedError, match="not available"):
            await storage.sync()

    async def test_get_pending_changes_raises_when_disabled(self, storage):
        with pytest.raises(NotSupportedError, match="not available"):
            await storage.get_pending_changes()


class TestMultipleCollections:
    """Tests for handling multiple collections."""

    async def test_register_multiple_collections(self, storage_config):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)

        await store.register_collection(
            Schema(
                name="todos",
                fields={"id": FieldType.STRING, "text": FieldType.STRING},
            )
        )
        await store.register_collection(
            Schema(
                name="notes",
                fields={"id": FieldType.STRING, "content": FieldType.STRING},
            )
        )

        # Each collection is independent
        await store.save("todos", {"text": "Todo 1"})
        await store.save("notes", {"content": "Note 1"})

        assert await store.count("todos") == 1
        assert await store.count("notes") == 1

        await store.close()

    async def test_collections_are_isolated(self, storage_config):
        store = SQLiteLocalFirstStorage()
        await store.initialize(storage_config)

        await store.register_collection(
            Schema(
                name="todos",
                fields={"id": FieldType.STRING, "text": FieldType.STRING},
            )
        )
        await store.register_collection(
            Schema(
                name="notes",
                fields={"id": FieldType.STRING, "text": FieldType.STRING},
            )
        )

        todo_id = await store.save("todos", {"id": "shared-id", "text": "Todo"})
        note_id = await store.save("notes", {"id": "shared-id", "text": "Note"})

        # Same ID in different collections
        todo = await store.get("todos", "shared-id")
        note = await store.get("notes", "shared-id")

        assert todo["text"] == "Todo"
        assert note["text"] == "Note"

        await store.close()
