"""Protocol definition for LocalFirstStorage."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from amplifier_module_storage_localfirst.types import (
        Change,
        Schema,
        StorageConfig,
        SyncResult,
    )


@runtime_checkable
class LocalFirstStorage(Protocol):
    """Generic local-first storage with optional sync.

    This is a TRUE AMPLIFIER MODULE — it works with any entity type.
    The schema defines what entities look like; the storage handles
    all the mechanics of persistence, querying, and sync.

    Implementations must provide all methods marked with `...`.
    """

    # === Lifecycle ===

    async def initialize(self, config: StorageConfig) -> None:
        """Initialize storage with configuration.

        Creates the database file if needed and sets up internal state.
        Must be called before any other operations.

        Args:
            config: Storage configuration.
        """
        ...

    async def register_collection(self, schema: Schema) -> None:
        """Register a collection with its schema.

        Creates the necessary tables/indexes for the collection.
        Can be called multiple times with the same schema (idempotent).

        Args:
            schema: Schema definition for the collection.

        Raises:
            SchemaError: If schema is invalid.
        """
        ...

    async def close(self) -> None:
        """Clean up resources.

        Closes database connections and stops background tasks.
        """
        ...

    # === CRUD Operations ===

    async def save(
        self,
        collection: str,
        entity: dict,
    ) -> str:
        """Save an entity.

        Creates if no ID present, updates if ID exists.

        Args:
            collection: Name of the collection.
            entity: Entity data. If 'id' field present, updates existing.

        Returns:
            The entity ID (generated if not provided).

        Raises:
            SchemaError: If collection not registered.
        """
        ...

    async def get(
        self,
        collection: str,
        entity_id: str,
    ) -> dict | None:
        """Get an entity by ID.

        Args:
            collection: Name of the collection.
            entity_id: ID of the entity.

        Returns:
            Entity dict, or None if not found.

        Raises:
            SchemaError: If collection not registered.
        """
        ...

    async def update(
        self,
        collection: str,
        entity_id: str,
        changes: dict,
    ) -> dict:
        """Partial update of an entity.

        Only updates fields present in changes dict.

        Args:
            collection: Name of the collection.
            entity_id: ID of the entity to update.
            changes: Dict of field changes to apply.

        Returns:
            Updated entity dict.

        Raises:
            NotFoundError: If entity doesn't exist.
            SchemaError: If collection not registered.
        """
        ...

    async def delete(
        self,
        collection: str,
        entity_id: str,
    ) -> bool:
        """Delete an entity.

        Args:
            collection: Name of the collection.
            entity_id: ID of the entity to delete.

        Returns:
            True if deleted, False if not found.

        Raises:
            SchemaError: If collection not registered.
        """
        ...

    # === Query Operations ===

    async def query(
        self,
        collection: str,
        filter: dict | None = None,
        sort: list[tuple[str, str]] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Query entities with filtering, sorting, pagination.

        Filter syntax:
            - Simple equality: {"status": "active"}
            - Comparison: {"created_at__gte": datetime(...)}
            - List membership: {"status__in": ["pending", "active"]}
            - String ops: {"title__contains": "groceries"}
            - Null check: {"due_date__is_null": True}

        Args:
            collection: Name of the collection.
            filter: Filter conditions (AND combined).
            sort: List of (field, direction) tuples. Direction: "asc" or "desc".
            limit: Maximum entities to return.
            offset: Number of entities to skip.

        Returns:
            List of matching entities.

        Raises:
            SchemaError: If collection not registered.
        """
        ...

    async def count(
        self,
        collection: str,
        filter: dict | None = None,
    ) -> int:
        """Count entities matching filter.

        Args:
            collection: Name of the collection.
            filter: Filter conditions (same syntax as query).

        Returns:
            Count of matching entities.

        Raises:
            SchemaError: If collection not registered.
        """
        ...

    # === Semantic Search (if enabled) ===

    @property
    def has_vector_search(self) -> bool:
        """Whether vector search is available."""
        ...

    async def semantic_search(
        self,
        collection: str,
        query: str,
        limit: int = 10,
        filter: dict | None = None,
    ) -> list[dict]:
        """Find semantically similar entities.

        Requires enable_vectors=True in config and vector_field in schema.

        Args:
            collection: Name of the collection.
            query: Text to find similar entities for.
            limit: Maximum entities to return.
            filter: Additional filter conditions.

        Returns:
            List of similar entities with _distance field added.

        Raises:
            NotSupportedError: If vectors not enabled.
            SchemaError: If collection not registered or no vector_field.
        """
        ...

    # === Sync Operations (if backend configured) ===

    @property
    def supports_sync(self) -> bool:
        """Whether sync is available."""
        ...

    async def sync(self) -> SyncResult:
        """Sync with backend.

        Pushes local changes, pulls remote changes.

        Returns:
            SyncResult with details.

        Raises:
            NotSupportedError: If backend_url not configured.
            SyncError: If sync fails.
        """
        ...

    async def get_pending_changes(self) -> list[Change]:
        """Get changes not yet synced.

        Returns:
            List of pending changes.

        Raises:
            NotSupportedError: If backend_url not configured.
        """
        ...

    async def force_push(self, collection: str, entity_id: str) -> None:
        """Force push a specific entity.

        Resolves conflict by using local version.

        Args:
            collection: Name of the collection.
            entity_id: ID of the entity.

        Raises:
            NotSupportedError: If backend_url not configured.
            NotFoundError: If entity not found.
        """
        ...

    async def force_pull(self, collection: str, entity_id: str) -> None:
        """Force pull a specific entity.

        Resolves conflict by using remote version.

        Args:
            collection: Name of the collection.
            entity_id: ID of the entity.

        Raises:
            NotSupportedError: If backend_url not configured.
            SyncError: If remote entity not found.
        """
        ...
