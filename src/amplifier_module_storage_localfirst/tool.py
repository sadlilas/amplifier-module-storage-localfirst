"""
LocalStorageTool - Amplifier Tool for local-first SQLite storage.

Provides agents with persistent storage capabilities via tool invocation.
"""

from typing import Any

from amplifier_core import ToolResult

from .sqlite import SQLiteLocalFirstStorage
from .types import FieldType, Schema, StorageConfig


class LocalStorageTool:
    """
    Amplifier Tool for local-first storage operations.

    Wraps SQLiteLocalFirstStorage to expose CRUD operations as tool calls.
    """

    def __init__(
        self,
        storage: SQLiteLocalFirstStorage,
        config: dict[str, Any] | None = None,
    ):
        """
        Initialize the LocalStorageTool.

        Args:
            storage: Initialized SQLiteLocalFirstStorage instance
            config: Optional tool configuration
        """
        self._storage = storage
        self._config = config or {}

    @property
    def name(self) -> str:
        return "local_storage"

    @property
    def description(self) -> str:
        return (
            "Persist and query data in local SQLite storage. "
            "Supports save, get, query, update, delete, and count operations "
            "on schema-defined collections."
        )

    def get_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": ["save", "get", "query", "update", "delete", "count"],
                    "description": "The storage operation to perform",
                },
                "collection": {
                    "type": "string",
                    "description": "Name of the collection to operate on",
                },
                "entity": {
                    "type": "object",
                    "description": "Entity data for save operation",
                },
                "entity_id": {
                    "type": "string",
                    "description": "Entity ID for get, update, or delete operations",
                },
                "changes": {
                    "type": "object",
                    "description": "Partial update data for update operation",
                },
                "filter": {
                    "type": "object",
                    "description": "Filter conditions for query/count (e.g., {'status': 'active'})",
                },
                "sort": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "description": "Sort specification as [[field, direction], ...] for query",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results for query (default: 100)",
                },
                "offset": {
                    "type": "integer",
                    "description": "Skip N results for query pagination (default: 0)",
                },
            },
            "required": ["operation", "collection"],
        }

    async def execute(self, input: dict[str, Any]) -> ToolResult:
        """Execute a storage operation."""
        operation = input.get("operation")
        collection = input.get("collection")

        if not operation:
            return ToolResult(
                success=False,
                output=None,
                error={"message": "Missing required field: operation"},
            )

        if not collection:
            return ToolResult(
                success=False,
                output=None,
                error={"message": "Missing required field: collection"},
            )

        try:
            if operation == "save":
                return await self._save(collection, input)
            elif operation == "get":
                return await self._get(collection, input)
            elif operation == "query":
                return await self._query(collection, input)
            elif operation == "update":
                return await self._update(collection, input)
            elif operation == "delete":
                return await self._delete(collection, input)
            elif operation == "count":
                return await self._count(collection, input)
            else:
                return ToolResult(
                    success=False,
                    output=None,
                    error={"message": f"Unknown operation: {operation}"},
                )
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error={"message": str(e), "type": type(e).__name__},
            )

    async def _save(self, collection: str, input: dict[str, Any]) -> ToolResult:
        """Save an entity to collection."""
        entity = input.get("entity")
        if not entity:
            return ToolResult(
                success=False,
                output=None,
                error={"message": "Missing required field: entity"},
            )

        entity_id = await self._storage.save(collection, entity)
        saved = await self._storage.get(collection, entity_id)

        return ToolResult(
            success=True,
            output={"id": entity_id, "entity": saved},
        )

    async def _get(self, collection: str, input: dict[str, Any]) -> ToolResult:
        """Get an entity by ID."""
        entity_id = input.get("entity_id")
        if not entity_id:
            return ToolResult(
                success=False,
                output=None,
                error={"message": "Missing required field: entity_id"},
            )

        entity = await self._storage.get(collection, entity_id)

        if entity is None:
            return ToolResult(
                success=True,
                output={"found": False, "entity": None},
            )

        return ToolResult(
            success=True,
            output={"found": True, "entity": entity},
        )

    async def _query(self, collection: str, input: dict[str, Any]) -> ToolResult:
        """Query entities with filtering and pagination."""
        filter_dict = input.get("filter")
        sort = input.get("sort")
        limit = input.get("limit", 100)
        offset = input.get("offset", 0)

        # Convert sort from list of lists to list of tuples if provided
        sort_tuples = None
        if sort:
            sort_tuples = [(s[0], s[1]) for s in sort if len(s) >= 2]

        entities = await self._storage.query(
            collection,
            filter=filter_dict,
            sort=sort_tuples,
            limit=limit,
            offset=offset,
        )

        return ToolResult(
            success=True,
            output={"count": len(entities), "entities": entities},
        )

    async def _update(self, collection: str, input: dict[str, Any]) -> ToolResult:
        """Partial update of an entity."""
        entity_id = input.get("entity_id")
        changes = input.get("changes")

        if not entity_id:
            return ToolResult(
                success=False,
                output=None,
                error={"message": "Missing required field: entity_id"},
            )

        if not changes:
            return ToolResult(
                success=False,
                output=None,
                error={"message": "Missing required field: changes"},
            )

        updated = await self._storage.update(collection, entity_id, changes)

        return ToolResult(
            success=True,
            output={"entity": updated},
        )

    async def _delete(self, collection: str, input: dict[str, Any]) -> ToolResult:
        """Delete an entity."""
        entity_id = input.get("entity_id")

        if not entity_id:
            return ToolResult(
                success=False,
                output=None,
                error={"message": "Missing required field: entity_id"},
            )

        deleted = await self._storage.delete(collection, entity_id)

        return ToolResult(
            success=True,
            output={"deleted": deleted},
        )

    async def _count(self, collection: str, input: dict[str, Any]) -> ToolResult:
        """Count entities matching filter."""
        filter_dict = input.get("filter")

        count = await self._storage.count(collection, filter=filter_dict)

        return ToolResult(
            success=True,
            output={"count": count},
        )
