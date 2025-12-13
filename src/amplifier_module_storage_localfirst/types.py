"""Type definitions for LocalFirstStorage."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class FieldType(Enum):
    """Supported field types for schema definitions."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    DATE = "date"
    JSON = "json"  # For nested objects/arrays


@dataclass
class Schema:
    """Define the shape of entities in a collection.

    Attributes:
        name: Collection name.
        fields: Dict mapping field names to their types.
        primary_key: Field to use as primary key (default: "id").
        indexes: Fields to create indexes on for faster queries.
        vector_field: Field to embed for semantic search (if enabled).
    """

    name: str
    fields: dict[str, FieldType]
    primary_key: str = "id"
    indexes: list[str] | None = None
    vector_field: str | None = None


@dataclass
class StorageConfig:
    """Configuration for a LocalFirstStorage instance.

    Attributes:
        db_path: Path to the local database file.
        backend_url: Optional URL for backend sync. None = local only.
        auth_token: Optional auth token for backend API.
        conflict_strategy: How to handle sync conflicts:
            - "last_write_wins": Most recent timestamp wins (default)
            - "manual": Surface conflicts for manual resolution
            - "merge": Use custom merge handler
        auto_sync: Whether to sync automatically on changes.
        sync_interval: Seconds between background sync attempts.
        enable_vectors: Enable semantic search via embeddings.
        embedding_model: Model for generating embeddings.
    """

    db_path: str
    backend_url: str | None = None
    auth_token: str | None = None
    conflict_strategy: str = "last_write_wins"
    auto_sync: bool = True
    sync_interval: int = 60
    enable_vectors: bool = False
    embedding_model: str = "all-MiniLM-L6-v2"


@dataclass
class SyncResult:
    """Result of a sync operation.

    Attributes:
        pushed: Number of changes pushed to backend.
        pulled: Number of changes pulled from backend.
        conflicts: List of unresolved conflicts.
        errors: List of error messages.
        sync_token: Token for next incremental sync.
    """

    pushed: int = 0
    pulled: int = 0
    conflicts: list[Conflict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    sync_token: str | None = None


@dataclass
class Conflict:
    """A sync conflict between local and remote versions.

    Attributes:
        collection: Name of the collection.
        entity_id: ID of the conflicting entity.
        local_version: Local entity data.
        remote_version: Remote entity data.
        local_timestamp: When local version was modified.
        remote_timestamp: When remote version was modified.
    """

    collection: str
    entity_id: str
    local_version: dict[str, Any]
    remote_version: dict[str, Any]
    local_timestamp: datetime
    remote_timestamp: datetime


@dataclass
class Change:
    """A pending change to sync.

    Attributes:
        collection: Name of the collection.
        entity_id: ID of the entity.
        operation: Type of operation ("create", "update", "delete").
        data: Entity data (for create/update).
        timestamp: When the change occurred.
    """

    collection: str
    entity_id: str
    operation: str  # "create", "update", "delete"
    data: dict[str, Any]
    timestamp: datetime
