"""Error types for LocalFirstStorage."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amplifier_module_storage_localfirst.types import Conflict


class StorageError(Exception):
    """Base class for storage errors."""

    pass


class NotFoundError(StorageError):
    """Entity not found in storage.

    Attributes:
        collection: Name of the collection.
        entity_id: ID that was not found.
    """

    def __init__(self, collection: str, entity_id: str):
        self.collection = collection
        self.entity_id = entity_id
        super().__init__(f"Not found: {collection}/{entity_id}")


class SchemaError(StorageError):
    """Schema validation or registration error.

    Raised when:
    - Schema definition is invalid
    - Collection not registered
    - Field type mismatch
    """

    pass


class SyncError(StorageError):
    """Sync operation failed.

    Raised when backend communication fails or sync cannot complete.
    """

    pass


class ConflictError(SyncError):
    """Unresolved sync conflict.

    Raised when conflict_strategy is "manual" and a conflict is detected.

    Attributes:
        conflict: The conflict details.
    """

    def __init__(self, conflict: Conflict):
        self.conflict = conflict
        super().__init__(f"Conflict: {conflict.collection}/{conflict.entity_id}")


class NotSupportedError(StorageError):
    """Operation not supported.

    Raised when:
    - Vector search called but vectors not enabled
    - Sync called but backend_url not configured
    """

    pass
