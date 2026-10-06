"""Experimental scenario-level client API.

Two layers only: the scenario layer (:class:`Memcache` /
:class:`AsyncMemcache`, one door per usage scenario, business values in and
out) and the protocol layer (``cache.meta``, a 1:1 typed surface over the
meta protocol plus a raw ``execute`` escape hatch). Everything under
``memcache.experiment`` may change in any release.
"""

from ..errors import (
    AmbiguousWriteError,
    CommandError,
    ConflictError,
    MemcacheError,
    NotFoundError,
    OperationFailedError,
    ProtocolError,
    SerializeError,
)
from ..meta_command import MetaCommand, MetaResult
from ..serialize import (
    CompressedSerializer,
    JsonSerializer,
    PickleSerializer,
    Serializer,
    StrictSerializer,
)
from .async_client import AsyncBatch, AsyncMemcache, AsyncMetaNamespace
from .client import (
    FOREVER,
    Batch,
    Deferred,
    ItemInfo,
    Memcache,
    MetaNamespace,
    Ttl,
)
from .meta_api import MetaCommandResult

__all__ = [
    "AmbiguousWriteError",
    "AsyncBatch",
    "AsyncMemcache",
    "AsyncMetaNamespace",
    "Batch",
    "CommandError",
    "CompressedSerializer",
    "ConflictError",
    "Deferred",
    "FOREVER",
    "ItemInfo",
    "JsonSerializer",
    "Memcache",
    "MemcacheError",
    "MetaCommand",
    "MetaCommandResult",
    "MetaNamespace",
    "MetaResult",
    "NotFoundError",
    "OperationFailedError",
    "PickleSerializer",
    "ProtocolError",
    "SerializeError",
    "Serializer",
    "StrictSerializer",
    "Ttl",
]
