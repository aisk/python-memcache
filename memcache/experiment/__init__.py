"""Experimental scenario-level client API.

Two layers only: the scenario layer (:class:`Memcache` /
:class:`AsyncMemcache`, one door per usage scenario, business values in and
out) and the protocol layer (``cache.meta``, a 1:1 typed surface over the
meta protocol plus a raw ``execute`` escape hatch). Everything under
``memcache.experiment`` may change in any release.
"""

from ..errors import (
    AmbiguousWriteError,
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
from .async_client import AsyncMemcache, AsyncMetaNamespace, AsyncPipeline
from .client import (
    FOREVER,
    Deferred,
    ItemInfo,
    Memcache,
    MetaNamespace,
    Pipeline,
)
from .meta_api import MetaCommandResult

__all__ = [
    "AmbiguousWriteError",
    "AsyncMemcache",
    "AsyncMetaNamespace",
    "AsyncPipeline",
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
    "Pipeline",
    "ProtocolError",
    "SerializeError",
    "Serializer",
    "StrictSerializer",
]
