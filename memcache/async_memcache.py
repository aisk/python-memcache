from typing import Any

from .async_connection import AsyncConnection  # noqa: F401 re-export
from .connection import Addr
from .errors import MemcacheError
from .experiment import Get, Meta
from .experiment.async_meta_client import AsyncMetaClient
from .experiment.result import GetStatus, MutationStatus
from .meta_command import MetaCommand, MetaResult
from .serialize import dump, load, DumpFunc, FuncSerializer, LoadFunc

__all__ = ["AsyncConnection", "AsyncMemcache"]


class AsyncMemcache:
    """
    Async Memcache client.

    :param addr: memcached server addresses to be connected.

      The address can be a two elements tuple, as ``(ip, port)`` format.

      The address can be None, thus the default server ``("localhost", 11211)`` should
      be used.

      The address can be a list of tuple, like ``[("192.168.1.10", 11211),
      ("192.168.1.11", 11211)]``. In this situation, the keys will be hashed to one
      of those servers by consistent hash algorithm.
    :param max_idle: The max number of idle connections to keep per server.
      Connections are created on demand and returned to the pool after use; a
      connection returned while the pool already holds ``max_idle`` idle
      connections is closed instead. Pass None to keep every connection.
    :param timeout: Timeout in seconds for every operation. Pass None to wait
      indefinitely.
    :param load_func: Function to load the bytes content from memcached to python
      values.
    :param dump_func: Function to dump the python values to bytes content to store in
      memcached.
    :param username: Memcached ASCII protocol authentication username.
    :param password: Memcached ASCII protocol authentication password.
    """

    def __init__(
        self,
        addr: Addr | list[Addr] | None = None,
        *,
        max_idle: int | None = 23,
        timeout: float | None = 1.0,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump,
        username: str | None = None,
        password: str | None = None,
    ):
        self._meta = AsyncMetaClient(
            addr,
            max_idle=max_idle,
            timeout=timeout,
            serializer=FuncSerializer(dump_func, load_func),
            username=username,
            password=password,
        )

    async def __aenter__(self) -> "AsyncMemcache":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._meta.close()

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return await self._meta.execute_meta_command(command)

    async def flush_all(self) -> None:
        await self._meta.flush_all()

    async def set(
        self, key: bytes | str, value: Any, *, expire: int | None = None
    ) -> None:
        await self._meta.set(key, value, ttl=expire)

    async def get(self, key: bytes | str) -> Any | None:
        r = await self._meta.get(key)
        return r.value if r.status is GetStatus.HIT else None

    async def gets(self, key: bytes | str) -> tuple[Any, int] | None:
        """
        Get a value and its CAS token from memcached.

        :param key: The key to retrieve
        :return: A tuple of (value, cas_token) or None if key doesn't exist
        """
        r = await self._meta.get(key, meta=Meta.CAS)
        if r.status is not GetStatus.HIT:
            return None
        if r.cas_token is None:
            raise MemcacheError("CAS token not found in response")
        return r.value, r.cas_token

    async def cas(
        self,
        key: bytes | str,
        value: Any,
        cas_token: int,
        *,
        expire: int | None = None,
    ) -> None:
        """
        Store a value using compare-and-swap operation.

        :param key: The key to store
        :param value: The value to store
        :param cas_token: The CAS token from a previous gets operation
        :param expire: Optional expiration time in seconds
        :raises MemcacheError: If the CAS token doesn't match or other error occurs
        """
        result = await self._meta.cas(key, value, cas_token, ttl=expire)
        if result.status is not MutationStatus.STORED:
            raise MemcacheError("CAS operation failed: token mismatch or other error")

    async def delete(self, key: bytes | str) -> bool:
        return (await self._meta.delete(key)).status is MutationStatus.STORED

    async def touch(self, key: bytes | str, expire: int) -> bool:
        return (await self._meta.touch(key, expire)).status is MutationStatus.STORED

    async def add(
        self, key: bytes | str, value: Any, *, expire: int | None = None
    ) -> bool:
        return (
            await self._meta.add(key, value, ttl=expire)
        ).status is MutationStatus.STORED

    async def replace(
        self, key: bytes | str, value: Any, *, expire: int | None = None
    ) -> bool:
        result = await self._meta.set(key, value, ttl=expire, mode="replace")
        return result.status is MutationStatus.STORED

    async def append(self, key: bytes | str, value: Any) -> bool:
        return (await self._meta.append(key, value)).status is MutationStatus.STORED

    async def prepend(self, key: bytes | str, value: Any) -> bool:
        return (await self._meta.prepend(key, value)).status is MutationStatus.STORED

    async def get_many(self, keys: list[bytes | str]) -> dict[str, Any]:
        results = await self._meta.batch([Get(key) for key in keys])
        return {
            r.key if isinstance(r.key, str) else r.key.decode("latin-1"): r.value
            for r in results
            if r.status is GetStatus.HIT
        }

    async def incr(self, key: bytes | str, value: int = 1) -> int:
        result = await self._meta.increment(key, value)
        if result.status is not MutationStatus.STORED or result.value is None:
            raise MemcacheError("key not found")
        return result.value

    async def decr(self, key: bytes | str, value: int = 1) -> int:
        result = await self._meta.decrement(key, value)
        if result.status is not MutationStatus.STORED or result.value is None:
            raise MemcacheError("key not found")
        return result.value
