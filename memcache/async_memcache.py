from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Union

from .async_connection import AsyncConnection, AsyncPool  # noqa: F401 re-export
from .connection import Addr
from .errors import MemcacheError
from .experiment.async_meta_client import AsyncMetaClient
from .meta_command import MetaCommand, MetaResult
from .serialize import dump, load, DumpFunc, LoadFunc

__all__ = ["AsyncConnection", "AsyncPool", "AsyncMemcache"]


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
    :param pool_size: The connection pool size. This size will be used as the max
      number to keep the connections for future uses.
    :param pool_timeout: If the there is no available connection in the pool, and the
      ``pool_size`` is reached, wait the specified time to get an available connection,
      or a `asyncio.TimeoutError` is raised.
    :param load_func: Function to load the bytes content from memcached to python
      values.
    :param dump_func: Function to dump the python values to bytes content to store in
      memcached.
    :param username: Memcached ASCII protocol authentication username.
    :param password: Memcached ASCII protocol authentication password.
    """

    def __init__(
        self,
        addr: Union[Addr, List[Addr], None] = None,
        *,
        pool_size: Optional[int] = 23,
        pool_timeout: Optional[int] = 1,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._meta = AsyncMetaClient(
            addr,
            pool_size=pool_size,
            pool_timeout=pool_timeout,
            load_func=load_func,
            dump_func=dump_func,
            username=username,
            password=password,
        )

    @asynccontextmanager
    async def _get_connection(
        self, key: Union[str, bytes]
    ) -> AsyncIterator[AsyncConnection]:
        async with self._meta._get_connection(key) as conn:
            yield conn

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return await self._meta.execute_meta_command(command)

    async def flush_all(self) -> None:
        await self._meta.flush_all()

    async def set(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> None:
        await self._meta.set(key, value, expire=expire)

    async def get(self, key: Union[bytes, str]) -> Optional[Any]:
        r = await self._meta.get(key)
        return r.value if r is not None else None

    async def gets(self, key: Union[bytes, str]) -> Optional[Tuple[Any, int]]:
        """
        Get a value and its CAS token from memcached.

        :param key: The key to retrieve
        :return: A tuple of (value, cas_token) or None if key doesn't exist
        """
        r = await self._meta.get(key, return_cas=True)
        if r is None:
            return None
        if r.cas_token is None:
            raise MemcacheError("CAS token not found in response")
        return r.value, r.cas_token

    async def cas(
        self,
        key: Union[bytes, str],
        value: Any,
        cas_token: int,
        *,
        expire: Optional[int] = None,
    ) -> None:
        """
        Store a value using compare-and-swap operation.

        :param key: The key to store
        :param value: The value to store
        :param cas_token: The CAS token from a previous gets operation
        :param expire: Optional expiration time in seconds
        :raises MemcacheError: If the CAS token doesn't match or other error occurs
        """
        ok = await self._meta.cas(key, value, cas_token, expire=expire)
        if not ok:
            raise MemcacheError("CAS operation failed: token mismatch or other error")

    async def delete(self, key: Union[bytes, str]) -> bool:
        return await self._meta.delete(key)

    async def touch(self, key: Union[bytes, str], expire: int) -> bool:
        return await self._meta.touch(key, expire)

    async def add(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> bool:
        return await self._meta.add(key, value, expire=expire)

    async def replace(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> bool:
        return await self._meta.replace(key, value, expire=expire)

    async def append(self, key: Union[bytes, str], value: Any) -> bool:
        return await self._meta.append(key, value)

    async def prepend(self, key: Union[bytes, str], value: Any) -> bool:
        return await self._meta.prepend(key, value)

    async def get_many(self, keys: List[Union[bytes, str]]) -> Dict[str, Any]:
        results = await self._meta.get_many(keys)
        return {k: v.value for k, v in results.items()}

    async def incr(self, key: Union[bytes, str], value: int = 1) -> int:
        return await self._meta.incr(key, value)

    async def decr(self, key: Union[bytes, str], value: int = 1) -> int:
        return await self._meta.decr(key, value)
