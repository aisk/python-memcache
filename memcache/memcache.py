from contextlib import contextmanager
from typing import Any
from collections.abc import Iterator

import hashring

from .connection import Addr, Connection, Pool  # re-export for backward compat
from .errors import MemcacheError
from .experiment.meta_client import MetaClient
from .experiment.operation import Get
from .experiment.result import GetStatus, Meta, MutationStatus
from .meta_command import MetaCommand, MetaResult
from .serialize import dump, load, DumpFunc, LoadFunc

__all__ = ["Addr", "Connection", "Pool", "Memcache"]


class Memcache:
    """
    Memcache client.

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
      or a `queue.Empty` is raised.
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
        pool_size: int | None = 23,
        pool_timeout: int | None = 1,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump,
        username: str | None = None,
        password: str | None = None,
    ):
        self._meta = MetaClient(
            addr,
            pool_size=pool_size,
            pool_timeout=pool_timeout,
            load_func=load_func,
            dump_func=dump_func,
            username=username,
            password=password,
        )
        compat_addr = addr or ("localhost", 11211)
        addresses = compat_addr if isinstance(compat_addr, list) else [compat_addr]
        pools = []
        for server in addresses:

            def make(server: Addr = server) -> Connection:
                return Connection(server, username=username, password=password)

            pools.append(Pool(make, max_size=pool_size, timeout=pool_timeout))
        self._compat_connections = hashring.HashRing(pools)

    def __enter__(self) -> "Memcache":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        self._meta.close()
        for pool in self._compat_connections.nodes:
            pool.close()

    @contextmanager
    def _get_connection(self, key: str | bytes) -> Iterator[Connection]:
        routing_key = key if isinstance(key, str) else key.decode("latin-1")
        pool = self._compat_connections.get_node(routing_key)
        with pool.get() as conn:
            yield conn

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return self._meta.execute_meta_command(command)

    def flush_all(self) -> None:
        self._meta.flush_all()

    def set(self, key: bytes | str, value: Any, *, expire: int | None = None) -> None:
        self._meta.set(key, value, ttl=expire)

    def get(self, key: bytes | str) -> Any | None:
        r = self._meta.get(key)
        return r.value if r.status is GetStatus.HIT else None

    def gets(self, key: bytes | str) -> tuple[Any, int] | None:
        """
        Get a value and its CAS token from memcached.

        :param key: The key to retrieve
        :return: A tuple of (value, cas_token) or None if key doesn't exist
        """
        r = self._meta.get(key, meta=Meta.CAS)
        if r.status is not GetStatus.HIT:
            return None
        if r.cas_token is None:
            raise MemcacheError("CAS token not found in response")
        return r.value, r.cas_token

    def cas(
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
        result = self._meta.cas(key, value, cas_token, ttl=expire)
        if result.status is not MutationStatus.STORED:
            raise MemcacheError("CAS operation failed: token mismatch or other error")

    def delete(self, key: bytes | str) -> bool:
        return self._meta.delete(key).status is MutationStatus.STORED

    def touch(self, key: bytes | str, expire: int) -> bool:
        return self._meta.touch(key, expire).status is MutationStatus.STORED

    def add(self, key: bytes | str, value: Any, *, expire: int | None = None) -> bool:
        return self._meta.add(key, value, ttl=expire).status is MutationStatus.STORED

    def replace(
        self, key: bytes | str, value: Any, *, expire: int | None = None
    ) -> bool:
        result = self._meta.set(key, value, ttl=expire, mode="replace")
        return result.status is MutationStatus.STORED

    def append(self, key: bytes | str, value: Any) -> bool:
        return self._meta.append(key, value).status is MutationStatus.STORED

    def prepend(self, key: bytes | str, value: Any) -> bool:
        return self._meta.prepend(key, value).status is MutationStatus.STORED

    def get_many(self, keys: list[bytes | str]) -> dict[str, Any]:
        results = self._meta.batch([Get(key) for key in keys])
        return {
            r.key if isinstance(r.key, str) else r.key.decode("latin-1"): r.value
            for r in results
            if r.status is GetStatus.HIT
        }

    def incr(self, key: bytes | str, value: int = 1) -> int:
        result = self._meta.increment(key, value)
        if result.status is not MutationStatus.STORED or result.value is None:
            raise MemcacheError("key not found")
        return result.value

    def decr(self, key: bytes | str, value: int = 1) -> int:
        result = self._meta.decrement(key, value)
        if result.status is not MutationStatus.STORED or result.value is None:
            raise MemcacheError("key not found")
        return result.value
