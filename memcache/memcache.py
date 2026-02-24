from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .connection import Addr, Connection, Pool  # re-export for backward compat
from .errors import MemcacheError
from .experiment.meta_client import MetaClient
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
        addr: Union[Addr, List[Addr], None] = None,
        *,
        pool_size: Optional[int] = 23,
        pool_timeout: Optional[int] = 1,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump,
        username: Optional[str] = None,
        password: Optional[str] = None,
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

    @contextmanager
    def _get_connection(self, key: Union[str, bytes]) -> Iterator[Connection]:
        with self._meta._get_connection(key) as conn:
            yield conn

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return self._meta.execute_meta_command(command)

    def flush_all(self) -> None:
        self._meta.flush_all()

    def set(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> None:
        self._meta.set(key, value, expire=expire)

    def get(self, key: Union[bytes, str]) -> Optional[Any]:
        r = self._meta.get(key)
        return r.value if r is not None else None

    def gets(self, key: Union[bytes, str]) -> Optional[Tuple[Any, int]]:
        """
        Get a value and its CAS token from memcached.

        :param key: The key to retrieve
        :return: A tuple of (value, cas_token) or None if key doesn't exist
        """
        r = self._meta.get(key, return_cas=True)
        if r is None:
            return None
        if r.cas_token is None:
            raise MemcacheError("CAS token not found in response")
        return r.value, r.cas_token

    def cas(
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
        ok = self._meta.cas(key, value, cas_token, expire=expire)
        if not ok:
            raise MemcacheError("CAS operation failed: token mismatch or other error")

    def delete(self, key: Union[bytes, str]) -> bool:
        return self._meta.delete(key)

    def touch(self, key: Union[bytes, str], expire: int) -> bool:
        return self._meta.touch(key, expire)

    def add(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> bool:
        return self._meta.add(key, value, expire=expire)

    def replace(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> bool:
        return self._meta.replace(key, value, expire=expire)

    def append(self, key: Union[bytes, str], value: Any) -> bool:
        return self._meta.append(key, value)

    def prepend(self, key: Union[bytes, str], value: Any) -> bool:
        return self._meta.prepend(key, value)

    def get_many(self, keys: List[Union[bytes, str]]) -> Dict[str, Any]:
        results = self._meta.get_many(keys)
        return {k: v.value for k, v in results.items()}

    def incr(self, key: Union[bytes, str], value: int = 1) -> int:
        return self._meta.incr(key, value)

    def decr(self, key: Union[bytes, str], value: int = 1) -> int:
        return self._meta.decr(key, value)
