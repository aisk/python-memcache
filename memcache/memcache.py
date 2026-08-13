from typing import Any

from .connection import Addr, Connection  # re-export for backward compat
from .errors import CasMismatchError, MemcacheError
from .experiment.meta_client import MetaClient
from .experiment.operation import Get
from .experiment.result import Field, GetStatus, MutationStatus
from .meta_command import MetaCommand, MetaResult
from .serialize import dump, load, DumpFunc, FuncSerializer, LoadFunc

__all__ = ["Addr", "Connection", "Memcache"]


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
        self._meta = MetaClient(
            addr,
            max_idle=max_idle,
            timeout=timeout,
            serializer=FuncSerializer(dump_func, load_func),
            username=username,
            password=password,
        )

    def __enter__(self) -> "Memcache":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        self._meta.close()

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
        r = self._meta.get(key, fields=Field.CAS)
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
        :raises CasMismatchError: If the item changed since the CAS token was read
        :raises MemcacheError: If the key doesn't exist or the operation fails
        """
        result = self._meta.cas(key, value, cas_token, ttl=expire)
        if result.status is MutationStatus.CAS_MISMATCH:
            raise CasMismatchError("CAS token mismatch")
        if result.status is MutationStatus.NOT_FOUND:
            raise MemcacheError("key not found")
        if result.status is not MutationStatus.STORED:
            raise MemcacheError(
                f"CAS operation failed: {result.status.name}"
            ) from result.error

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

    def append(self, key: bytes | str, value: bytes | str) -> bool:
        if isinstance(value, str):
            value = value.encode()
        return self._meta.append(key, value).status is MutationStatus.STORED

    def prepend(self, key: bytes | str, value: bytes | str) -> bool:
        if isinstance(value, str):
            value = value.encode()
        return self._meta.prepend(key, value).status is MutationStatus.STORED

    def get_many(self, keys: list[bytes | str]) -> dict[bytes | str, Any]:
        """Read several keys at once, keyed by the key objects passed in.

        A hit is reported under the very key the caller handed over, bytes or
        str, so the result stays indexable by whatever the caller already has.
        """
        results = self._meta.batch([Get(key) for key in keys])
        return {r.key: r.value for r in results if r.status is GetStatus.HIT}

    def incr(self, key: bytes | str, value: int = 1) -> int:
        result = self._meta.increment(key, value)
        if result.status is MutationStatus.NOT_FOUND:
            raise MemcacheError("key not found")
        if result.status is not MutationStatus.STORED:
            raise MemcacheError(
                f"increment failed: {result.status.name}"
            ) from result.error
        if result.value is None:
            raise MemcacheError("no value in increment response")
        return result.value

    def decr(self, key: bytes | str, value: int = 1) -> int:
        result = self._meta.decrement(key, value)
        if result.status is MutationStatus.NOT_FOUND:
            raise MemcacheError("key not found")
        if result.status is not MutationStatus.STORED:
            raise MemcacheError(
                f"decrement failed: {result.status.name}"
            ) from result.error
        if result.value is None:
            raise MemcacheError("no value in decrement response")
        return result.value
