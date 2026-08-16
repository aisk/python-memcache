from typing import Any

from .connection import Addr, Connection  # noqa: F401 re-export for backward compat
from .errors import CasMismatchError, MemcacheError
from .experiment._core import WireOp, settle
from .experiment.client import Memcache as _ScenarioMemcache
from .experiment.meta_api import MetaCommandResult, build_get
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
        servers = _server_list(addr)
        self._serializer = FuncSerializer(dump_func, load_func)
        self._client = _ScenarioMemcache(
            *servers,
            serializer=self._serializer,
            max_idle=max_idle,
            timeout=timeout,
            username=username,
            password=password,
        )
        self._meta = self._client.meta

    def __enter__(self) -> "Memcache":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return self._client._execute_meta(command)

    def flush_all(self) -> None:
        self._client.flush_all()

    def set(self, key: bytes | str, value: Any, *, expire: int | None = None) -> None:
        raw, flags = self._serializer.dump(key, value)
        self._meta.set(key, raw, client_flags=flags, ttl=expire)

    def _load_hit(self, key: bytes | str, result: MetaCommandResult) -> Any:
        return self._serializer.load(key, result.value or b"", result.client_flags or 0)

    def get(self, key: bytes | str) -> Any | None:
        result = self._meta.get(key, return_client_flags=True)
        if result.rc != b"VA":
            return None
        return self._load_hit(key, result)

    def gets(self, key: bytes | str) -> tuple[Any, int] | None:
        """
        Get a value and its CAS token from memcached.

        :param key: The key to retrieve
        :return: A tuple of (value, cas_token) or None if key doesn't exist
        """
        result = self._meta.get(key, return_client_flags=True, return_cas=True)
        if result.rc != b"VA":
            return None
        if result.cas is None:
            raise MemcacheError("CAS token not found in response")
        return self._load_hit(key, result), result.cas

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
        raw, flags = self._serializer.dump(key, value)
        result = self._meta.set(
            key, raw, client_flags=flags, ttl=expire, compare_cas=cas_token
        )
        if result.rc == b"EX":
            raise CasMismatchError("CAS token mismatch")
        if result.rc == b"NF":
            raise MemcacheError("key not found")
        if not result.ok:
            raise MemcacheError("CAS operation failed: %r" % result.rc)

    def delete(self, key: bytes | str) -> bool:
        return self._meta.delete(key).ok

    def touch(self, key: bytes | str, expire: int) -> bool:
        return self._meta.get(key, value=False, touch=expire).ok

    def add(self, key: bytes | str, value: Any, *, expire: int | None = None) -> bool:
        raw, flags = self._serializer.dump(key, value)
        return self._meta.set(key, raw, client_flags=flags, ttl=expire, mode="add").ok

    def replace(
        self, key: bytes | str, value: Any, *, expire: int | None = None
    ) -> bool:
        raw, flags = self._serializer.dump(key, value)
        return self._meta.set(
            key, raw, client_flags=flags, ttl=expire, mode="replace"
        ).ok

    def append(self, key: bytes | str, value: bytes | str) -> bool:
        if isinstance(value, str):
            value = value.encode()
        return self._meta.set(key, value, mode="append").ok

    def prepend(self, key: bytes | str, value: bytes | str) -> bool:
        if isinstance(value, str):
            value = value.encode()
        return self._meta.set(key, value, mode="prepend").ok

    def get_many(self, keys: list[bytes | str]) -> dict[bytes | str, Any]:
        """Read several keys at once, keyed by the key objects passed in.

        A hit is reported under the very key the caller handed over, bytes or
        str, so the result stays indexable by whatever the caller already has.
        """
        ops = [
            WireOp(
                build_get(key, value=True, return_client_flags=True),
                side_effect=False,
            )
            for key in keys
        ]
        outcomes = self._client._run_ops(ops)
        found: dict[bytes | str, Any] = {}
        for key, outcome in zip(keys, outcomes):
            response = settle(key, outcome)
            if response is not None and response.rc == b"VA":
                found[key] = self._load_hit(key, response)
        return found

    def _arithmetic(self, key: bytes | str, delta: int, decrement: bool) -> int:
        result = self._meta.arithmetic(key, delta=delta, decrement=decrement)
        verb = "decrement" if decrement else "increment"
        if result.rc == b"NF":
            raise MemcacheError("key not found")
        if result.rc != b"VA" or not result.value:
            raise MemcacheError("%s failed: %r" % (verb, result.rc))
        return int(result.value)

    def incr(self, key: bytes | str, value: int = 1) -> int:
        return self._arithmetic(key, value, False)

    def decr(self, key: bytes | str, value: int = 1) -> int:
        return self._arithmetic(key, value, True)


def _server_list(addr: Addr | list[Addr] | None) -> list[Addr]:
    if addr is None:
        return []
    if isinstance(addr, tuple):
        return [addr]
    if isinstance(addr, list) and addr:
        return list(addr)
    raise TypeError("addr must be a server tuple or a non-empty list")
