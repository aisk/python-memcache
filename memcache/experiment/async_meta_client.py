from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import hashring

from ..async_connection import AsyncConnection, AsyncPool
from ..connection import Addr
from ..errors import MemcacheError
from ..meta_command import MetaCommand, MetaResult
from ..serialize import dump, load, DumpFunc, LoadFunc
from .meta_client import _parse_flags
from .result import GetResult


class AsyncMetaClient:
    """
    Async memcache client with full meta protocol capability.

    Mirror of MetaClient using anyio-based AsyncConnection/AsyncPool.
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
        self._load = load_func
        self._dump = dump_func

        addr = addr or ("localhost", 11211)
        if isinstance(addr, list):
            addrs: List[Addr] = addr
            nodes: List[AsyncPool] = []
            for a in addrs:
                def _make(a: Addr = a) -> AsyncConnection:
                    return AsyncConnection(
                        a,
                        username=username,
                        password=password,
                    )
                nodes.append(
                    AsyncPool(_make, max_size=pool_size, timeout=pool_timeout)
                )
            self._connections = hashring.HashRing(nodes)
        elif isinstance(addr, tuple):
            a_single: Addr = addr

            def _make_single() -> AsyncConnection:
                return AsyncConnection(
                    a_single,
                    username=username,
                    password=password,
                )
            self._connections = hashring.HashRing(
                [AsyncPool(_make_single, max_size=pool_size, timeout=pool_timeout)]
            )
        else:
            raise TypeError("invalid type for addr")

    @asynccontextmanager
    async def _get_connection(
        self, key: Union[str, bytes]
    ) -> AsyncIterator[AsyncConnection]:
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        pool = self._connections.get_node(key)
        async with pool.get() as connection:
            yield connection

    @staticmethod
    def _to_bytes(key: Union[str, bytes]) -> bytes:
        if isinstance(key, str):
            return key.encode()
        return key

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        async with self._get_connection(command.key) as connection:
            return await connection.execute_meta_command(command)

    # ------------------------------------------------------------------ #
    # Meta Get (mg)                                                      #
    # ------------------------------------------------------------------ #

    async def get(
        self,
        key: Union[str, bytes],
        *,
        return_cas: bool = False,
        return_ttl: bool = False,
        return_last_access: bool = False,
        return_size: bool = False,
        return_hit_before: bool = False,
        update_ttl: Optional[int] = None,
        no_lru_bump: bool = False,
        vivify_on_miss_ttl: Optional[int] = None,
        recache_ttl_threshold: Optional[int] = None,
        check_cas: Optional[int] = None,
    ) -> Optional[GetResult[Any]]:
        key_bytes = self._to_bytes(key)
        flags: List[bytes] = [b"v", b"f"]
        if return_cas:
            flags.append(b"c")
        if return_ttl:
            flags.append(b"t")
        if return_last_access:
            flags.append(b"l")
        if return_size:
            flags.append(b"s")
        if return_hit_before:
            flags.append(b"h")
        if update_ttl is not None:
            flags.append(b"T%d" % update_ttl)
        if no_lru_bump:
            flags.append(b"u")
        if vivify_on_miss_ttl is not None:
            flags.append(b"N%d" % vivify_on_miss_ttl)
        if recache_ttl_threshold is not None:
            flags.append(b"R%d" % recache_ttl_threshold)
        if check_cas is not None:
            flags.append(b"C%d" % check_cas)

        command = MetaCommand(cm=b"mg", key=key_bytes, flags=flags)
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"EN":
            return None

        parsed = _parse_flags(result.flags)

        gr: GetResult[Any] = GetResult()
        gr.is_stale = parsed.get("is_stale", False)
        gr.won_recache = parsed.get("won_recache", False)
        gr.already_won = parsed.get("already_won", False)
        if return_cas:
            gr.cas_token = parsed.get("cas_token")
        if return_ttl:
            gr.ttl = parsed.get("ttl")
        if return_last_access:
            gr.last_access = parsed.get("last_access")
        if return_size:
            gr.size = parsed.get("size")
        if return_hit_before:
            gr.hit_before = parsed.get("hit_before")
        if "key" in parsed:
            gr.key = parsed["key"]

        if result.value is not None and len(result.value) > 0:
            client_flags = parsed.get("client_flags", 0)
            gr.value = self._load(key_bytes, result.value, client_flags)

        return gr

    async def touch(self, key: Union[str, bytes], expire: int) -> bool:
        """Update the TTL of a key without returning its value."""
        key_bytes = self._to_bytes(key)
        command = MetaCommand(
            cm=b"mg",
            key=key_bytes,
            flags=[b"T%d" % expire],
        )
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)
        return result.rc != b"EN"

    async def get_many(
        self,
        keys: List[Union[str, bytes]],
    ) -> Dict[str, GetResult[Any]]:
        """Retrieve multiple keys; missing keys are omitted from the result."""
        result: Dict[str, GetResult[Any]] = {}
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode("utf-8")
            gr = await self.get(key)
            if gr is not None:
                result[key_str] = gr
        return result

    # ------------------------------------------------------------------ #
    # Meta Set (ms)                                                      #
    # ------------------------------------------------------------------ #

    async def set(
        self,
        key: Union[str, bytes],
        value: Any,
        *,
        expire: Optional[int] = None,
    ) -> None:
        key_bytes = self._to_bytes(key)
        raw_value, client_flags = self._dump(key_bytes, value)
        flags: List[bytes] = [b"F%d" % client_flags]
        if expire is not None:
            flags.append(b"T%d" % expire)

        command = MetaCommand(
            cm=b"ms",
            key=key_bytes,
            datalen=len(raw_value),
            flags=flags,
            value=raw_value,
        )
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc != b"HD":
            raise MemcacheError(f"set failed: {result.rc.decode()}")

    async def add(
        self,
        key: Union[str, bytes],
        value: Any,
        *,
        expire: Optional[int] = None,
    ) -> bool:
        """Store only if key does not already exist. Returns True on success."""
        key_bytes = self._to_bytes(key)
        raw_value, client_flags = self._dump(key_bytes, value)
        flags: List[bytes] = [b"ME", b"F%d" % client_flags]
        if expire is not None:
            flags.append(b"T%d" % expire)

        command = MetaCommand(
            cm=b"ms",
            key=key_bytes,
            datalen=len(raw_value),
            flags=flags,
            value=raw_value,
        )
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"HD":
            return True
        if result.rc == b"NS":
            return False
        raise MemcacheError(f"add failed: {result.rc.decode()}")

    async def replace(
        self,
        key: Union[str, bytes],
        value: Any,
        *,
        expire: Optional[int] = None,
    ) -> bool:
        """Store only if key already exists. Returns True on success."""
        key_bytes = self._to_bytes(key)
        raw_value, client_flags = self._dump(key_bytes, value)
        flags: List[bytes] = [b"MR", b"F%d" % client_flags]
        if expire is not None:
            flags.append(b"T%d" % expire)

        command = MetaCommand(
            cm=b"ms",
            key=key_bytes,
            datalen=len(raw_value),
            flags=flags,
            value=raw_value,
        )
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"HD":
            return True
        if result.rc == b"NS":
            return False
        raise MemcacheError(f"replace failed: {result.rc.decode()}")

    async def append(
        self,
        key: Union[str, bytes],
        value: Any,
        *,
        vivify_ttl: Optional[int] = None,
    ) -> bool:
        """Append value to an existing key. Returns True on success."""
        key_bytes = self._to_bytes(key)
        raw_value, client_flags = self._dump(key_bytes, value)
        flags: List[bytes] = [b"MA", b"F%d" % client_flags]
        if vivify_ttl is not None:
            flags.append(b"N%d" % vivify_ttl)

        command = MetaCommand(
            cm=b"ms",
            key=key_bytes,
            datalen=len(raw_value),
            flags=flags,
            value=raw_value,
        )
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"HD":
            return True
        if result.rc == b"NS":
            return False
        raise MemcacheError(f"append failed: {result.rc.decode()}")

    async def prepend(
        self,
        key: Union[str, bytes],
        value: Any,
        *,
        vivify_ttl: Optional[int] = None,
    ) -> bool:
        """Prepend value to an existing key. Returns True on success."""
        key_bytes = self._to_bytes(key)
        raw_value, client_flags = self._dump(key_bytes, value)
        flags: List[bytes] = [b"MP", b"F%d" % client_flags]
        if vivify_ttl is not None:
            flags.append(b"N%d" % vivify_ttl)

        command = MetaCommand(
            cm=b"ms",
            key=key_bytes,
            datalen=len(raw_value),
            flags=flags,
            value=raw_value,
        )
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"HD":
            return True
        if result.rc == b"NS":
            return False
        raise MemcacheError(f"prepend failed: {result.rc.decode()}")

    async def cas(
        self,
        key: Union[str, bytes],
        value: Any,
        cas_token: int,
        *,
        expire: Optional[int] = None,
    ) -> bool:
        """Compare-and-swap. Returns True on success, False on CAS conflict."""
        key_bytes = self._to_bytes(key)
        raw_value, client_flags = self._dump(key_bytes, value)
        flags: List[bytes] = [b"F%d" % client_flags, b"C%d" % cas_token]
        if expire is not None:
            flags.append(b"T%d" % expire)

        command = MetaCommand(
            cm=b"ms",
            key=key_bytes,
            datalen=len(raw_value),
            flags=flags,
            value=raw_value,
        )
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"HD":
            return True
        if result.rc in (b"EX", b"NF"):
            return False
        raise MemcacheError(f"cas failed: {result.rc.decode()}")

    # ------------------------------------------------------------------ #
    # Meta Delete (md)                                                   #
    # ------------------------------------------------------------------ #

    async def delete(
        self,
        key: Union[str, bytes],
        *,
        cas_token: Optional[int] = None,
    ) -> bool:
        """Delete a key. Returns True on success, False if key not found."""
        key_bytes = self._to_bytes(key)
        flags: List[bytes] = []
        if cas_token is not None:
            flags.append(b"C%d" % cas_token)

        command = MetaCommand(cm=b"md", key=key_bytes, flags=flags)
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"HD":
            return True
        if result.rc in (b"NF", b"EX"):
            return False
        raise MemcacheError(f"delete failed: {result.rc.decode()}")

    async def invalidate(
        self,
        key: Union[str, bytes],
        *,
        stale_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> bool:
        """Mark a key as stale (stale-while-revalidate pattern)."""
        key_bytes = self._to_bytes(key)
        flags: List[bytes] = [b"I"]
        if stale_ttl is not None:
            flags.append(b"T%d" % stale_ttl)
        if cas_token is not None:
            flags.append(b"C%d" % cas_token)

        command = MetaCommand(cm=b"md", key=key_bytes, flags=flags)
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"HD":
            return True
        if result.rc in (b"NF", b"EX"):
            return False
        raise MemcacheError(f"invalidate failed: {result.rc.decode()}")

    # ------------------------------------------------------------------ #
    # Meta Arithmetic (ma)                                               #
    # ------------------------------------------------------------------ #

    async def incr(
        self,
        key: Union[str, bytes],
        delta: int = 1,
        *,
        initial: Optional[int] = None,
        initial_ttl: Optional[int] = None,
        update_ttl: Optional[int] = None,
    ) -> int:
        """Increment counter. Raises MemcacheError if key missing and no initial."""
        key_bytes = self._to_bytes(key)
        flags: List[bytes] = [b"D%d" % delta, b"v"]
        if initial is not None:
            flags.append(b"J%d" % initial)
            if initial_ttl is not None:
                flags.append(b"N%d" % initial_ttl)
        if update_ttl is not None:
            flags.append(b"T%d" % update_ttl)

        command = MetaCommand(cm=b"ma", key=key_bytes, flags=flags)
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"NF":
            raise MemcacheError("key not found")
        if result.rc != b"VA":
            raise MemcacheError(f"incr failed: {result.rc.decode()}")
        if result.value is None:
            raise MemcacheError("incr: no value returned")
        return int(result.value)

    async def decr(
        self,
        key: Union[str, bytes],
        delta: int = 1,
        *,
        initial: Optional[int] = None,
        initial_ttl: Optional[int] = None,
        update_ttl: Optional[int] = None,
    ) -> int:
        """Decrement counter (floor 0). Raises MemcacheError if key missing."""
        key_bytes = self._to_bytes(key)
        flags: List[bytes] = [b"D%d" % delta, b"MD", b"v"]
        if initial is not None:
            flags.append(b"J%d" % initial)
            if initial_ttl is not None:
                flags.append(b"N%d" % initial_ttl)
        if update_ttl is not None:
            flags.append(b"T%d" % update_ttl)

        command = MetaCommand(cm=b"ma", key=key_bytes, flags=flags)
        async with self._get_connection(key_bytes) as connection:
            result = await connection.execute_meta_command(command)

        if result.rc == b"NF":
            raise MemcacheError("key not found")
        if result.rc != b"VA":
            raise MemcacheError(f"decr failed: {result.rc.decode()}")
        if result.value is None:
            raise MemcacheError("decr: no value returned")
        return int(result.value)

    # ------------------------------------------------------------------ #
    # Other                                                              #
    # ------------------------------------------------------------------ #

    async def flush_all(self, delay: int = 0) -> None:
        for pool in self._connections.nodes:
            async with pool.get() as connection:
                await connection.flush_all(delay)
