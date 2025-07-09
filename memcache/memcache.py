import asyncio
import threading
import queue
from contextlib import contextmanager
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

import hashring

from .async_memcache import Addr, AsyncConnection
from .meta_command import MetaCommand, MetaResult
from .serialize import DumpFunc, LoadFunc, dump, load


class Connection:
    def __init__(
        self,
        addr: Tuple[str, int],
        *,
        load_func: LoadFunc = load,
        dump_func: DumpFunc = dump,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._loop = asyncio.new_event_loop()
        self._async_connection = AsyncConnection(
            addr,
            load_func=load_func,
            dump_func=dump_func,
            username=username,
            password=password,
        )

    def flush_all(self) -> None:
        return self._loop.run_until_complete(self._async_connection.flush_all())

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return self._loop.run_until_complete(
            self._async_connection.execute_meta_command(command)
        )

    def set(
        self, key: Union[bytes, str], value: Any, expire: Optional[int] = None
    ) -> None:
        return self._loop.run_until_complete(
            self._async_connection.set(key, value, expire)
        )

    def get(self, key: Union[bytes, str]) -> Optional[Any]:
        return self._loop.run_until_complete(self._async_connection.get(key))

    def delete(self, key: Union[bytes, str]) -> None:
        return self._loop.run_until_complete(self._async_connection.delete(key))


class Pool:
    def __init__(
        self,
        create_connection: Callable[..., Connection],
        max_size: Optional[int],
        timeout: Optional[int],
    ) -> None:
        self._create_connection = create_connection
        self._max_size = max_size
        self._timeout = timeout
        self._size = 0
        self._lock = threading.Lock()
        self._connections: queue.Queue[Connection] = queue.Queue()

    @contextmanager
    def get(self) -> Iterator[Connection]:
        try:
            connection = self._connections.get_nowait()
            yield connection
            self._connections.put(connection)
        except queue.Empty:
            if self._max_size and self._size >= self._max_size:
                connection = self._connections.get(timeout=self._timeout)
                yield connection
                self._connections.put(connection)
            else:
                with self._lock:
                    self._size += 1
                connection = self._create_connection()
                yield connection
                self._connections.put(connection)


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
        addr = addr or ("localhost", 11211)
        if isinstance(addr, list):
            addrs: List[Addr] = addr
            nodes: List[Pool] = []
            for addr in addrs:
                create_connection = lambda: Connection(
                    addr,
                    load_func=load_func,
                    dump_func=dump_func,
                    username=username,
                    password=password,
                )
                nodes.append(
                    Pool(create_connection, max_size=pool_size, timeout=pool_timeout)
                )
            self._connections = hashring.HashRing(nodes)
        elif isinstance(addr, tuple):
            a: Addr = addr
            create_connection = lambda: Connection(
                a,
                load_func=load_func,
                dump_func=dump_func,
                username=username,
                password=password,
            )
            self._connections = hashring.HashRing(
                [Pool(create_connection, max_size=pool_size, timeout=pool_timeout)]
            )
        else:
            raise TypeError("invalid type for addr")

    @contextmanager
    def _get_connection(self, key: Union[str, bytes]) -> Iterator[Connection]:
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        pool = self._connections.get_node(key)
        with pool.get() as connection:
            yield connection

    def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        with self._get_connection(command.key) as connection:
            return connection.execute_meta_command(command)

    def flush_all(self) -> None:
        for pool in self._connections.nodes:
            with pool.get() as connection:
                connection.flush_all()

    def set(
        self, key: Union[bytes, str], value: Any, *, expire: Optional[int] = None
    ) -> None:
        with self._get_connection(key) as connection:
            return connection.set(key, value, expire=expire)

    def get(self, key: Union[bytes, str]) -> Optional[Any]:
        with self._get_connection(key) as connection:
            return connection.get(key)

    def delete(self, key: Union[bytes, str]) -> None:
        with self._get_connection(key) as connection:
            return connection.delete(key)
