import asyncio
from typing import List, Tuple, Union, Optional

from .memcache import Addr, MemcacheError, MetaCommand, MetaResult


class AsyncConnection:
    def __init__(self, addr: Union[Tuple[str, int]]):
        self._addr = addr
        self._connected = False

    async def _connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(
            self._addr[0], self._addr[1]
        )
        self._connected = True

    async def flush_all(self) -> None:
        if not self._connected:
            await self._connect()

        self.writer.write(b"flush_all\r\n")
        await self.writer.drain()
        response = await self.reader.readline()
        if response != b"OK\r\n":
            raise MemcacheError(response.removesuffix(b"\r\n"))

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        if not self._connected:
            await self._connect()

        header = b" ".join([command.cm, command.key] + command.flags + [b"\r\n"])
        self.writer.write(header)
        if command.value:
            self.writer.write(command.value + b"\r\n")
        await self.writer.drain()
        return await self._receive_meta_result()

    async def _receive_meta_result(self) -> MetaResult:
        header = await self.reader.readline()
        parts = header.split()
        rc = parts[0]
        flags = parts[1:]
        value = None
        if rc == b"VA":
            size = int(parts[1])
            flags = parts[2:]
            value = await self.reader.read(size)
            await self.reader.read(2)  # read the "\r\n"
        return MetaResult(rc=rc, flags=flags, value=value)

    async def set(
        self, key: Union[bytes, str], value: bytes, expire: Optional[int] = None
    ) -> None:
        flags = [b"S%d" % len(value)]
        if expire:
            flags.append(b"T%d" % expire)

        command = MetaCommand(cm=b"ms", key=key, flags=flags, value=value)
        await self.execute_meta_command(command)

    async def get(self, key: Union[bytes, str]) -> Optional[bytes]:
        command = MetaCommand(cm=b"mg", key=key, flags=[b"v"], value=None)
        return (await self.execute_meta_command(command)).value

    async def delete(self, key: Union[bytes, str]) -> None:
        command = MetaCommand(cm=b"md", key=key, flags=[], value=None)
        await self.execute_meta_command(command)


class AsyncMemcache:
    def __init__(self, addr: Union[Addr, List[Addr]] = None):
        addr = addr or ("localhost", 11211)
        if isinstance(addr, list):
            self.connections = [AsyncConnection(x) for x in addr]
        else:
            self.connections = [AsyncConnection(addr)]

    def _get_connection(self, key) -> AsyncConnection:
        return self.connections[hash(key) % len(self.connections)]

    async def execute_meta_command(self, command: MetaCommand) -> MetaResult:
        return await self._get_connection(command.key).execute_meta_command(command)

    async def flush_all(self) -> None:
        for connection in self.connections:
            await connection.flush_all()

    async def set(
        self, key: Union[bytes, str], value: bytes, *, expire: Optional[int] = None
    ) -> None:
        return await self._get_connection(key).set(key, value, expire=expire)

    async def get(self, key: Union[bytes, str]) -> Optional[bytes]:
        return await self._get_connection(key).get(key)

    async def delete(self, key: Union[bytes, str]) -> None:
        return await self._get_connection(key).delete(key)
