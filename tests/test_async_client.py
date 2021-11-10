import asyncio

import pytest

import memcache


@pytest.fixture()
def client():
    return memcache.AsyncMemcache(("localhost", 11211))


@pytest.mark.asyncio
async def test_execute_command(client):
    command = memcache.MetaCommand(
        cm=b"ms", key=b"foo", datalen=3, flags=[b"T10"], value=b"bar"
    )
    result = await client.execute_meta_command(command)
    assert result.rc == b"HD"

    command = memcache.MetaCommand(cm=b"mg", key=b"foo", flags=[b"v"], value=None)
    result = await client.execute_meta_command(command)
    assert result.rc == b"VA"
    assert result.value == b"bar"


@pytest.mark.asyncio
async def test_set_get(client):
    await client.set("foofoo", ("barbar",), expire=1)
    assert await client.get(b"foofoo") == ("barbar",)
    await asyncio.sleep(1.1)
    assert await client.get(b"foofoo") is None


@pytest.mark.asyncio
async def test_delete(client):
    await client.set(b"to_be_deleted", b"xxxx")
    await client.delete(b"to_be_deleted")
    assert await client.get(b"to_be_deleted") is None


@pytest.mark.asyncio
async def test_flush_all(client):
    await client.flush_all()
    await client.flush_all()
