import asyncio
import time

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


@pytest.mark.asyncio
async def test_pool_timeout():
    client = memcache.AsyncMemcache(pool_size=1, pool_timeout=1)

    start = time.time()
    async with client._get_connection("test"):
        try:
            async with client._get_connection("test"):
                pass
        except asyncio.TimeoutError:
            assert time.time() - start > 1
        else:
            raise ValueError("empty not raised")


@pytest.mark.asyncio
async def test_gets(client):
    await client.set("test_key", "test_value")
    result = await client.gets("test_key")
    assert result is not None
    value, cas_token = result
    assert value == "test_value"
    assert isinstance(cas_token, int)
    assert cas_token > 0


@pytest.mark.asyncio
async def test_gets_missing_key(client):
    await client.delete("nonexistent_key")
    assert await client.gets("nonexistent_key") is None


@pytest.mark.asyncio
async def test_cas_success(client):
    await client.set("cas_key", "initial_value")
    _, cas_token = await client.gets("cas_key")

    await client.cas("cas_key", "updated_value", cas_token)
    assert await client.get("cas_key") == "updated_value"


@pytest.mark.asyncio
async def test_cas_failure(client):
    await client.set("cas_key", "initial_value")
    _, cas_token = await client.gets("cas_key")

    # Modify the value outside of CAS
    await client.set("cas_key", "modified_value")

    # CAS should fail with old token
    with pytest.raises(memcache.MemcacheError):
        await client.cas("cas_key", "updated_value", cas_token)
    assert await client.get("cas_key") == "modified_value"


@pytest.mark.asyncio
async def test_cas_with_expire(client):
    await client.set("cas_expire_key", "initial_value")
    _, cas_token = await client.gets("cas_expire_key")

    await client.cas("cas_expire_key", "updated_value", cas_token, expire=1)
    assert await client.get("cas_expire_key") == "updated_value"

    await asyncio.sleep(1.1)
    assert await client.get("cas_expire_key") is None


@pytest.mark.asyncio
async def test_incr(client):
    await client.set("counter", 10)
    result = await client.incr("counter")
    assert result == 11
    assert await client.get("counter") == 11


@pytest.mark.asyncio
async def test_incr_with_value(client):
    await client.set("counter", 5)
    result = await client.incr("counter", 3)
    assert result == 8
    assert await client.get("counter") == 8


@pytest.mark.asyncio
async def test_decr(client):
    await client.set("counter", 10)
    result = await client.decr("counter")
    assert result == 9
    assert await client.get("counter") == 9


@pytest.mark.asyncio
async def test_decr_with_value(client):
    await client.set("counter", 10)
    result = await client.decr("counter", 3)
    assert result == 7
    assert await client.get("counter") == 7


@pytest.mark.asyncio
async def test_incr_decr_combined(client):
    await client.set("counter", 100)
    
    # Increment multiple times
    assert await client.incr("counter", 10) == 110
    assert await client.incr("counter", 5) == 115
    
    # Decrement multiple times
    assert await client.decr("counter", 3) == 112
    assert await client.decr("counter") == 111
    
    # Final value
    assert await client.get("counter") == 111


@pytest.mark.asyncio
async def test_incr_missing_key(client):
    await client.delete("nonexistent_counter")
    with pytest.raises(memcache.MemcacheError):
        await client.incr("nonexistent_counter")


@pytest.mark.asyncio
async def test_decr_missing_key(client):
    await client.delete("nonexistent_counter")
    with pytest.raises(memcache.MemcacheError):
        await client.decr("nonexistent_counter")
