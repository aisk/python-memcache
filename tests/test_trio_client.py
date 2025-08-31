import pytest
import trio
import time

import memcache


@pytest.fixture()
def client():
    return memcache.AsyncMemcache(("localhost", 11211))


@pytest.mark.trio
async def test_trio_connection(client):
    """Test basic trio connection functionality."""

    # Test basic set/get operations
    await client.set("trio_test", "trio_value")
    result = await client.get("trio_test")
    assert result == "trio_value"

    # Clean up
    await client.delete("trio_test")


@pytest.mark.trio
async def test_trio_set_get(client):
    """Test trio set and get operations with different data types."""

    # Test string
    await client.set("trio_string", "hello trio")
    assert await client.get("trio_string") == "hello trio"

    # Test integer
    await client.set("trio_int", 42)
    assert await client.get("trio_int") == 42

    # Test list
    await client.set("trio_list", [1, 2, 3])
    assert await client.get("trio_list") == [1, 2, 3]

    # Clean up
    await client.delete("trio_string")
    await client.delete("trio_int")
    await client.delete("trio_list")


@pytest.mark.trio
async def test_trio_expire(client):
    """Test trio operations with expiration."""

    await client.set("trio_expire", "temporary", expire=1)
    assert await client.get("trio_expire") == "temporary"

    # Wait for expiration
    await trio.sleep(1.1)
    assert await client.get("trio_expire") is None


@pytest.mark.trio
async def test_trio_gets_cas(client):
    """Test trio gets and cas operations."""

    await client.set("trio_cas", "initial")

    # Get value with CAS token
    result = await client.gets("trio_cas")
    assert result is not None
    value, cas_token = result
    assert value == "initial"
    assert isinstance(cas_token, int)

    # Successful CAS operation
    await client.cas("trio_cas", "updated", cas_token)
    assert await client.get("trio_cas") == "updated"

    # Clean up
    await client.delete("trio_cas")


@pytest.mark.trio
async def test_trio_flush_all(client):
    """Test trio flush_all operation."""

    # Set some data
    await client.set("trio_flush1", "data1")
    await client.set("trio_flush2", "data2")

    # Flush all
    await client.flush_all()

    # Verify data is gone
    assert await client.get("trio_flush1") is None
    assert await client.get("trio_flush2") is None


@pytest.mark.trio
async def test_trio_execute_command(client):
    """Test trio meta command execution."""
    command = memcache.MetaCommand(
        cm=b"ms", key=b"foo", datalen=3, flags=[b"T10"], value=b"bar"
    )
    result = await client.execute_meta_command(command)
    assert result.rc == b"HD"

    command = memcache.MetaCommand(cm=b"mg", key=b"foo", flags=[b"v"], value=None)
    result = await client.execute_meta_command(command)
    assert result.rc == b"VA"
    assert result.value == b"bar"


@pytest.mark.trio
async def test_trio_gets_missing_key(client):
    """Test trio gets operation with missing key."""
    await client.delete("nonexistent_key")
    assert await client.gets("nonexistent_key") is None


@pytest.mark.trio
async def test_trio_cas_failure(client):
    """Test trio CAS operation failure."""
    await client.set("cas_key", "initial_value")
    _, cas_token = await client.gets("cas_key")

    # Modify the value outside of CAS
    await client.set("cas_key", "modified_value")

    # CAS should fail with old token
    with pytest.raises(memcache.MemcacheError):
        await client.cas("cas_key", "updated_value", cas_token)
    assert await client.get("cas_key") == "modified_value"


@pytest.mark.trio
async def test_trio_incr(client):
    """Test trio increment operation."""
    await client.set("counter", 10)
    result = await client.incr("counter")
    assert result == 11
    assert await client.get("counter") == 11


@pytest.mark.trio
async def test_trio_decr(client):
    """Test trio decrement operation."""
    await client.set("counter", 10)
    result = await client.decr("counter")
    assert result == 9
    assert await client.get("counter") == 9


@pytest.mark.trio
async def test_trio_incr_decr_combined(client):
    """Test combined increment and decrement operations."""
    await client.set("counter", 100)
    
    # Increment multiple times
    assert await client.incr("counter", 10) == 110
    assert await client.incr("counter", 5) == 115
    
    # Decrement multiple times
    assert await client.decr("counter", 3) == 112
    assert await client.decr("counter") == 111
    
    # Final value
    assert await client.get("counter") == 111


@pytest.mark.trio
async def test_trio_incr_missing_key(client):
    """Test increment operation on missing key."""
    await client.delete("nonexistent_counter")
    with pytest.raises(memcache.MemcacheError):
        await client.incr("nonexistent_counter")


@pytest.mark.trio
async def test_trio_decr_missing_key(client):
    """Test decrement operation on missing key."""
    await client.delete("nonexistent_counter")
    with pytest.raises(memcache.MemcacheError):
        await client.decr("nonexistent_counter")
