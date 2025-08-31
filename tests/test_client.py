import queue
import threading
import time

import pytest

import memcache


@pytest.fixture()
def client():
    return memcache.Memcache(("localhost", 11211))


def test_execute_command(client):
    command = memcache.MetaCommand(
        cm=b"ms", key=b"foo", datalen=3, flags=[b"T10"], value=b"bar"
    )
    result = client.execute_meta_command(command)
    assert result.rc == b"HD"

    command = memcache.MetaCommand(cm=b"mg", key=b"foo", flags=[b"v"], value=None)
    result = client.execute_meta_command(command)
    assert result.rc == b"VA"
    assert result.value == b"bar"


def test_set_get(client):
    client.set("foofoo", "barbar", expire=1)
    assert client.get(b"foofoo") == "barbar"
    time.sleep(1.1)
    assert client.get("foofoo") is None


def test_delete(client):
    client.set(b"to_be_deleted", b"xxxx")
    client.delete(b"to_be_deleted")
    assert client.get(b"to_be_deleted") is None


def test_flush_all(client):
    client.flush_all()
    client.flush_all()


def test_pool_timeout():
    client = memcache.Memcache(pool_size=1, pool_timeout=1)

    start = time.time()
    with client._get_connection("test"):
        try:
            with client._get_connection("test"):
                pass
        except queue.Empty:
            assert time.time() - start > 1
        else:
            raise ValueError("empty not raised")


def test_gets(client):
    client.set("test_key", "test_value")
    result = client.gets("test_key")
    assert result is not None
    value, cas_token = result
    assert value == "test_value"
    assert isinstance(cas_token, int)
    assert cas_token > 0


def test_gets_missing_key(client):
    client.delete("nonexistent_key")
    assert client.gets("nonexistent_key") is None


def test_cas_success(client):
    client.set("cas_key", "initial_value")
    _, cas_token = client.gets("cas_key")

    client.cas("cas_key", "updated_value", cas_token)
    assert client.get("cas_key") == "updated_value"


def test_cas_failure(client):
    client.set("cas_key", "initial_value")
    _, cas_token = client.gets("cas_key")

    # Modify the value outside of CAS
    client.set("cas_key", "modified_value")

    # CAS should fail with old token
    with pytest.raises(memcache.MemcacheError):
        client.cas("cas_key", "updated_value", cas_token)
    assert client.get("cas_key") == "modified_value"


def test_cas_with_expire(client):
    client.set("cas_expire_key", "initial_value")
    _, cas_token = client.gets("cas_expire_key")

    client.cas("cas_expire_key", "updated_value", cas_token, expire=1)
    assert client.get("cas_expire_key") == "updated_value"

    time.sleep(1.1)
    assert client.get("cas_expire_key") is None


def test_incr(client):
    client.set("counter", 10)
    result = client.incr("counter")
    assert result == 11
    assert client.get("counter") == 11


def test_incr_with_value(client):
    client.set("counter", 5)
    result = client.incr("counter", 3)
    assert result == 8
    assert client.get("counter") == 8


def test_decr(client):
    client.set("counter", 10)
    result = client.decr("counter")
    assert result == 9
    assert client.get("counter") == 9


def test_decr_with_value(client):
    client.set("counter", 10)
    result = client.decr("counter", 3)
    assert result == 7
    assert client.get("counter") == 7


def test_incr_decr_combined(client):
    client.set("counter", 100)
    
    # Increment multiple times
    assert client.incr("counter", 10) == 110
    assert client.incr("counter", 5) == 115
    
    # Decrement multiple times
    assert client.decr("counter", 3) == 112
    assert client.decr("counter") == 111
    
    # Final value
    assert client.get("counter") == 111


def test_incr_missing_key(client):
    client.delete("nonexistent_counter")
    with pytest.raises(memcache.MemcacheError):
        client.incr("nonexistent_counter")


def test_decr_missing_key(client):
    client.delete("nonexistent_counter")
    with pytest.raises(memcache.MemcacheError):
        client.decr("nonexistent_counter")
