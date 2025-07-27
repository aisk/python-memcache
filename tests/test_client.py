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
