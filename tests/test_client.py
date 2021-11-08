import time

import pytest

import memcache


@pytest.fixture()
def client():
    return memcache.Memcache(("localhost", 11211))


def test_execute_command(client):
    command = memcache.MetaCommand(cm=b"ms", key=b"foo", datalen=3, flags=[b"T10"],
                                   value=b"bar")
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
