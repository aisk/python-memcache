import pytest

import memcache


@pytest.fixture()
def client():
    return memcache.Memcache("memcache://localhost:12345")


def test_hello(client):
    command = memcache.MetaCommand(cm=b"ms", key=b"foo", flags=[b"S3"], value=b"bar")
    result = client.execute_meta_command(command)
    assert result.rc == b"OK"

    command = memcache.MetaCommand(cm=b"mg", key=b"foo", flags=[b"v"], value=None)
    result = client.execute_meta_command(command)
    assert result.rc == b"VA"
    assert result.value == b"bar"
    client.connection.close()


def test_flush_all(client):
    client.flush_all()
    client.flush_all()
