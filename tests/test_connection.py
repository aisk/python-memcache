import pytest

import memcache


@pytest.fixture()
def connection():
    return memcache.Connection("localhost", 12345)


def test_hello(connection):
    command = memcache.MetaCommand(cm=b"ms", key=b"foo", flags=[b"S3"], value=b"bar")
    result = connection.execute_meta_command(command)
    assert result.rc == b"OK"

    command = memcache.MetaCommand(cm=b"mg", key=b"foo", flags=[b"v"], value=None)
    result = connection.execute_meta_command(command)
    assert result.rc == b"VA"
    assert result.value == b"bar"
    connection.close()


def test_flush_all(connection):
    connection.flush_all()
    connection.flush_all()
