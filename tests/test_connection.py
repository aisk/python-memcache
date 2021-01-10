import memcache


def test_hello():
    conn = memcache.Connection("localhost", 12345)
    command = memcache.MetaCommand(cm=b"ms", key=b"foo", flags=[b"S3"], value=b"bar")
    result = conn.send_meta_command(command)
    assert result.rc ==b"OK"
    conn.close()
