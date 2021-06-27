from memcache import serialize


def test_serialize():
    result, flags = serialize.dump("test", 1)
    assert flags == serialize.FLAG_INT
    assert 1 == serialize.load("test", result, flags)

    result, flags = serialize.dump("test", b"foo")
    assert flags == serialize.FLAG_BYTES
    assert b"foo" == serialize.load("test", result, flags)

    result, flags = serialize.dump("test", "foo")
    assert flags == serialize.FLAG_STR
    assert "foo" == serialize.load("test", result, flags)

    result, flags = serialize.dump("test", {"foo": "bar"})
    assert flags == serialize.FLAG_PICKLE
    assert {"foo": "bar"} == serialize.load("test", result, flags)
