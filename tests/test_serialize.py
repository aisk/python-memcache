import pytest

from memcache import serialize
from memcache.errors import SerializeError
from memcache.serialize import JsonSerializer, PickleSerializer, StrictSerializer


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


def test_primitive_fast_path_is_shared_across_serializers():
    for serializer in (StrictSerializer(), PickleSerializer(), JsonSerializer()):
        assert serializer.dump("k", b"raw") == (b"raw", serialize.FLAG_BYTES)
        assert serializer.dump("k", 7) == (b"7", serialize.FLAG_INT)
        assert serializer.dump("k", "s") == (b"s", serialize.FLAG_STR)
        assert serializer.load("k", b"7", serialize.FLAG_INT) == 7
        assert serializer.load("k", b"s", serialize.FLAG_STR) == "s"
        assert serializer.load("k", b"raw", serialize.FLAG_BYTES) == b"raw"


def test_strict_serializer_rejects_objects_and_foreign_flags():
    strict = StrictSerializer()
    with pytest.raises(TypeError):
        strict.dump("k", {"a": 1})
    with pytest.raises(TypeError):
        strict.dump("k", None)
    with pytest.raises(TypeError):
        strict.dump("k", True)

    pickled, flags = PickleSerializer().dump("k", {"a": 1})
    with pytest.raises(SerializeError):
        strict.load("k", pickled, flags)
    with pytest.raises(SerializeError):
        strict.load("k", b"{}", serialize.FLAG_JSON)
    with pytest.raises(SerializeError):
        strict.load("k", b"x", 1 << 10)


def test_pickle_serializer_round_trips_objects_and_bool():
    pickler = PickleSerializer()
    raw, flags = pickler.dump("k", {"a": 1})
    assert flags == serialize.FLAG_PICKLE
    assert pickler.load("k", raw, flags) == {"a": 1}

    raw, flags = pickler.dump("k", True)
    assert flags == serialize.FLAG_PICKLE
    assert pickler.load("k", raw, flags) is True

    with pytest.raises(SerializeError):
        pickler.load("k", b"{}", serialize.FLAG_JSON)


def test_json_serializer_round_trips_objects():
    jsonner = JsonSerializer()
    raw, flags = jsonner.dump("k", {"a": [1, None, True]})
    assert flags == serialize.FLAG_JSON
    assert jsonner.load("k", raw, flags) == {"a": [1, None, True]}

    with pytest.raises(SerializeError):
        jsonner.load("k", raw, serialize.FLAG_PICKLE)
    with pytest.raises(TypeError):
        jsonner.dump("k", object())


def test_legacy_dump_preserves_bool():
    raw, flags = serialize.dump("k", True)
    assert serialize.load("k", raw, flags) is True
