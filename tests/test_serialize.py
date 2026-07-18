import bz2
import gzip
import lzma
import os
import sys
import zlib

import pytest

if sys.version_info >= (3, 14):
    from compression import zstd
else:
    from backports import zstd

from memcache import serialize
from memcache.errors import SerializeError
from memcache.serialize import (
    CompressedSerializer,
    JsonSerializer,
    PickleSerializer,
    StrictSerializer,
)


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


def test_compressed_serializer_round_trips_large_values():
    compressed = CompressedSerializer(PickleSerializer(), min_size=64)
    value = {"body": "x" * 500}
    raw, flags = compressed.dump("k", value)
    assert flags == serialize.FLAG_PICKLE | serialize.FLAG_COMPRESSED
    assert raw.startswith(b"\x28\xb5\x2f\xfd")  # zstd frame magic
    assert len(raw) < 500
    assert compressed.load("k", raw, flags) == value

    raw, flags = compressed.dump("k", "y" * 500)
    assert flags == serialize.FLAG_STR | serialize.FLAG_COMPRESSED
    assert compressed.load("k", raw, flags) == "y" * 500


def test_compressed_serializer_skips_small_int_and_incompressible():
    compressed = CompressedSerializer(StrictSerializer(), min_size=64)
    assert compressed.dump("k", "tiny") == (b"tiny", serialize.FLAG_STR)

    # Arithmetic commands parse stored bytes directly; ints must stay raw
    # even when their decimal form exceeds min_size.
    big_int = int("9" * 200)
    assert compressed.dump("k", big_int) == (b"9" * 200, serialize.FLAG_INT)

    noise = os.urandom(512)
    assert compressed.dump("k", noise) == (noise, serialize.FLAG_BYTES)


def test_compressed_serializer_sniffs_known_formats():
    compressed = CompressedSerializer(StrictSerializer(), min_size=64)
    payload = b"payload " * 100
    flags = serialize.FLAG_BYTES | serialize.FLAG_COMPRESSED
    for packed in (
        zstd.compress(payload),
        zlib.compress(payload),
        gzip.compress(payload),
        bz2.compress(payload),
        lzma.compress(payload),
    ):
        assert compressed.load("k", packed, flags) == payload


def test_compressed_serializer_rejects_bad_payloads():
    compressed = CompressedSerializer(StrictSerializer(), min_size=64)
    flags = serialize.FLAG_BYTES | serialize.FLAG_COMPRESSED
    with pytest.raises(SerializeError):
        compressed.load("k", b"\x00 no recognizable header", flags)
    with pytest.raises(SerializeError):
        compressed.load("k", b"\x78\x9c corrupt zlib body", flags)
    with pytest.raises(SerializeError):
        compressed.load("k", b"\x1f\x8b corrupt gzip body", flags)

    # A serializer without the wrapper must refuse the compressed bit
    # rather than hand back undecoded bytes.
    packed, packed_flags = compressed.dump("k", b"z" * 500)
    with pytest.raises(SerializeError):
        StrictSerializer().load("k", packed, packed_flags)

    with pytest.raises(ValueError):
        CompressedSerializer(StrictSerializer(), min_size=0)
