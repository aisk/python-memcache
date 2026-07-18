import threading

import pytest

from memcache import MetaCommand
from memcache.experiment import (
    AmbiguousWriteError,
    Arithmetic,
    ArithmeticResult,
    Delete,
    Get,
    GetStatus,
    JsonSerializer,
    LeaseState,
    Meta,
    MetaClient,
    MutationStatus,
    PickleSerializer,
    ResultValueError,
    SerializeError,
    Set,
    ValueState,
)
from memcache.errors import MemcacheError, PipelineError


@pytest.fixture()
def client():
    with MetaClient(("localhost", 11211)) as value:
        value.flush_all()
        yield value


@pytest.fixture()
def pickle_client():
    with MetaClient(("localhost", 11211), serializer=PickleSerializer()) as value:
        value.flush_all()
        yield value


def test_sync_client_has_no_background_event_loop_thread():
    before = {thread.ident for thread in threading.enumerate()}
    client = MetaClient(("localhost", 11211))
    try:
        assert {thread.ident for thread in threading.enumerate()} == before
        assert not hasattr(client, "_loop")
        assert not hasattr(client, "_thread")
    finally:
        client.close()


def test_pipeline_error_is_a_memcache_error():
    error = PipelineError(1, [], ConnectionResetError("lost"))
    assert isinstance(error, MemcacheError)
    assert error.written == 1
    assert isinstance(error.cause, ConnectionResetError)


def test_close_is_idempotent_and_rejects_new_work():
    client = MetaClient(("localhost", 11211))
    client.close()
    client.close()
    with pytest.raises(RuntimeError, match="client is closed"):
        client.get("key")


def test_batch_marks_written_side_effects_ambiguous(monkeypatch):
    client = MetaClient(("localhost", 11211))

    def fail(commands, timeout):
        raise PipelineError(2, [], ConnectionResetError("lost"))

    monkeypatch.setattr(client._servers[0], "pipeline", fail)
    results = client.batch([Set("a", "v"), Get("b"), Set("c", "v")])
    assert results[0].status is MutationStatus.AMBIGUOUS
    assert results[1].status is GetStatus.FAILED
    assert results[2].status is MutationStatus.FAILED

    with pytest.raises(AmbiguousWriteError):
        client.set("a", "v")
    client.close()


def test_server_failure_is_isolated_in_batch():
    client = MetaClient([("localhost", 11211), ("localhost", 1)], timeout=0.2)
    good = bad = None
    for number in range(10000):
        key = "sync-shard-%d" % number
        port = client._server_for(key).addr[1]
        if port == 11211 and good is None:
            good = key
        elif port == 1 and bad is None:
            bad = key
        if good is not None and bad is not None:
            break
    assert good is not None and bad is not None

    results = client.batch([Set(good, "ok"), Set(bad, "no"), Get(good)])
    assert results[0].status is MutationStatus.STORED
    assert results[1].status is MutationStatus.FAILED
    assert results[2].status is GetStatus.HIT
    assert results[2].value == "ok"
    client.close()


def test_explicit_get_states_and_values(client):
    assert client.get("missing").status is GetStatus.MISS
    with pytest.raises(ResultValueError):
        client.get("missing").value

    client.set("empty", b"")
    assert client.get("empty").value == b""
    assert client.get("missing").value_or("fallback") == "fallback"


def test_default_serializer_is_strict(client, pickle_client):
    with pytest.raises(TypeError):
        client.set("obj", {"a": 1})
    with pytest.raises(TypeError):
        client.set("none", None)
    with pytest.raises(TypeError):
        client.set("flag", True)

    # Refusing to unpickle matters more than refusing to pickle: loads is
    # the code-execution side. Reads of foreign pickled data must fail.
    pickle_client.set("pickled", {"a": 1})
    result = client.get("pickled")
    assert result.status is GetStatus.FAILED
    assert isinstance(result.error, SerializeError)


def test_pickle_serializer_round_trips_objects(pickle_client):
    pickle_client.set("none", None)
    none = pickle_client.get("none")
    assert none.status is GetStatus.HIT
    assert none.value is None
    assert bool(none)

    pickle_client.set("obj", {"a": 1})
    assert pickle_client.get("obj").value == {"a": 1}

    pickle_client.set("flag", True)
    assert pickle_client.get("flag").value is True


def test_json_serializer_round_trips_objects():
    with MetaClient(("localhost", 11211), serializer=JsonSerializer()) as client:
        client.set("doc", {"a": [1, None, True]})
        assert client.get("doc").value == {"a": [1, None, True]}
        # Primitive flags are shared across serializers.
        client.set("n", 7)
        assert client.get("n").value == 7


def test_metadata_inspect_and_conditional_read(client):
    stored = client.set("article", "body", ttl=60, return_cas=True)
    assert stored.status is MutationStatus.STORED
    assert stored.cas is not None

    result = client.get("article", meta=Meta.CAS | Meta.TTL | Meta.SIZE)
    assert result.item.cas == stored.cas
    assert result.item.ttl is not None and result.item.ttl > 0
    assert result.item.size == 4

    unchanged = client.get("article", unless_cas=stored.cas)
    assert unchanged.status is GetStatus.UNCHANGED
    assert unchanged.item.cas == stored.cas
    with pytest.raises(ResultValueError):
        unchanged.value

    changed = client.get("article", unless_cas=stored.cas + 1)
    assert changed.status is GetStatus.HIT
    assert changed.value == "body"
    assert changed.item.cas == stored.cas

    metadata = client.inspect("article")
    assert metadata.status is GetStatus.HIT
    assert metadata.item.size == 4
    with pytest.raises(ResultValueError):
        metadata.value


def test_store_modes_versions_and_conveniences(client):
    assert client.set("condition", "first", mode="add")
    assert client.add("condition", "second").status is MutationStatus.ALREADY_EXISTS
    assert client.set("absent", "x", mode="replace").status is MutationStatus.NOT_FOUND

    old = client.get("condition", meta=Meta.CAS)
    assert (
        client.cas("condition", "second", old.item.cas).status is MutationStatus.STORED
    )
    assert (
        client.cas("condition", "third", old.item.cas).status
        is MutationStatus.CAS_MISMATCH
    )

    versioned = client.set("versioned", "v", version=42, return_cas=True)
    assert versioned.cas == 42
    assert client.get("versioned", meta=Meta.CAS).item.cas == 42


def test_batch_is_ordered_pipeline_and_keeps_duplicates(client):
    client.set("a", "A")
    results = client.batch(
        [
            Get("a", meta=Meta.CAS),
            Get(b"missing"),
            Set("b", b"B"),
            Delete("absent"),
            Get("a"),
            Arithmetic("counter", initial=10, initial_ttl=60),
        ]
    )
    assert len(results) == 6
    assert results[0].key == "a" and results[0].value == "A"
    assert results[1].key == b"missing" and results[1].status is GetStatus.MISS
    assert results[2].status is MutationStatus.STORED
    assert results[3].status is MutationStatus.NOT_FOUND
    assert results[4].value == "A"
    assert isinstance(results[5], ArithmeticResult)
    assert results[5].value == 10

    many = client.batch([Get("a"), Get(b"missing"), Get("a")])
    assert [item.key for item in many] == ["a", b"missing", "a"]
    assert [item.status for item in many] == [
        GetStatus.HIT,
        GetStatus.MISS,
        GetStatus.HIT,
    ]


def test_lease_miss_pending_and_fulfill(client):
    winner = client.get_with_lease("report", lease_ttl=30)
    assert (winner.value_state, winner.lease_state) == (
        ValueState.MISSING,
        LeaseState.GRANTED,
    )
    assert winner.status is GetStatus.MISS

    observer = client.get("report")
    assert observer.status is GetStatus.PENDING
    assert observer.lease_state is LeaseState.BUSY

    loser = client.get_with_lease("report", lease_ttl=30)
    assert loser.value_state is ValueState.MISSING
    assert loser.lease_state is LeaseState.BUSY

    assert winner.fulfill("ready", ttl=60).status is MutationStatus.STORED
    assert client.get("report").value == "ready"


def test_lease_early_recache_stale_and_all_fulfill_outcomes(client):
    client.set("early", "value", ttl=30)
    first = client.get_with_lease("early", lease_ttl=20, refresh_before=60)
    second = client.get_with_lease("early", lease_ttl=20, refresh_before=60)
    assert first.value == second.value == "value"
    assert first.value_state is ValueState.FRESH
    assert first.lease_state is LeaseState.GRANTED
    assert second.lease_state is LeaseState.BUSY

    client.set("stale", "old", ttl=60)
    client.invalidate("stale", stale_for=60)
    stale = client.get_with_lease("stale", lease_ttl=20)
    assert stale.value == "old"
    assert stale.value_state is ValueState.STALE
    assert stale.lease_state is LeaseState.GRANTED

    client.set("stale", "new")
    assert stale.fulfill("rebuilt").status is MutationStatus.CAS_MISMATCH

    expired = client.get_with_lease("gone", lease_ttl=20)
    client.delete("gone")
    assert expired.fulfill("late").status is MutationStatus.NOT_FOUND


def test_byte_concatenation_arithmetic_touch_and_delete(client):
    client.set("bytes", b"middle")
    assert client.append("bytes", b" end")
    assert client.prepend("bytes", b"start ")
    assert client.get("bytes").value == b"start middle end"
    with pytest.raises(TypeError):
        client.append("bytes", "not bytes")

    assert client.increment("n", initial=5, initial_ttl=60).value == 5
    assert client.increment("n", 3).value == 8
    assert client.decrement("n", 20).value == 0
    counted = client.increment("n", 1, return_ttl=True)
    assert counted.value == 1
    assert counted.item.ttl is not None and counted.item.ttl > 0
    assert client.touch("n", 60).status is MutationStatus.STORED
    assert client.touch("no", 60).status is MutationStatus.NOT_FOUND

    current = client.get("n", meta=Meta.CAS)
    assert (
        client.delete("n", compare_cas=current.item.cas).status is MutationStatus.STORED
    )
    assert client.delete("n").status is MutationStatus.NOT_FOUND


def test_binary_key_and_bytes_escape_hatch(client):
    key = b"binary key\x00"
    client.set(key, "value")
    assert client.get(key).key == key
    assert client.get(key).value == "value"

    raw = client.meta.execute(command="mg", key=key, flags=[b"v"])
    assert raw.rc == b"VA"
    assert raw.value == b"value"

    raw_batch = client.meta.batch(
        [MetaCommand(b"mg", key, flags=[b"v"]), MetaCommand(b"mg", b"none")]
    )
    assert [item.rc for item in raw_batch] == [b"VA", b"EN"]


def test_meta_namespace_get_set_flags(client):
    stored = client.meta.set(
        "wire",
        b"payload",
        ttl=60,
        client_flags=7,
        return_cas=True,
        return_size=True,
        return_key=True,
        opaque=b"tok1",
    )
    assert stored.rc == b"HD" and stored.ok and bool(stored)
    assert stored.cas is not None
    assert stored.size == len(b"payload")
    assert stored.key == b"wire"
    assert stored.opaque == b"tok1"

    got = client.meta.get(
        "wire",
        return_cas=True,
        return_ttl=True,
        return_size=True,
        return_client_flags=True,
        return_key=True,
        opaque=17,
    )
    assert got.rc == b"VA"
    assert got.value == b"payload"
    assert got.client_flags == 7
    assert got.cas == stored.cas
    assert got.ttl is not None and 0 < got.ttl <= 60
    assert got.key == b"wire"
    assert got.opaque == b"17"

    unchanged = client.meta.get("wire", unless_cas=stored.cas)
    assert unchanged.rc == b"HD" and unchanged.value is None

    header_only = client.meta.get("wire", value=False, return_size=True)
    assert header_only.rc == b"HD"
    assert header_only.size == len(b"payload")

    miss = client.meta.get("absent")
    assert miss.rc == b"EN" and not miss.ok and not miss

    binary = b"binary key\x00"
    client.meta.set(binary, b"x")
    assert client.meta.get(binary, return_key=True).key == binary


def test_meta_namespace_store_modes_and_cas(client):
    assert client.meta.set("m", b"1", mode="add").rc == b"HD"
    assert client.meta.set("m", b"0", mode="add").rc == b"NS"
    assert client.meta.set("m", b"23", mode="append").rc == b"HD"
    assert client.meta.set("m", b"0", mode="prepend").rc == b"HD"
    assert client.meta.get("m").value == b"0123"
    assert client.meta.set("missing", b"x", mode="replace").rc == b"NS"

    cas = client.meta.get("m", value=False, return_cas=True).cas
    assert client.meta.set("m", b"x", compare_cas=cas + 1).rc == b"EX"
    assert client.meta.set("m", b"x", compare_cas=cas, new_cas=99).rc == b"HD"
    assert client.meta.get("m", value=False, return_cas=True).cas == 99

    with pytest.raises(ValueError):
        client.meta.set("m", b"x", mode="upsert")
    with pytest.raises(TypeError):
        client.meta.set("m", "not bytes")
    with pytest.raises(ValueError):
        client.meta.set("m", b"x", vivify_ttl=60)


def test_meta_namespace_delete_invalidate_and_tombstone(client):
    client.meta.set("gone", b"x")
    assert client.meta.delete("gone").rc == b"HD"
    assert client.meta.delete("gone").rc == b"NF"

    client.meta.set("stale", b"old", ttl=60)
    assert client.meta.delete("stale", invalidate=True, ttl=30).rc == b"HD"
    revalidate = client.meta.get("stale")
    assert revalidate.value == b"old"
    assert revalidate.stale and revalidate.won

    client.meta.set("husk", b"body")
    assert client.meta.delete("husk", drop_value=True).rc == b"HD"
    emptied = client.meta.get("husk", return_size=True)
    assert emptied.rc == b"VA"
    assert emptied.value == b"" and emptied.size == 0

    with pytest.raises(ValueError):
        client.meta.delete("stale", ttl=30)


def test_meta_namespace_arithmetic_and_debug(client):
    created = client.meta.arithmetic("counter", initial=5, initial_ttl=60)
    assert created.rc == b"VA" and created.value == b"5"
    assert client.meta.arithmetic("counter", delta=3).value == b"8"
    grown = client.meta.arithmetic(
        "counter", delta=20, decrement=True, return_ttl=True, return_cas=True
    )
    assert grown.value == b"0"
    assert grown.ttl is not None and grown.cas is not None
    assert client.meta.arithmetic("missing").rc == b"NF"
    with pytest.raises(ValueError):
        client.meta.arithmetic("counter", initial=1)

    info = client.meta.debug("counter")
    assert info is not None
    assert "size" in info and "cas" in info
    assert client.meta.debug("missing") is None
    # A digit-leading key must not be confused with a VA datalen token.
    client.meta.set("123digits", b"x")
    assert client.meta.debug("123digits") is not None
    # memcached ignores the b flag on me, which would turn a binary-key
    # debug into a silent miss; the client must refuse loudly instead.
    with pytest.raises(ValueError):
        client.meta.debug(b"binary key\x00")


def test_meta_namespace_opaque_validation(client):
    with pytest.raises(ValueError):
        client.meta.get("k", opaque=b"has space")
    with pytest.raises(ValueError):
        client.meta.get("k", opaque=b"x" * 33)
    with pytest.raises(TypeError):
        client.meta.get("k", opaque=1.5)


def test_programming_errors_are_raised_before_batch(client):
    with pytest.raises(ValueError):
        client.batch([Get("a", refresh_before=10)])
    with pytest.raises(ValueError):
        client.batch([Arithmetic("n", initial=1)])
    with pytest.raises(ValueError):
        client.batch([Delete("a", stale_for=10)])
    with pytest.raises(ValueError):
        client.set("a", "v", mode="upsert")
    # Add only stores when no item exists, so there is no CAS to compare.
    with pytest.raises(ValueError):
        client.set("a", "v", mode="add", compare_cas=1)
    # The server silently ignores T for append/prepend; the miss path takes
    # its TTL from vivify_ttl instead.
    with pytest.raises(ValueError):
        client.batch([Set("a", b"v", mode="append", ttl=5)])
    # Concatenation happens on raw bytes; serialized values would corrupt
    # the stored item, so non-bytes payloads are rejected on any path.
    with pytest.raises(TypeError):
        client.batch([Set("a", "not bytes", mode="prepend")])
