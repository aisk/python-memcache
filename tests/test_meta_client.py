import threading

import pytest

from memcache import MetaCommand
from memcache.experiment import (
    ABSENT,
    AmbiguousWriteError,
    ArithmeticResult,
    Delete,
    Get,
    GetStatus,
    IfCas,
    Increment,
    LeaseState,
    Meta,
    MetaClient,
    MutationStatus,
    ResultValueError,
    Set,
    ValueState,
)
from memcache.errors import MemcacheError, PipelineError


@pytest.fixture()
def client():
    with MetaClient(("localhost", 11211)) as value:
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

    client.set("none", None)
    none = client.get("none")
    assert none.status is GetStatus.HIT
    assert none.value is None
    assert bool(none)

    client.set("empty", b"")
    assert client.get("empty").value == b""
    assert client.get("missing").value_or("fallback") == "fallback"


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


def test_conditions_versions_and_conveniences(client):
    assert client.set("condition", "first", condition=ABSENT)
    assert client.add("condition", "second").status is MutationStatus.ALREADY_EXISTS
    assert client.replace("absent", "x").status is MutationStatus.NOT_FOUND

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
            Increment("counter", initial=10, initial_ttl=60),
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

    many = client.get_many(["a", b"missing", "a"])
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

    assert winner.fulfill({"ready": True}, ttl=60).status is MutationStatus.STORED
    assert client.get("report").value == {"ready": True}


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
    assert client.append_bytes("bytes", b" end")
    assert client.prepend_bytes("bytes", b"start ")
    assert client.get("bytes").value == b"start middle end"
    with pytest.raises(TypeError):
        client.append_bytes("bytes", "not bytes")

    assert client.increment("n", initial=5, initial_ttl=60).value == 5
    assert client.increment("n", 3).value == 8
    assert client.decrement("n", 20).value == 0
    assert client.touch("n", 60).status is MutationStatus.STORED
    assert client.touch("no", 60).status is MutationStatus.NOT_FOUND

    current = client.get("n", meta=Meta.CAS)
    assert (
        client.delete("n", condition=IfCas(current.item.cas)).status
        is MutationStatus.STORED
    )
    assert client.delete("n").status is MutationStatus.NOT_FOUND


def test_binary_key_and_raw_escape_hatch(client):
    key = b"binary key\x00"
    client.set(key, "value")
    assert client.get(key).key == key
    assert client.get(key).value == "value"

    raw = client.raw.execute(command="mg", key=key, flags=[b"v"])
    assert raw.rc == b"VA"
    assert raw.value == b"value"

    raw_batch = client.raw.batch(
        [MetaCommand(b"mg", key, flags=[b"v"]), MetaCommand(b"mg", b"none")]
    )
    assert [item.rc for item in raw_batch] == [b"VA", b"EN"]


def test_programming_errors_are_raised_before_batch(client):
    with pytest.raises(ValueError):
        client.batch([Get("a", refresh_before=10)])
    with pytest.raises(ValueError):
        client.batch([Increment("n", initial=1)])
    with pytest.raises(ValueError):
        client.batch([Delete("a", stale_for=10)])
    with pytest.raises(TypeError):
        client.set("a", "v", condition=object())
