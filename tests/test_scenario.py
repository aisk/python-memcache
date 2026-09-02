import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from memcache.errors import PipelineError
from memcache.experiment import (
    FOREVER,
    AmbiguousWriteError,
    ConflictError,
    ItemInfo,
    JsonSerializer,
    Memcache,
    NotFoundError,
    OperationFailedError,
    PickleSerializer,
    SerializeError,
)

ADDR = ("localhost", 11211)
DEAD_ADDR = ("localhost", 1)


@pytest.fixture()
def cache():
    with Memcache(ADDR, serializer=PickleSerializer()) as client:
        client.flush_all()
        yield client


@pytest.fixture()
def strict_cache():
    with Memcache(ADDR) as client:
        client.flush_all()
        yield client


@pytest.fixture()
def dead_cache():
    failures: list[BaseException] = []
    with Memcache(
        DEAD_ADDR,
        serializer=PickleSerializer(),
        on_error="degrade",
        on_failure=failures.append,
        timeout=0.2,
    ) as client:
        client.failures = failures  # type: ignore[attr-defined]
        yield client


# ----------------------------------------------------------------------
# S1: object cache


def test_set_get_delete_roundtrip(cache):
    cache.set("user:1", {"name": "an"}, ttl=600)
    assert cache.get("user:1") == {"name": "an"}
    assert cache.delete("user:1") is True
    assert cache.get("user:1") is None
    assert cache.delete("user:1") is False


def test_get_miss_returns_default(cache):
    assert cache.get("missing") is None
    assert cache.get("missing", default="fallback") == "fallback"


def test_bytes_and_str_keys_name_the_same_item(cache):
    cache.set("twin", 1, ttl=60)
    assert cache.get(b"twin") == 1


def test_set_forever(cache):
    cache.set("eternal", "v", ttl=FOREVER)
    assert cache.inspect("eternal").ttl == -1


def test_ttl_validation():
    with Memcache(ADDR) as client:
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=-1)
        with pytest.raises(TypeError):
            client.set("k", "v", ttl=None)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            client.set("k", "v", ttl=1.5)  # type: ignore[arg-type]


def test_ttl_accepts_timedelta(cache):
    cache.set("lasting", "v", ttl=timedelta(minutes=10))
    assert 0 < cache.inspect("lasting").ttl <= 600


def test_ttl_subsecond_timedelta_rounds_up_not_forever(cache):
    cache.set("blink", "v", ttl=timedelta(milliseconds=500))
    assert cache.inspect("blink").ttl != -1


def test_ttl_rejects_negative_timedelta():
    with Memcache(ADDR) as client:
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=timedelta(seconds=-1))
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=timedelta(milliseconds=-500))


def test_ttl_accepts_aware_datetime(cache):
    cache.set("dated", "v", ttl=datetime.now(timezone.utc) + timedelta(minutes=5))
    # The absolute moment is rounded up to a whole second, so the remaining
    # lifetime may read one second past the requested duration.
    assert 0 < cache.inspect("dated").ttl <= 301


def test_ttl_rejects_naive_and_past_datetime():
    with Memcache(ADDR) as client:
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=datetime.now() + timedelta(minutes=5))
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=datetime.now(timezone.utc) - timedelta(minutes=5))


def test_zero_byte_values_are_rejected(cache):
    with pytest.raises(SerializeError):
        cache.set("empty", b"", ttl=60)
    with pytest.raises(SerializeError):
        cache.set("empty", "", ttl=60)


def test_strict_serializer_rejects_objects(strict_cache):
    with pytest.raises(TypeError):
        strict_cache.set("obj", {"a": 1}, ttl=60)


# ----------------------------------------------------------------------
# get's parameter cluster constraints


def test_bare_get_rejects_factory_only_parameters(cache):
    with pytest.raises(TypeError):
        cache.get("k", ttl=60)
    with pytest.raises(TypeError):
        cache.get("k", refresh_ahead=10)


def test_factory_requires_ttl(cache):
    with pytest.raises(TypeError):
        cache.get("k", factory=lambda: 1)


def test_factory_rejects_extend_ttl(cache):
    with pytest.raises(TypeError):
        cache.get("k", factory=lambda: 1, ttl=60, extend_ttl=60)


def test_refresh_ahead_must_be_shorter_than_ttl(cache):
    with pytest.raises(ValueError):
        cache.get("k", factory=lambda: 1, ttl=60, refresh_ahead=60)


def test_refresh_ahead_accepts_timedelta(cache):
    value = cache.get(
        "k",
        factory=lambda: 1,
        ttl=timedelta(minutes=1),
        refresh_ahead=timedelta(seconds=10),
    )
    assert value == 1


def test_refresh_ahead_checked_against_datetime_ttl(cache):
    soon = datetime.now(timezone.utc) + timedelta(seconds=30)
    with pytest.raises(ValueError):
        cache.get("k", factory=lambda: 1, ttl=soon, refresh_ahead=60)


# ----------------------------------------------------------------------
# S2: aggregation


def test_get_many_returns_hits_keyed_by_caller_keys(cache):
    cache.set_many({"a": 1, b"b": 2}, ttl=60)
    found = cache.get_many(["a", b"b", "missing"])
    assert found == {"a": 1, b"b": 2}


def test_set_many_delete_many(cache):
    cache.set_many({"x": "1", "y": "2", "z": "3"}, ttl=60)
    cache.delete_many(["x", "y"])
    assert cache.get_many(["x", "y", "z"]) == {"z": "3"}


def test_large_batch_crosses_pipeline_chunks(cache):
    # Values sized so the request pipeline exceeds one 512KB chunk.
    payload = "x" * 20_000
    mapping = {"bulk:%d" % i: payload for i in range(40)}
    cache.set_many(mapping, ttl=60)
    found = cache.get_many(list(mapping))
    assert found == mapping


# ----------------------------------------------------------------------
# S3: factory and stampede protection


def test_factory_computes_once_and_writes_back(cache):
    calls = []

    def build():
        calls.append(1)
        return {"report": 42}

    assert cache.get("report", factory=build, ttl=60) == {"report": 42}
    assert cache.get("report", factory=build, ttl=60) == {"report": 42}
    assert len(calls) == 1
    assert cache.get("report") == {"report": 42}


def test_factory_result_none_is_written_back(cache):
    calls = []

    def build():
        calls.append(1)
        return None

    assert cache.get("nullable", factory=build, ttl=60) is None
    assert cache.get("nullable", factory=build, ttl=60) is None
    assert len(calls) == 1


def test_factory_merges_concurrent_callers_in_process(cache):
    calls = []
    started = threading.Event()

    def build():
        calls.append(1)
        started.wait(1)
        return "shared"

    results = []
    threads = [
        threading.Thread(
            target=lambda: results.append(cache.get("hot", factory=build, ttl=60))
        )
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    time.sleep(0.2)
    started.set()
    for thread in threads:
        thread.join()
    assert results == ["shared"] * 8
    assert len(calls) == 1


def test_factory_loser_in_another_process_waits_for_winner(cache):
    with Memcache(ADDR, serializer=PickleSerializer()) as other:
        release = threading.Event()

        def slow_build():
            release.wait(2)
            return "winner"

        winner = threading.Thread(
            target=lambda: cache.get("cross", factory=slow_build, ttl=60)
        )
        winner.start()
        time.sleep(0.1)
        release.set()
        # The other client polls until the winner's write lands.
        never = lambda: pytest.fail("the loser must not recompute")  # noqa: E731
        assert other.get("cross", factory=never, ttl=60) == "winner"
        winner.join()


def test_factory_exception_propagates_and_releases_lease(cache):
    def boom():
        raise RuntimeError("factory failed")

    with pytest.raises(RuntimeError, match="factory failed"):
        cache.get("fragile", factory=boom, ttl=60)
    # The lease was released, so the next call re-elects immediately
    # instead of waiting out the placeholder ttl.
    start = time.monotonic()
    assert cache.get("fragile", factory=lambda: "ok", ttl=60) == "ok"
    assert time.monotonic() - start < 0.5


def test_factory_write_back_is_conditional(cache):
    failures: list[BaseException] = []
    cache._on_failure = failures.append
    entered = threading.Event()
    release = threading.Event()

    def build():
        entered.set()
        release.wait(2)
        return "computed"

    result = []
    winner = threading.Thread(
        target=lambda: result.append(cache.get("cond", factory=build, ttl=60))
    )
    winner.start()
    entered.wait(1)
    with Memcache(ADDR, serializer=PickleSerializer()) as other:
        other.set("cond", "overwritten", ttl=60)
    release.set()
    winner.join()
    # The caller still gets the computed value, but the stale write-back
    # was abandoned instead of clobbering the concurrent set.
    assert result == ["computed"]
    assert cache.get("cond") == "overwritten"
    assert any("abandoned" in str(failure) for failure in failures)


# ----------------------------------------------------------------------
# S4: refresh ahead


def test_refresh_ahead_recomputes_before_expiry(cache):
    assert cache.get("feed", factory=lambda: "v1", ttl=4, refresh_ahead=3) == "v1"
    time.sleep(2)
    # Inside the refresh window the sync winner recomputes and returns the
    # fresh value; the item never actually expires.
    assert cache.get("feed", factory=lambda: "v2", ttl=4, refresh_ahead=3) == "v2"
    assert cache.get("feed") == "v2"


# ----------------------------------------------------------------------
# S5: soft invalidation


def test_soft_delete_serves_stale_to_plain_readers(cache):
    cache.set("article", "v1", ttl=600)
    assert cache.delete("article", grace=60) is True
    # Plain readers keep the old copy, repeatedly: the accidental recache
    # win of the first read is handed back.
    assert cache.get("article") == "v1"
    assert cache.get("article") == "v1"


def test_soft_delete_elects_factory_reader_to_refresh(cache):
    cache.set("article", "v1", ttl=600)
    cache.delete("article", grace=60)
    assert cache.get("article", factory=lambda: "v2", ttl=600) == "v2"
    assert cache.get("article") == "v2"


def test_hard_delete_is_a_full_miss(cache):
    cache.set("gone", "v1", ttl=600)
    cache.delete("gone")
    assert cache.get("gone") is None


def test_update_treats_stale_as_miss(cache):
    cache.set("counted", 10, ttl=600)
    cache.delete("counted", grace=60)
    # fn transforms rather than recomputes; laundering invalidated data
    # back to fresh is exactly what must not happen.
    assert cache.update("counted", lambda n: n + 1, default=0, ttl=600) == 1
    assert cache.get("counted") == 1


def test_update_stale_without_default_raises(cache):
    cache.set("stale-only", 10, ttl=600)
    cache.delete("stale-only", grace=60)
    with pytest.raises(NotFoundError):
        cache.update("stale-only", lambda n: n + 1, ttl=600)
    # The consumed stale token went back: a factory read still gets elected.
    assert cache.get("stale-only", factory=lambda: "fresh", ttl=60) == "fresh"


# ----------------------------------------------------------------------
# S6: update


def test_update_transforms_and_returns_new_value(cache):
    cache.set("cart", [1], ttl=600)
    assert cache.update("cart", lambda cart: cart + [2], ttl=600) == [1, 2]
    assert cache.get("cart") == [1, 2]


def test_update_miss_starts_from_default(cache):
    assert cache.update("cart", lambda cart: cart + [1], default=[], ttl=600) == [1]


def test_update_miss_without_default_raises(cache):
    with pytest.raises(NotFoundError):
        cache.update("absent", lambda v: v, ttl=600)


def test_update_is_atomic_under_concurrency(cache):
    cache.set("n", 0, ttl=600)
    errors = []

    def bump():
        try:
            for _ in range(20):
                cache.update("n", lambda n: n + 1, ttl=600)
        except Exception as exc:  # pragma: no cover - diagnostic
            errors.append(exc)

    threads = [threading.Thread(target=bump) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert cache.get("n") == 80


def test_update_fn_exception_aborts_without_writing(cache):
    cache.set("safe", 1, ttl=600)
    with pytest.raises(ZeroDivisionError):
        cache.update("safe", lambda v: 1 / 0 and v, ttl=600)  # noqa: B018
    assert cache.get("safe") == 1


def test_update_fn_exception_propagates(cache):
    cache.set("safe", 1, ttl=600)

    def bad(value):
        raise LookupError("no")

    with pytest.raises(LookupError):
        cache.update("safe", bad, ttl=600)


# ----------------------------------------------------------------------
# S7: counters


def test_incr_counts_from_zero_on_miss(cache):
    assert cache.incr("rate", ttl=60) == 1
    assert cache.incr("rate", ttl=60) == 2
    assert cache.incr("rate", 5, ttl=60) == 7


def test_decr_saturates_at_zero(cache):
    assert cache.incr("credits", 3, ttl=60) == 3
    assert cache.decr("credits", 10, ttl=60) == 0
    assert cache.decr("fresh", 5, ttl=60) == 0


def test_counter_ttl_applies_on_create_only(cache):
    cache.incr("window", ttl=5)
    cache.incr("window", ttl=500)
    assert cache.inspect("window").ttl <= 5


def test_counter_delta_validation(cache):
    with pytest.raises(ValueError):
        cache.incr("rate", 0, ttl=60)
    with pytest.raises(ValueError):
        cache.incr("rate", -1, ttl=60)


# ----------------------------------------------------------------------
# S8/S9: add, replace, touch, extend_ttl


def test_add_wins_only_once(cache):
    assert cache.add("job:today", "1", ttl=60) is True
    assert cache.add("job:today", "1", ttl=60) is False


def test_replace_never_resurrects(cache):
    assert cache.replace("session", {"uid": 1}, ttl=60) is False
    cache.set("session", {"uid": 1}, ttl=60)
    assert cache.replace("session", {"uid": 2}, ttl=60) is True
    cache.delete("session")
    assert cache.replace("session", {"uid": 3}, ttl=60) is False
    assert cache.get("session") is None


def test_touch_extends_without_value_transfer(cache):
    cache.set("render", "page", ttl=5)
    assert cache.touch("render", 600) is True
    assert cache.inspect("render").ttl > 500
    assert cache.touch("ghost", 600) is False


def test_get_extend_ttl_slides_expiry(cache):
    cache.set("session", "data", ttl=5)
    assert cache.get("session", extend_ttl=600) == "data"
    assert cache.inspect("session").ttl > 500
    assert cache.get("session-miss", "gone", extend_ttl=600) == "gone"


# ----------------------------------------------------------------------
# S11: event buffers


def test_append_pop_buffer(cache):
    cache.append("events", b"login;", ttl=600)
    cache.append("events", "click;", ttl=600)
    assert cache.pop("events") == b"login;click;"
    assert cache.pop("events") is None
    assert cache.pop("events", default=b"") == b""


def test_prepend_puts_newest_first(cache):
    cache.append("log", b"first;", ttl=600)
    cache.prepend("log", b"newest;", ttl=600)
    assert cache.pop("log") == b"newest;first;"


def test_append_rejects_empty_and_non_bytes(cache):
    with pytest.raises(ValueError):
        cache.append("events", b"", ttl=600)
    with pytest.raises(TypeError):
        cache.append("events", 123, ttl=600)


def test_pop_returns_objects_too(cache):
    cache.set("token", {"once": True}, ttl=600)
    assert cache.pop("token") == {"once": True}
    assert cache.get("token") is None


def test_pop_never_loses_concurrent_appends(cache):
    total = 200
    done = threading.Event()

    def produce():
        for i in range(total):
            cache.append("stream", b"%d;" % i, ttl=600)
        done.set()

    producer = threading.Thread(target=produce)
    collected: list[bytes] = []
    producer.start()
    while not done.is_set() or cache.inspect("stream") is not None:
        chunk = cache.pop("stream", default=b"")
        collected.extend(part for part in chunk.split(b";") if part)
    producer.join()
    assert sorted(int(part) for part in collected) == list(range(total))


# ----------------------------------------------------------------------
# S12: pipeline


def test_pipeline_runs_mixed_verbs_in_one_batch(cache):
    cache.set("user", {"uid": 1}, ttl=600)
    with cache.pipeline() as p:
        user = p.get("user")
        hits = p.incr("rate", ttl=60)
        touched = p.touch("user", 900)
        added = p.add("job", "1", ttl=60)
        info = p.inspect("user")
    assert user.value == {"uid": 1}
    assert hits.value == 1
    assert touched.value is True
    assert added.value is True
    assert isinstance(info.value, ItemInfo)


def test_pipeline_value_unreadable_before_exit(cache):
    with cache.pipeline() as p:
        deferred = p.get("k")
        with pytest.raises(RuntimeError):
            deferred.value
    assert deferred.value is None


def test_pipeline_semantic_outcomes_are_per_operation(cache):
    cache.set("present", "v", ttl=60)
    with cache.pipeline() as p:
        hit = p.get("present")
        miss = p.get("absent", "fallback")
        not_replaced = p.replace("absent", "v", ttl=60)
        deleted = p.delete("absent")
    assert hit.value == "v"
    assert miss.value == "fallback"
    assert not_replaced.value is False
    assert deleted.value is False


def test_pipeline_body_exception_skips_execution(cache):
    with pytest.raises(RuntimeError, match="boom"):
        with cache.pipeline() as p:
            deferred = p.set("skipped", "v", ttl=60)
            raise RuntimeError("boom")
    with pytest.raises(RuntimeError):
        deferred.value
    assert cache.get("skipped") is None


def test_pipeline_attribution_trusts_in_order_processing():
    # A later command's answer proves the server processed everything before
    # it on that connection, so a silent quiet write ahead of it is settled
    # even though the batch died before its barrier.
    from memcache.experiment._core import (
        PipelineRun,
        WireOp,
        WireOutcome,
        finalize_outcomes,
        resolve_group,
    )
    from memcache.meta_command import MetaCommand, MetaResult

    ops = [
        (0, WireOp(MetaCommand(b"ms", b"a", 1, [], b"x"), side_effect=True)),
        (1, WireOp(MetaCommand(b"mg", b"b", None, [], None), side_effect=False)),
        (2, WireOp(MetaCommand(b"ms", b"c", 1, [], b"x"), side_effect=True)),
        (3, WireOp(MetaCommand(b"ms", b"d", 1, [], b"x"), side_effect=True)),
    ]
    answered = MetaResult(b"VA", 1, [b"O1"], b"v")
    run = PipelineRun([answered], written=3, error=OSError("gone"))
    pending: list[WireOutcome | None] = [None] * 4
    resolve_group(ops, pending, run)
    output = finalize_outcomes(pending)
    assert output[0].error is None and output[0].response is None
    assert output[1].response is not None
    assert output[2].ambiguous and output[2].error is run.error
    assert not output[3].ambiguous and output[3].error is run.error


def test_pipeline_has_no_multi_round_trip_verbs(cache):
    pipeline = cache.pipeline()
    with pytest.raises(TypeError):
        pipeline.get("k", factory=lambda: 1, ttl=60)
    assert not hasattr(pipeline, "update")
    assert not hasattr(pipeline, "pop")
    assert not hasattr(pipeline, "get_many")


# ----------------------------------------------------------------------
# S13: inspect


def test_inspect_reports_metadata(cache):
    cache.set("probe", "x" * 100, ttl=300)
    info = cache.inspect("probe")
    assert 0 < info.ttl <= 300
    assert info.size >= 100
    assert info.last_access >= 0
    assert info.hit_before is False
    cache.get("probe")
    assert cache.inspect("probe").hit_before is True


def test_inspect_miss_returns_none(cache):
    assert cache.inspect("missing") is None


# ----------------------------------------------------------------------
# prefix


def test_prefix_isolates_namespaces():
    with Memcache(ADDR, prefix="app1:", serializer=JsonSerializer()) as one:
        with Memcache(ADDR, prefix="app2:", serializer=JsonSerializer()) as two:
            one.flush_all()
            one.set("k", "one", ttl=60)
            two.set("k", "two", ttl=60)
            assert one.get("k") == "one"
            assert two.get("k") == "two"


def test_prefix_applies_to_meta_namespace():
    with Memcache(ADDR, prefix="pre:", serializer=JsonSerializer()) as client:
        client.flush_all()
        client.set("k", "v", ttl=60)
        assert client.meta.get("k").rc == b"VA"
        with Memcache(ADDR, serializer=JsonSerializer()) as raw:
            assert raw.get("pre:k") == "v"


# ----------------------------------------------------------------------
# S10: failure policy


def test_raise_mode_surfaces_infrastructure_failures():
    with Memcache(DEAD_ADDR, timeout=0.2) as client:
        with pytest.raises(OperationFailedError):
            client.get("k")
        with pytest.raises((OperationFailedError, AmbiguousWriteError)):
            client.set("k", "v", ttl=60)


def test_degrade_reads_become_misses(dead_cache):
    assert dead_cache.get("k") is None
    assert dead_cache.get("k", default="d") == "d"
    assert dead_cache.get_many(["a", "b"]) == {}
    assert dead_cache.inspect("k") is None
    assert dead_cache.failures


def test_degrade_writes_are_silently_dropped(dead_cache):
    assert dead_cache.set("k", "v", ttl=60) is None
    dead_cache.set_many({"a": 1}, ttl=60)
    assert dead_cache.delete("k") is False
    dead_cache.delete_many(["k"])
    assert dead_cache.touch("k", 60) is False
    assert dead_cache.append("k", b"x", ttl=60) is None


def test_degrade_never_fakes_business_answers(dead_cache):
    with pytest.raises(OperationFailedError):
        dead_cache.add("k", "v", ttl=60)
    with pytest.raises(OperationFailedError):
        dead_cache.replace("k", "v", ttl=60)
    with pytest.raises(OperationFailedError):
        dead_cache.incr("k", ttl=60)
    with pytest.raises(OperationFailedError):
        dead_cache.update("k", lambda v: v, default=0, ttl=60)
    with pytest.raises(OperationFailedError):
        dead_cache.pop("k")


def test_degrade_factory_computes_without_cache(dead_cache):
    assert dead_cache.get("k", factory=lambda: "computed", ttl=60) == "computed"


def test_degrade_pipeline_follows_the_table(dead_cache):
    with dead_cache.pipeline() as p:
        read = p.get("k", "d")
        write = p.set("k", "v", ttl=60)
        counter = p.incr("k", ttl=60)
    assert read.value == "d"
    assert write.value is None
    with pytest.raises(OperationFailedError):
        counter.value


# ----------------------------------------------------------------------
# lifecycle


def test_closed_client_rejects_operations():
    client = Memcache(ADDR)
    client.close()
    with pytest.raises(RuntimeError, match="client is closed"):
        client.get("k")


def test_conflict_error_is_exported():
    assert issubclass(ConflictError, Exception)
    assert issubclass(PipelineError, Exception)
