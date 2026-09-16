import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from memcache.errors import PipelineError
from memcache.meta_command import MetaCommand
from memcache.experiment import (
    FOREVER,
    AmbiguousWriteError,
    CommandError,
    ConflictError,
    ItemInfo,
    JsonSerializer,
    Memcache,
    NotFoundError,
    OperationFailedError,
    PickleSerializer,
    SerializeError,
)
from memcache.experiment._core import wait_schedule

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


def test_ttl_subsecond_timedelta_rounds_up_not_forever():
    # Checked on the wire value: a one second entry can already be gone by
    # the time a follow-up read reaches the server.
    from memcache.experiment._core import wire_ttl

    assert wire_ttl(timedelta(milliseconds=500)) == 1


def test_ttl_rejects_negative_timedelta():
    with Memcache(ADDR) as client:
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=timedelta(seconds=-1))
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=timedelta(milliseconds=-500))


def test_ttl_accepts_aware_datetime(cache):
    cache.set("dated", "v", ttl=datetime.now(timezone.utc) + timedelta(minutes=5))
    # The absolute moment is rounded up to a whole second and memcached's
    # own clock only ticks once a second, so the remaining lifetime may read
    # up to two seconds past the requested duration.
    assert 0 < cache.inspect("dated").ttl <= 302


def test_ttl_rejects_naive_and_past_datetime():
    with Memcache(ADDR) as client:
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=datetime.now() + timedelta(minutes=5))
        with pytest.raises(ValueError):
            client.set("k", "v", ttl=datetime.now(timezone.utc) - timedelta(minutes=5))


def test_overlong_keys_are_argument_errors(cache):
    long_key = "k" * 300
    for call in (
        lambda: cache.get(long_key),
        lambda: cache.set(long_key, "v", ttl=60),
        lambda: cache.delete(long_key),
        lambda: cache.meta.get(long_key),
    ):
        with pytest.raises(ValueError, match="key too long"):
            call()
    with pytest.raises(ValueError, match="key too long"):
        with cache.pipeline() as p:
            p.set(long_key, "v", ttl=60)
    # The prefix counts toward the limit, so a key that fits alone can
    # still be rejected once namespaced.
    with Memcache(ADDR, prefix="p" * 200) as prefixed:
        with pytest.raises(ValueError, match="key too long"):
            prefixed.get("k" * 60)


def test_pipeline_counts_written_only_after_encoding():
    # A command the encoder rejects never reaches the wire, so a failed
    # pipeline must not report it as possibly landed.
    from memcache.connection import Connection

    connection = Connection(ADDR, timeout=1)
    try:
        with pytest.raises(PipelineError) as info:
            connection.send_pipeline([MetaCommand(b"ms", b"k" * 300, 1, [], b"x")])
        assert info.value.written == 0
    finally:
        connection.close()


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


def test_refresh_ahead_requires_a_finite_ttl(cache):
    with pytest.raises(ValueError, match="finite ttl"):
        cache.get("k", factory=lambda: 1, ttl=FOREVER, refresh_ahead=10)


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


def test_factory_loser_outwaits_a_slow_winner(cache):
    with Memcache(ADDR, serializer=PickleSerializer()) as other:

        def slow_build():
            time.sleep(1.2)
            return "winner"

        winner = threading.Thread(
            target=lambda: cache.get("cross", factory=slow_build, ttl=60)
        )
        winner.start()
        time.sleep(0.1)
        # A factory slower than a second still costs one computation: the
        # default lease_wait budget keeps the loser polling until the
        # winner's write lands.
        never = lambda: pytest.fail("the loser must not recompute")  # noqa: E731
        assert other.get("cross", factory=never, ttl=60) == "winner"
        winner.join()


def test_lease_wait_bounds_the_cross_process_wait(cache):
    with Memcache(ADDR, serializer=PickleSerializer(), lease_wait=0.2) as other:
        release = threading.Event()

        def slow_build():
            release.wait(5)
            return "winner"

        winner = threading.Thread(
            target=lambda: cache.get("cross", factory=slow_build, ttl=60)
        )
        winner.start()
        time.sleep(0.1)
        started = time.monotonic()
        # Past its budget the loser computes locally without writing back.
        assert other.get("cross", factory=lambda: "local", ttl=60) == "local"
        assert 0.2 <= time.monotonic() - started < 1.0
        release.set()
        winner.join()
        assert cache.get("cross") == "winner"


def test_lease_wait_zero_never_waits(cache):
    with Memcache(ADDR, serializer=PickleSerializer(), lease_wait=0) as other:
        release = threading.Event()

        def slow_build():
            release.wait(5)
            return "winner"

        winner = threading.Thread(
            target=lambda: cache.get("cross", factory=slow_build, ttl=60)
        )
        winner.start()
        time.sleep(0.1)
        started = time.monotonic()
        assert other.get("cross", factory=lambda: "local", ttl=60) == "local"
        assert time.monotonic() - started < 0.1
        release.set()
        winner.join()


def test_lease_ttl_bounds_the_winner_placeholder(cache):
    with Memcache(ADDR, serializer=PickleSerializer(), lease_ttl=7) as short:
        release = threading.Event()

        def slow_build():
            release.wait(5)
            return "winner"

        winner = threading.Thread(
            target=lambda: short.get("leased", factory=slow_build, ttl=60)
        )
        winner.start()
        time.sleep(0.1)
        # The placeholder the winner holds expires with the client's lease
        # ttl, so a winner that dies is re-elected after that long.
        raw = cache.meta.get("leased", return_ttl=True, return_size=True)
        assert raw.size == 0 and raw.ttl is not None and 5 <= raw.ttl <= 7
        release.set()
        winner.join()


def test_lease_policy_validation():
    with pytest.raises(ValueError, match="lease_ttl"):
        Memcache(ADDR, lease_ttl=0)
    with pytest.raises(TypeError, match="lease_ttl"):
        Memcache(ADDR, lease_ttl=1.5)
    with pytest.raises(ValueError, match="30 days"):
        Memcache(ADDR, lease_ttl=timedelta(days=31))
    with pytest.raises(ValueError, match="lease_wait"):
        Memcache(ADDR, lease_wait=-1)
    with pytest.raises(TypeError, match="lease_wait"):
        Memcache(ADDR, lease_wait="1")
    Memcache(
        ADDR, lease_ttl=timedelta(seconds=10), lease_wait=timedelta(milliseconds=50)
    ).close()


def test_wait_schedule_spends_exactly_its_budget():
    assert wait_schedule(0) == ()
    assert wait_schedule(0.01) == (0.01,)
    schedule = wait_schedule(5)
    assert sum(schedule) == pytest.approx(5)
    assert schedule[0] == 0.025
    assert max(schedule) == 0.5
    # Delays only grow until the last one, which is trimmed to the budget.
    assert list(schedule[:-1]) == sorted(schedule[:-1])


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


def test_elected_follower_writes_the_shared_value_back(cache):
    with Memcache(ADDR, serializer=PickleSerializer(), lease_wait=0.3) as short:
        release = threading.Event()
        calls = []

        def slow_build():
            calls.append(1)
            release.wait(5)
            return "shared"

        winner = threading.Thread(
            target=lambda: short.get("k", factory=slow_build, ttl=60)
        )
        winner.start()
        time.sleep(0.1)
        # The winner's placeholder disappears under it (an operator delete,
        # or a lease that expired before a slow factory finished), so the
        # next same-process reader wins a fresh lease of its own while the
        # first factory is still running.
        cache.delete("k")
        results = []
        follower = threading.Thread(
            target=lambda: results.append(short.get("k", factory=slow_build, ttl=60))
        )
        follower.start()
        time.sleep(0.1)
        release.set()
        winner.join()
        follower.join()
        assert results == ["shared"]
        assert calls == [1]
        # The follower shared the winner's value and paid for its own lease
        # with it, so the cache is warm instead of holding a placeholder
        # that makes every reader wait out lease_wait until it expires.
        assert cache.get("k") == "shared"
        start = time.monotonic()
        assert short.get("k", factory=lambda: "cold", ttl=60) == "shared"
        assert time.monotonic() - start < 0.2


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


def test_stale_winner_failure_returns_the_recache_token(cache):
    cache.set("article", "v1", ttl=600)
    cache.delete("article", grace=60)

    def boom():
        raise RuntimeError("factory failed")

    with pytest.raises(RuntimeError, match="factory failed"):
        cache.get("article", factory=boom, ttl=600)
    assert cache.get("article") == "v1"
    calls = []

    def rebuild():
        calls.append(1)
        return "v2"

    # The failed winner handed its token back, so the next factory reader
    # is elected instead of everyone serving v1 for the rest of the grace.
    assert cache.get("article", factory=rebuild, ttl=600) == "v2"
    assert calls == [1]
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


def test_slide_sticks_on_a_stale_entry(cache):
    # A read that touches a soft-deleted entry wins the grace period's
    # recache token; handing the token back must not undo the slide.
    cache.set("session", "data", ttl=1800)
    cache.delete("session", grace=60)
    assert cache.get("session", extend_ttl=1800) == "data"
    assert cache.inspect("session").ttl > 1000
    cache.set("render", "page", ttl=1800)
    cache.delete("render", grace=60)
    assert cache.touch("render", 1800) is True
    assert cache.inspect("render").ttl > 1000
    # The token did go back: a factory reader is still elected.
    calls = []
    rebuild = lambda: calls.append(1) or "new"  # noqa: E731
    assert cache.get("render", factory=rebuild, ttl=60) == "new"
    assert calls == [1]


def test_lease_placeholder_is_absent_to_inspect_and_touch(cache):
    release = threading.Event()

    def slow_build():
        release.wait(5)
        return "built"

    winner = threading.Thread(
        target=lambda: cache.get("leased", factory=slow_build, ttl=60)
    )
    winner.start()
    time.sleep(0.1)
    try:
        # The winner's zero-byte placeholder is a coordination artifact, not
        # an item: every read-only door folds it to absence.
        assert cache.get("leased") is None
        assert cache.inspect("leased") is None
        assert cache.touch("leased", 600) is False
    finally:
        release.set()
        winner.join()
    assert cache.inspect("leased").size > 0


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


def test_pipeline_execution_failure_reaches_every_deferred(cache):
    with pytest.raises(RuntimeError, match="closed"):
        with cache.pipeline() as p:
            read = p.get("k")
            cache.close()
    with pytest.raises(RuntimeError, match="closed"):
        read.value


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


def test_pipeline_attributes_server_error_lines_by_position():
    # An error line carries no opaque token; it belongs to a command between
    # the answers around it, and to the first one that owed an answer.
    from memcache.experiment._core import (
        PipelineRun,
        WireOp,
        WireOutcome,
        finalize_outcomes,
        resolve_group,
    )
    from memcache.meta_command import MetaResult

    def op(cm, key, quiet=True):
        value = b"x" if cm == b"ms" else None
        command = MetaCommand(cm, key, 1 if value else None, [], value)
        return WireOp(command, side_effect=cm != b"mg", quiet=quiet)

    ops = list(
        enumerate(
            [
                op(b"ms", b"a"),
                op(b"ma", b"s", quiet=False),
                op(b"mg", b"b"),
                op(b"ms", b"c"),
            ]
        )
    )
    responses = [
        MetaResult(b"CLIENT_ERROR", None, [], None, error="cannot increment"),
        MetaResult(b"VA", 1, [b"O2"], b"v"),
    ]
    pending: list[WireOutcome | None] = [None] * 4
    resolve_group(ops, pending, PipelineRun(responses))
    output = finalize_outcomes(pending)
    assert output[0].error is None and output[0].response is None
    assert isinstance(output[1].error, CommandError) and not output[1].ambiguous
    assert output[2].response is not None
    assert output[3].error is None

    # With only quiet commands in the span, every silent one is reported
    # rejected rather than guessed at.
    ops = list(enumerate([op(b"ms", b"a"), op(b"ms", b"b")]))
    pending = [None] * 2
    resolve_group(ops, pending, PipelineRun([responses[0]]))
    output = finalize_outcomes(pending)
    assert all(isinstance(o.error, CommandError) for o in output)


def test_server_rejections_are_definite_and_local(cache):
    cache.set("text", "abc", ttl=60)
    cache.set("other", "v", ttl=60)
    with pytest.raises(TypeError, match="not a counter"):
        cache.incr("text", ttl=60)
    # The rejection is that operation's own answer: nothing else in the
    # batch is disturbed, and the connection stays usable.
    with cache.pipeline() as p:
        counter = p.incr("text", ttl=60)
        written = p.set("a", "aval", ttl=60)
        read = p.get("other")
        touched = p.touch("other", 600)
    with pytest.raises(TypeError, match="not a counter"):
        counter.value
    assert written.value is None and cache.get("a") == "aval"
    assert read.value == "v"
    assert touched.value is True
    # A value the server will not accept is a definite failure with the
    # server's message attached, never an ambiguous write.
    with pytest.raises(OperationFailedError) as info:
        cache.set("huge", b"x" * (2 * 1024 * 1024), ttl=60)
    assert isinstance(info.value.__cause__, CommandError)
    assert cache.get("huge") is None
    with pytest.raises(CommandError):
        cache.meta.arithmetic("text", delta=1)


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


def test_raise_mode_surfaces_unacknowledged_writes(hung_addr):
    with Memcache(hung_addr, timeout=0.2) as client:
        with pytest.raises(OperationFailedError) as info:
            client.get("k")
        assert info.value.key == "k"
        assert "TimeoutError" in str(info.value)
        with pytest.raises(AmbiguousWriteError) as info:
            client.set("k", "v", ttl=60)
        # One except clause covers every infrastructure failure; the
        # subclass is there for callers that must know a repeat is unsafe.
        assert isinstance(info.value, OperationFailedError)
        assert info.value.key == "k" and "may or may not" in str(info.value)
        with pytest.raises(OperationFailedError):
            client.get("k", factory=lambda: "v", ttl=60)


@pytest.fixture()
def hung_cache(hung_addr):
    failures: list[BaseException] = []
    with Memcache(
        hung_addr,
        serializer=PickleSerializer(),
        on_error="degrade",
        on_failure=failures.append,
        timeout=0.2,
    ) as client:
        client.failures = failures  # type: ignore[attr-defined]
        yield client


def test_degrade_absorbs_unacknowledged_idempotent_writes(hung_cache):
    # A hung server is the common outage: the write goes out and no answer
    # comes back. For writes whose repetition is harmless that is just "the
    # cache is down", so degrade covers them exactly like a refused connection.
    assert hung_cache.get("k", default="d") == "d"
    assert hung_cache.get("k", extend_ttl=60) is None
    assert hung_cache.set("k", "v", ttl=60) is None
    hung_cache.set_many({"a": 1}, ttl=60)
    assert hung_cache.delete("k") is False
    assert hung_cache.delete("k", grace=60) is False
    hung_cache.delete_many(["k"])
    assert hung_cache.touch("k", 60) is False
    assert hung_cache.get("k", factory=lambda: "local", ttl=60) == "local"
    assert hung_cache.failures
    assert all(isinstance(f, AmbiguousWriteError) for f in hung_cache.failures[2:])


def test_degrade_surfaces_unacknowledged_mutations(hung_cache):
    # A counter or a concatenation that may have landed cannot be safely
    # repeated, so its ambiguity is never folded into "the cache is down".
    for mutate in (
        lambda: hung_cache.incr("k", ttl=60),
        lambda: hung_cache.decr("k", ttl=60),
        lambda: hung_cache.append("k", b"x", ttl=60),
        lambda: hung_cache.prepend("k", b"x", ttl=60),
        lambda: hung_cache.add("k", "v", ttl=60),
        lambda: hung_cache.replace("k", "v", ttl=60),
    ):
        with pytest.raises(AmbiguousWriteError):
            mutate()
    with hung_cache.pipeline() as p:
        write = p.set("k", "v", ttl=60)
        fragment = p.append("k", b"x", ttl=60)
    assert write.value is None
    with pytest.raises(AmbiguousWriteError):
        fragment.value


def test_unreadable_values_follow_the_failure_policy(cache):
    # A value this client's serializer cannot read is a failure of the
    # cache for that key, not an answer: it is wrapped, so the degrade
    # policy and the failure hook apply exactly as for a dead server.
    cache.set("pickled", {"a": 1}, ttl=60)
    with Memcache(ADDR) as strict:
        with pytest.raises(OperationFailedError) as info:
            strict.get("pickled")
        assert isinstance(info.value.__cause__, SerializeError)
        with pytest.raises(OperationFailedError):
            strict.pop("pickled")
        with pytest.raises(OperationFailedError):
            strict.update("pickled", lambda v: v, ttl=60)
        with pytest.raises(OperationFailedError):
            strict.get("pickled", factory=lambda: "fresh", ttl=60)
    failures: list[BaseException] = []
    with Memcache(ADDR, on_error="degrade", on_failure=failures.append) as lenient:
        assert lenient.get("pickled", default="d") == "d"
        assert lenient.get_many(["pickled"]) == {}
        assert lenient.get("pickled", factory=lambda: "local", ttl=60) == "local"
        with lenient.pipeline() as p:
            read = p.get("pickled", "d")
        assert read.value == "d"
    assert len(failures) == 4
    assert all(isinstance(f.__cause__, SerializeError) for f in failures)


def test_unreadable_stale_value_returns_the_recache_token(cache):
    cache.set("article", {"v": 1}, ttl=600)
    cache.delete("article", grace=60)
    with Memcache(ADDR) as strict:
        # This read wins the grace period's single recache token and then
        # fails to deserialize; the token must go back or the factory
        # election stays closed for the rest of the window.
        with pytest.raises(OperationFailedError):
            strict.get("article")
    calls = []

    def rebuild():
        calls.append(1)
        return {"v": 2}

    assert cache.get("article", factory=rebuild, ttl=600) == {"v": 2}
    assert calls == [1]


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
