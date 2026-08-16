import asyncio
import time

import pytest
import pytest_asyncio

from memcache.experiment import (
    AsyncMemcache,
    NotFoundError,
    OperationFailedError,
    PickleSerializer,
)

ADDR = ("localhost", 11211)
DEAD_ADDR = ("localhost", 1)


# pytest-asyncio finalizes async fixtures in a different task than the one
# that set them up, which is incompatible with entering the client's task
# group in __aenter__. The fixture therefore skips the context manager
# (background work then runs inline); tests about background behavior open
# their own context-managed client in the test body.
@pytest_asyncio.fixture()
async def cache():
    client = AsyncMemcache(ADDR, serializer=PickleSerializer())
    await client.flush_all()
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_roundtrip_verbs(cache):
    await cache.set("user", {"uid": 1}, ttl=600)
    assert await cache.get("user") == {"uid": 1}
    assert await cache.get("missing", default="d") == "d"
    assert await cache.delete("user") is True
    assert await cache.delete("user") is False
    assert await cache.incr("rate", ttl=60) == 1
    assert await cache.decr("rate", 5, ttl=60) == 0
    assert await cache.add("job", "1", ttl=60) is True
    assert await cache.add("job", "1", ttl=60) is False
    assert await cache.replace("job", "2", ttl=60) is True
    assert await cache.touch("job", 600) is True
    await cache.append("events", b"a;", ttl=60)
    await cache.prepend("events", b"z;", ttl=60)
    assert await cache.pop("events") == b"z;a;"
    info = await cache.inspect("job")
    assert info is not None and info.ttl > 500
    assert await cache.inspect("missing") is None


@pytest.mark.asyncio
async def test_many_verbs(cache):
    await cache.set_many({"a": 1, "b": 2}, ttl=60)
    assert await cache.get_many(["a", "b", "c"]) == {"a": 1, "b": 2}
    await cache.delete_many(["a", "b"])
    assert await cache.get_many(["a", "b"]) == {}


@pytest.mark.asyncio
async def test_update_accepts_async_fn(cache):
    async def bump(cart):
        await asyncio.sleep(0.01)
        return cart + [1]

    assert await cache.update("cart", bump, default=[], ttl=600) == [1]
    with pytest.raises(NotFoundError):
        await cache.update("absent", bump, ttl=600)


@pytest.mark.asyncio
async def test_factory_accepts_async_and_sync_callables(cache):
    async def build():
        await asyncio.sleep(0.01)
        return "async"

    assert await cache.get("a", factory=build, ttl=60) == "async"
    assert await cache.get("s", factory=lambda: "sync", ttl=60) == "sync"


@pytest.mark.asyncio
async def test_factory_merges_concurrent_callers(cache):
    calls = []

    async def slow():
        calls.append(1)
        await asyncio.sleep(0.05)
        return "shared"

    results = await asyncio.gather(
        *[cache.get("hot", factory=slow, ttl=60) for _ in range(20)]
    )
    assert set(results) == {"shared"}
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_factory_exception_reaches_waiters_and_releases_lease(cache):
    async def boom():
        raise RuntimeError("factory failed")

    with pytest.raises(RuntimeError, match="factory failed"):
        await cache.get("fragile", factory=boom, ttl=60)
    start = time.monotonic()
    assert await cache.get("fragile", factory=lambda: "ok", ttl=60) == "ok"
    assert time.monotonic() - start < 0.5


@pytest.mark.asyncio
async def test_refresh_ahead_returns_current_value_and_recomputes_in_background():
    async with AsyncMemcache(ADDR, serializer=PickleSerializer()) as cache:
        await cache.flush_all()
        assert (
            await cache.get("feed", factory=lambda: "v1", ttl=4, refresh_ahead=3)
            == "v1"
        )
        await asyncio.sleep(2)
        started = asyncio.Event()

        async def rebuild():
            started.set()
            await asyncio.sleep(0.05)
            return "v2"

        # The async winner hands back the current value immediately; nobody
        # pays the refresh latency.
        before = time.monotonic()
        assert await cache.get("feed", factory=rebuild, ttl=4, refresh_ahead=3) == "v1"
        assert time.monotonic() - before < 0.05
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.sleep(0.2)
        assert await cache.get("feed") == "v2"


@pytest.mark.asyncio
async def test_stale_grace_serves_old_value_and_refreshes_in_background():
    async with AsyncMemcache(ADDR, serializer=PickleSerializer()) as cache:
        await cache.flush_all()
        await cache.set("article", "v1", ttl=600)
        await cache.delete("article", grace=60)

        async def rebuild():
            await asyncio.sleep(0.05)
            return "v2"

        assert await cache.get("article", factory=rebuild, ttl=600) == "v1"
        await asyncio.sleep(0.3)
        assert await cache.get("article") == "v2"


@pytest.mark.asyncio
async def test_waiter_cancellation_does_not_cancel_the_factory():
    async with AsyncMemcache(ADDR, serializer=PickleSerializer()) as cache:
        await cache.flush_all()
        release = asyncio.Event()

        async def slow():
            await release.wait()
            return "survived"

        first = asyncio.ensure_future(cache.get("hot", factory=slow, ttl=60))
        await asyncio.sleep(0.1)
        second = asyncio.ensure_future(cache.get("hot", factory=slow, ttl=60))
        await asyncio.sleep(0.1)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        release.set()
        assert await first == "survived"
        assert await cache.get("hot") == "survived"


@pytest.mark.asyncio
async def test_close_cancels_background_refresh():
    async with AsyncMemcache(ADDR, serializer=PickleSerializer()) as client:
        await client.flush_all()
        await client.set("k", "old", ttl=600)
        await client.delete("k", grace=600)
        started = asyncio.Event()

        async def hang():
            started.set()
            await asyncio.sleep(30)
            return "never"

        assert await client.get("k", factory=hang, ttl=600) == "old"
        await asyncio.wait_for(started.wait(), 1)
    # Exiting the context cancelled the hanging refresh task; the value
    # was never overwritten and nothing is left running.
    async with AsyncMemcache(ADDR, serializer=PickleSerializer()) as client:
        assert await client.get("k") == "old"


@pytest.mark.asyncio
async def test_pipeline(cache):
    await cache.set("user", {"uid": 1}, ttl=600)
    async with cache.pipeline() as p:
        user = p.get("user")
        hits = p.incr("rate", ttl=60)
        touched = p.touch("session", 600)
    assert user.value == {"uid": 1}
    assert hits.value == 1
    assert touched.value is False


@pytest.mark.asyncio
async def test_degrade_follows_the_table():
    failures: list[BaseException] = []
    async with AsyncMemcache(
        DEAD_ADDR,
        serializer=PickleSerializer(),
        on_error="degrade",
        on_failure=failures.append,
        timeout=0.2,
    ) as client:
        assert await client.get("k", default="d") == "d"
        await client.set("k", "v", ttl=60)
        assert await client.delete("k") is False
        assert await client.get("k", factory=lambda: "local", ttl=60) == "local"
        with pytest.raises(OperationFailedError):
            await client.incr("k", ttl=60)
        with pytest.raises(OperationFailedError):
            await client.add("k", "v", ttl=60)
    assert failures


@pytest.mark.asyncio
async def test_context_manager_closes_client():
    async with AsyncMemcache(ADDR) as client:
        pass
    with pytest.raises(RuntimeError, match="client is closed"):
        await client.get("k")


@pytest.mark.asyncio
async def test_exceptions_propagate_through_aexit():
    with pytest.raises(RuntimeError, match="boom"):
        async with AsyncMemcache(ADDR):
            raise RuntimeError("boom")
