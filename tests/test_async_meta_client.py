import pytest
import pytest_asyncio

from memcache import MetaCommand
from memcache.async_connection import PipelineError
from memcache.experiment import (
    AmbiguousWriteError,
    AsyncMetaClient,
    Get,
    GetStatus,
    LeaseState,
    Meta,
    MutationStatus,
    PickleSerializer,
    Set,
    ValueState,
)


@pytest.fixture()
def client():
    return AsyncMetaClient(("localhost", 11211))


@pytest_asyncio.fixture(autouse=True)
async def flush(client):
    await client.flush_all()
    yield
    await client.close()


@pytest.mark.asyncio
async def test_async_pool_grows_on_demand_and_reuses_fifo(client):
    server = client._servers[0]
    async with server._borrow() as first:
        async with server._borrow() as second:
            assert first is not second
    assert list(server._idle) == [second, first]
    async with server._borrow() as third:
        assert third is second  # oldest idle connection is reused first


@pytest.mark.asyncio
async def test_async_pool_trims_idle_connections_over_max_idle():
    async with AsyncMetaClient(("localhost", 11211), max_idle=1) as client:
        server = client._servers[0]
        async with server._borrow() as first:
            async with server._borrow() as second:
                assert first is not second
        assert list(server._idle) == [second]


@pytest.mark.asyncio
async def test_async_pool_discards_connection_on_failure(client):
    server = client._servers[0]
    await client.set("pooled", "v")
    assert len(server._idle) == 1
    with pytest.raises(ValueError):
        async with server._borrow():
            raise ValueError("boom")
    assert not server._idle


@pytest.mark.asyncio
async def test_async_api_has_the_same_shape(client):
    stored = await client.set("key", "value 1", ttl=60, return_cas=True)
    assert stored.status is MutationStatus.STORED
    assert stored.cas is not None

    result = await client.get("key", meta=Meta.CAS | Meta.TTL)
    assert result.status is GetStatus.HIT
    assert result.value == "value 1"
    assert result.item.cas == stored.cas

    unchanged = await client.get("key", unless_cas=stored.cas)
    assert unchanged.status is GetStatus.UNCHANGED

    batch = await client.batch([Get("key"), Get("missing"), Set("other", b"x")])
    assert [item.status for item in batch] == [
        GetStatus.HIT,
        GetStatus.MISS,
        MutationStatus.STORED,
    ]


@pytest.mark.asyncio
async def test_async_lease_cursor(client):
    winner = await client.get_with_lease("lease", lease_ttl=30)
    loser = await client.get_with_lease("lease", lease_ttl=30)
    assert winner.value_state is ValueState.MISSING
    assert winner.lease_state is LeaseState.GRANTED
    assert loser.lease_state is LeaseState.BUSY

    fulfilled = await winner.fulfill("ready", ttl=60)
    assert fulfilled.status is MutationStatus.STORED
    assert (await client.get("lease")).value == "ready"


@pytest.mark.asyncio
async def test_async_serializer_option(client):
    with pytest.raises(TypeError):
        await client.set("obj", {"value": 1})
    async with AsyncMetaClient(
        ("localhost", 11211), serializer=PickleSerializer()
    ) as pickled:
        stored = await pickled.set("obj", {"value": 1})
        assert stored.status is MutationStatus.STORED
        assert (await pickled.get("obj")).value == {"value": 1}


@pytest.mark.asyncio
async def test_async_meta_namespace_has_the_same_shape(client):
    stored = await client.meta.set("wire", b"payload", ttl=60, return_cas=True)
    assert stored.ok and stored.cas is not None

    got = await client.meta.get("wire", return_cas=True, return_ttl=True)
    assert got.rc == b"VA" and got.value == b"payload"
    assert got.cas == stored.cas

    count = await client.meta.arithmetic("hits", initial=1, initial_ttl=60)
    assert count.value == b"1"
    assert await client.meta.debug("wire") is not None

    assert (await client.meta.delete("wire")).rc == b"HD"
    assert (await client.meta.get("wire")).rc == b"EN"

    raw = await client.meta.execute(command="mg", key="hits", flags=[b"v"])
    assert raw.rc == b"VA" and raw.value == b"1"
    batch = await client.meta.batch(
        [MetaCommand(b"mg", b"hits", flags=[b"v"]), MetaCommand(b"mg", b"none")]
    )
    assert [item.rc for item in batch] == [b"VA", b"EN"]


@pytest.mark.asyncio
async def test_batch_marks_written_side_effects_ambiguous(monkeypatch):
    client = AsyncMetaClient(("localhost", 11211))

    async def fail(commands, timeout):
        raise PipelineError(2, [], ConnectionResetError("lost"))

    monkeypatch.setattr(client._servers[0], "pipeline", fail)
    results = await client.batch([Set("a", "v"), Get("b"), Set("c", "v")])
    assert results[0].status is MutationStatus.AMBIGUOUS
    assert results[1].status is GetStatus.FAILED
    assert results[2].status is MutationStatus.FAILED

    with pytest.raises(AmbiguousWriteError):
        await client.set("a", "v")
    await client.close()


@pytest.mark.asyncio
async def test_server_failure_is_isolated_in_batch():
    client = AsyncMetaClient([("localhost", 11211), ("localhost", 1)], timeout=0.2)
    good = bad = None
    for number in range(10000):
        key = "shard-%d" % number
        port = client._server_for(key).addr[1]
        if port == 11211 and good is None:
            good = key
        elif port == 1 and bad is None:
            bad = key
        if good is not None and bad is not None:
            break
    assert good is not None and bad is not None

    results = await client.batch([Set(good, "ok"), Set(bad, "no"), Get(good)])
    assert results[0].status is MutationStatus.STORED
    assert results[1].status is MutationStatus.FAILED
    assert results[2].status is GetStatus.HIT
    assert results[2].value == "ok"
    await client.close()
