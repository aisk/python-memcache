import pytest
import pytest_asyncio

from memcache.async_connection import PipelineError
from memcache.experiment import (
    AmbiguousWriteError,
    AsyncMetaClient,
    Get,
    GetStatus,
    LeaseState,
    Meta,
    MutationStatus,
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
async def test_async_api_has_the_same_shape(client):
    stored = await client.set("key", {"value": 1}, ttl=60, return_cas=True)
    assert stored.status is MutationStatus.STORED
    assert stored.cas is not None

    result = await client.get("key", meta=Meta.CAS | Meta.TTL)
    assert result.status is GetStatus.HIT
    assert result.value == {"value": 1}
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
