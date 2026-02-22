import asyncio

import pytest
import pytest_asyncio

from memcache.experiment import AsyncMetaClient, GetResult
from memcache import MemcacheError
from memcache.meta_command import MetaCommand


@pytest.fixture()
def client() -> AsyncMetaClient:
    return AsyncMetaClient(("localhost", 11211))


@pytest_asyncio.fixture(autouse=True)
async def flush(client: AsyncMetaClient) -> None:
    await client.flush_all()


# ------------------------------------------------------------------ #
# get / set                                                             #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_set_get(client: AsyncMetaClient) -> None:
    await client.set("key1", "hello")
    r = await client.get("key1")
    assert r is not None
    assert r.value == "hello"


@pytest.mark.asyncio
async def test_get_missing(client: AsyncMetaClient) -> None:
    assert await client.get("no_such_key") is None


@pytest.mark.asyncio
async def test_set_get_with_expire(client: AsyncMetaClient) -> None:
    await client.set("expkey", "val", expire=1)
    r = await client.get("expkey")
    assert r is not None
    assert r.value == "val"
    await asyncio.sleep(1.1)
    assert await client.get("expkey") is None


@pytest.mark.asyncio
async def test_get_returns_getresult_instance(client: AsyncMetaClient) -> None:
    await client.set("gr_type", 42)
    r = await client.get("gr_type")
    assert isinstance(r, GetResult)
    assert r.value == 42


@pytest.mark.asyncio
async def test_get_no_lru_bump(client: AsyncMetaClient) -> None:
    await client.set("nlb", "v")
    r = await client.get("nlb", no_lru_bump=True)
    assert r is not None
    assert r.value == "v"


@pytest.mark.asyncio
async def test_get_update_ttl(client: AsyncMetaClient) -> None:
    await client.set("utt", "v", expire=10)
    r = await client.get("utt", update_ttl=3600)
    assert r is not None
    assert r.value == "v"


@pytest.mark.asyncio
async def test_get_return_cas(client: AsyncMetaClient) -> None:
    await client.set("gr_cas", "v")
    r = await client.get("gr_cas", return_cas=True)
    assert r is not None
    assert isinstance(r.cas_token, int)
    assert r.cas_token > 0


@pytest.mark.asyncio
async def test_get_return_ttl(client: AsyncMetaClient) -> None:
    await client.set("gr_ttl", "v", expire=3600)
    r = await client.get("gr_ttl", return_ttl=True)
    assert r is not None
    assert r.ttl is not None
    assert r.ttl > 0


@pytest.mark.asyncio
async def test_get_no_ttl_requested(client: AsyncMetaClient) -> None:
    await client.set("gr_nottl", "v")
    r = await client.get("gr_nottl")
    assert r is not None
    assert r.ttl is None


@pytest.mark.asyncio
async def test_get_return_size(client: AsyncMetaClient) -> None:
    await client.set("gr_size", b"hello")
    r = await client.get("gr_size", return_size=True)
    assert r is not None
    assert r.size == 5


@pytest.mark.asyncio
async def test_get_return_hit_before(client: AsyncMetaClient) -> None:
    await client.set("gr_hit", "v")
    r1 = await client.get("gr_hit", return_hit_before=True)
    assert r1 is not None
    assert r1.hit_before is False
    r2 = await client.get("gr_hit", return_hit_before=True)
    assert r2 is not None
    assert r2.hit_before is True


@pytest.mark.asyncio
async def test_get_return_last_access(client: AsyncMetaClient) -> None:
    await client.set("gr_la", "v")
    r = await client.get("gr_la", return_last_access=True)
    assert r is not None
    assert r.last_access is not None
    assert isinstance(r.last_access, int)


# ------------------------------------------------------------------ #
# gat / touch                                                           #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_gat(client: AsyncMetaClient) -> None:
    await client.set("gat_key", "v", expire=1)
    result = await client.gat("gat_key", expire=3600)
    assert result == "v"
    await asyncio.sleep(1.1)
    r = await client.get("gat_key")
    assert r is not None
    assert r.value == "v"


@pytest.mark.asyncio
async def test_gat_miss(client: AsyncMetaClient) -> None:
    assert await client.gat("no_gat_key", expire=60) is None


@pytest.mark.asyncio
async def test_touch_existing(client: AsyncMetaClient) -> None:
    await client.set("touch_key", "v", expire=1)
    assert await client.touch("touch_key", expire=3600) is True
    await asyncio.sleep(1.1)
    r = await client.get("touch_key")
    assert r is not None
    assert r.value == "v"


@pytest.mark.asyncio
async def test_touch_missing(client: AsyncMetaClient) -> None:
    assert await client.touch("no_touch_key", expire=60) is False


# ------------------------------------------------------------------ #
# get_many                                                              #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_get_many(client: AsyncMetaClient) -> None:
    await client.set("m1", "a")
    await client.set("m2", "b")
    result = await client.get_many(["m1", "m2", "m_miss"])
    assert set(result.keys()) == {"m1", "m2"}
    assert result["m1"].value == "a"
    assert result["m2"].value == "b"


@pytest.mark.asyncio
async def test_get_many_all_miss(client: AsyncMetaClient) -> None:
    assert await client.get_many(["x1", "x2"]) == {}


# ------------------------------------------------------------------ #
# add / replace                                                         #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_add_new_key(client: AsyncMetaClient) -> None:
    assert await client.add("add_new", "v") is True
    r = await client.get("add_new")
    assert r is not None
    assert r.value == "v"


@pytest.mark.asyncio
async def test_add_existing_key(client: AsyncMetaClient) -> None:
    await client.set("add_exists", "original")
    assert await client.add("add_exists", "new") is False
    r = await client.get("add_exists")
    assert r is not None
    assert r.value == "original"


@pytest.mark.asyncio
async def test_replace_existing(client: AsyncMetaClient) -> None:
    await client.set("rep_key", "old")
    assert await client.replace("rep_key", "new") is True
    r = await client.get("rep_key")
    assert r is not None
    assert r.value == "new"


@pytest.mark.asyncio
async def test_replace_missing(client: AsyncMetaClient) -> None:
    assert await client.replace("rep_miss", "v") is False


# ------------------------------------------------------------------ #
# append / prepend                                                      #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_append(client: AsyncMetaClient) -> None:
    await client.set("app_key", b"hello")
    assert await client.append("app_key", b" world") is True
    r = await client.get("app_key")
    assert r is not None
    assert r.value == b"hello world"


@pytest.mark.asyncio
async def test_append_missing_key(client: AsyncMetaClient) -> None:
    assert await client.append("app_miss", b"v") is False


@pytest.mark.asyncio
async def test_append_vivify(client: AsyncMetaClient) -> None:
    assert await client.append("app_vivify", b"data", vivify_ttl=60) is True
    r = await client.get("app_vivify")
    assert r is not None
    assert r.value == b"data"


@pytest.mark.asyncio
async def test_prepend(client: AsyncMetaClient) -> None:
    await client.set("pre_key", b"world")
    assert await client.prepend("pre_key", b"hello ") is True
    r = await client.get("pre_key")
    assert r is not None
    assert r.value == b"hello world"


@pytest.mark.asyncio
async def test_prepend_missing_key(client: AsyncMetaClient) -> None:
    assert await client.prepend("pre_miss", b"v") is False


@pytest.mark.asyncio
async def test_prepend_vivify(client: AsyncMetaClient) -> None:
    assert await client.prepend("pre_vivify", b"data", vivify_ttl=60) is True
    r = await client.get("pre_vivify")
    assert r is not None
    assert r.value == b"data"


# ------------------------------------------------------------------ #
# cas                                                                   #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_cas_success(client: AsyncMetaClient) -> None:
    await client.set("cas_key", "initial")
    r = await client.get("cas_key", return_cas=True)
    assert r is not None
    assert r.cas_token is not None
    assert await client.cas("cas_key", "updated", r.cas_token) is True
    r2 = await client.get("cas_key")
    assert r2 is not None
    assert r2.value == "updated"


@pytest.mark.asyncio
async def test_cas_conflict(client: AsyncMetaClient) -> None:
    await client.set("cas_conf", "v")
    r = await client.get("cas_conf", return_cas=True)
    assert r is not None
    assert r.cas_token is not None
    await client.set("cas_conf", "modified")
    assert await client.cas("cas_conf", "new", r.cas_token) is False
    r2 = await client.get("cas_conf")
    assert r2 is not None
    assert r2.value == "modified"


@pytest.mark.asyncio
async def test_cas_missing_key(client: AsyncMetaClient) -> None:
    assert await client.cas("cas_no_key", "v", 12345) is False


# ------------------------------------------------------------------ #
# delete                                                                #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_delete_existing(client: AsyncMetaClient) -> None:
    await client.set("del_key", "v")
    assert await client.delete("del_key") is True
    assert await client.get("del_key") is None


@pytest.mark.asyncio
async def test_delete_missing(client: AsyncMetaClient) -> None:
    assert await client.delete("del_miss") is False


@pytest.mark.asyncio
async def test_delete_with_cas(client: AsyncMetaClient) -> None:
    await client.set("del_cas", "v")
    r = await client.get("del_cas", return_cas=True)
    assert r is not None
    assert r.cas_token is not None
    assert await client.delete("del_cas", cas_token=r.cas_token) is True


@pytest.mark.asyncio
async def test_delete_with_wrong_cas(client: AsyncMetaClient) -> None:
    await client.set("del_cas_bad", "v")
    assert await client.delete("del_cas_bad", cas_token=99999999) is False


# ------------------------------------------------------------------ #
# invalidate                                                            #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_invalidate(client: AsyncMetaClient) -> None:
    await client.set("inv_key", "v")
    assert await client.invalidate("inv_key") is True


@pytest.mark.asyncio
async def test_invalidate_missing(client: AsyncMetaClient) -> None:
    assert await client.invalidate("inv_miss") is False


@pytest.mark.asyncio
async def test_invalidate_stale_flag(client: AsyncMetaClient) -> None:
    await client.set("inv_stale", "v")
    await client.invalidate("inv_stale", stale_ttl=10)
    r = await client.get("inv_stale")
    assert r is not None
    assert r.is_stale is True


# ------------------------------------------------------------------ #
# incr / decr                                                           #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_incr(client: AsyncMetaClient) -> None:
    await client.set("ctr", 10)
    assert await client.incr("ctr") == 11


@pytest.mark.asyncio
async def test_incr_with_delta(client: AsyncMetaClient) -> None:
    await client.set("ctr2", 5)
    assert await client.incr("ctr2", 3) == 8


@pytest.mark.asyncio
async def test_incr_missing(client: AsyncMetaClient) -> None:
    with pytest.raises(MemcacheError):
        await client.incr("incr_miss")


@pytest.mark.asyncio
async def test_incr_with_initial(client: AsyncMetaClient) -> None:
    # On miss with J flag, memcached creates item with initial value (delta not applied)
    assert await client.incr("incr_init", initial=100, initial_ttl=60) == 100


@pytest.mark.asyncio
async def test_incr_with_initial_existing(client: AsyncMetaClient) -> None:
    await client.set("incr_init_ex", 5)
    assert await client.incr("incr_init_ex", initial=100, initial_ttl=60) == 6


@pytest.mark.asyncio
async def test_decr(client: AsyncMetaClient) -> None:
    await client.set("dctr", 10)
    assert await client.decr("dctr") == 9


@pytest.mark.asyncio
async def test_decr_with_delta(client: AsyncMetaClient) -> None:
    await client.set("dctr2", 10)
    assert await client.decr("dctr2", 3) == 7


@pytest.mark.asyncio
async def test_decr_floor_zero(client: AsyncMetaClient) -> None:
    await client.set("dctr3", 1)
    assert await client.decr("dctr3", 5) == 0


@pytest.mark.asyncio
async def test_decr_missing(client: AsyncMetaClient) -> None:
    with pytest.raises(MemcacheError):
        await client.decr("decr_miss")


@pytest.mark.asyncio
async def test_decr_with_initial(client: AsyncMetaClient) -> None:
    result = await client.decr("decr_init", initial=50, initial_ttl=60)
    # On miss with J flag, memcached creates item with initial value (delta not applied)
    assert result == 50


# ------------------------------------------------------------------ #
# flush_all                                                             #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_flush_all(client: AsyncMetaClient) -> None:
    await client.set("flush_k", "v")
    await client.flush_all()
    assert await client.get("flush_k") is None


# ------------------------------------------------------------------ #
# execute_meta_command (low-level pass-through)                         #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_execute_meta_command(client: AsyncMetaClient) -> None:
    cmd = MetaCommand(
        cm=b"ms", key=b"raw_key", datalen=3, flags=[b"T60"], value=b"raw"
    )
    result = await client.execute_meta_command(cmd)
    assert result.rc == b"HD"

    cmd2 = MetaCommand(cm=b"mg", key=b"raw_key", flags=[b"v"])
    result2 = await client.execute_meta_command(cmd2)
    assert result2.rc == b"VA"
    assert result2.value == b"raw"
