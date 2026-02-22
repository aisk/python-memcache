import time

import pytest

from memcache.experiment import GetResult, MetaClient
from memcache import MemcacheError
from memcache.meta_command import MetaCommand, MetaResult


@pytest.fixture()
def client() -> MetaClient:
    c = MetaClient(("localhost", 11211))
    c.flush_all()
    return c


# ------------------------------------------------------------------ #
# Bug fix: load_header correctly parses flags after datalen          #
# ------------------------------------------------------------------ #


def test_load_header_with_flags_after_datalen() -> None:
    # HD with flag containing non-digit prefix should NOT be parsed as datalen
    result = MetaResult.load_header(b"HD t20 c123\r\n")
    assert result.rc == b"HD"
    assert result.datalen is None
    assert b"t20" in result.flags
    assert b"c123" in result.flags


def test_load_header_va_with_datalen_and_flags() -> None:
    result = MetaResult.load_header(b"VA 5 f16 c999\r\n")
    assert result.rc == b"VA"
    assert result.datalen == 5
    assert b"f16" in result.flags
    assert b"c999" in result.flags


def test_load_header_en() -> None:
    result = MetaResult.load_header(b"EN\r\n")
    assert result.rc == b"EN"
    assert result.datalen is None
    assert result.flags == []


# ------------------------------------------------------------------ #
# Basic get / set                                                    #
# ------------------------------------------------------------------ #


def test_set_get(client: MetaClient) -> None:
    client.set("key1", "hello")
    assert client.get("key1") == "hello"


def test_get_missing(client: MetaClient) -> None:
    assert client.get("no_such_key") is None


def test_set_get_with_expire(client: MetaClient) -> None:
    client.set("expkey", "val", expire=1)
    assert client.get("expkey") == "val"
    time.sleep(1.1)
    assert client.get("expkey") is None


def test_get_no_lru_bump(client: MetaClient) -> None:
    client.set("nlb", "v")
    assert client.get("nlb", no_lru_bump=True) == "v"


def test_get_update_ttl(client: MetaClient) -> None:
    client.set("utt", "v", expire=10)
    assert client.get("utt", update_ttl=3600) == "v"


# ------------------------------------------------------------------ #
# get_result                                                         #
# ------------------------------------------------------------------ #


def test_get_result_miss(client: MetaClient) -> None:
    assert client.get_result("missing_key") is None


def test_get_result_basic(client: MetaClient) -> None:
    client.set("gr_key", "world")
    r = client.get_result("gr_key")
    assert r is not None
    assert r.value == "world"


def test_get_result_return_cas(client: MetaClient) -> None:
    client.set("gr_cas", "v")
    r = client.get_result("gr_cas", return_cas=True)
    assert r is not None
    assert isinstance(r.cas_token, int)
    assert r.cas_token > 0


def test_get_result_return_ttl(client: MetaClient) -> None:
    client.set("gr_ttl", "v", expire=3600)
    r = client.get_result("gr_ttl", return_ttl=True)
    assert r is not None
    assert r.ttl is not None
    assert r.ttl > 0


def test_get_result_no_ttl_requested(client: MetaClient) -> None:
    client.set("gr_nottl", "v")
    r = client.get_result("gr_nottl")
    assert r is not None
    assert r.ttl is None


def test_get_result_return_size(client: MetaClient) -> None:
    client.set("gr_size", b"hello")
    r = client.get_result("gr_size", return_size=True)
    assert r is not None
    assert r.size == 5


def test_get_result_return_hit_before(client: MetaClient) -> None:
    client.set("gr_hit", "v")
    # First get: not hit before
    r1 = client.get_result("gr_hit", return_hit_before=True)
    assert r1 is not None
    assert r1.hit_before is False
    # Second get: hit before
    r2 = client.get_result("gr_hit", return_hit_before=True)
    assert r2 is not None
    assert r2.hit_before is True


def test_get_result_return_last_access(client: MetaClient) -> None:
    client.set("gr_la", "v")
    r = client.get_result("gr_la", return_last_access=True)
    assert r is not None
    assert r.last_access is not None
    assert isinstance(r.last_access, int)


def test_get_result_no_lru_bump(client: MetaClient) -> None:
    client.set("gr_nlb", "v")
    r = client.get_result("gr_nlb", no_lru_bump=True)
    assert r is not None
    assert r.value == "v"


def test_get_result_returns_getresult_instance(client: MetaClient) -> None:
    client.set("gr_type", 42)
    r = client.get_result("gr_type")
    assert isinstance(r, GetResult)
    assert r.value == 42


# ------------------------------------------------------------------ #
# gat / touch                                                        #
# ------------------------------------------------------------------ #


def test_gat(client: MetaClient) -> None:
    client.set("gat_key", "v", expire=1)
    result = client.gat("gat_key", expire=3600)
    assert result == "v"
    # After gat with long TTL, key should still exist past original TTL
    time.sleep(1.1)
    assert client.get("gat_key") == "v"


def test_gat_miss(client: MetaClient) -> None:
    assert client.gat("no_gat_key", expire=60) is None


def test_touch_existing(client: MetaClient) -> None:
    client.set("touch_key", "v", expire=1)
    assert client.touch("touch_key", expire=3600) is True
    time.sleep(1.1)
    assert client.get("touch_key") == "v"


def test_touch_missing(client: MetaClient) -> None:
    assert client.touch("no_touch_key", expire=60) is False


# ------------------------------------------------------------------ #
# get_many                                                           #
# ------------------------------------------------------------------ #


def test_get_many(client: MetaClient) -> None:
    client.set("m1", "a")
    client.set("m2", "b")
    result = client.get_many(["m1", "m2", "m_miss"])
    assert result == {"m1": "a", "m2": "b"}


def test_get_many_all_miss(client: MetaClient) -> None:
    assert client.get_many(["x1", "x2"]) == {}


# ------------------------------------------------------------------ #
# add / replace                                                      #
# ------------------------------------------------------------------ #


def test_add_new_key(client: MetaClient) -> None:
    assert client.add("add_new", "v") is True
    assert client.get("add_new") == "v"


def test_add_existing_key(client: MetaClient) -> None:
    client.set("add_exists", "original")
    assert client.add("add_exists", "new") is False
    assert client.get("add_exists") == "original"


def test_replace_existing(client: MetaClient) -> None:
    client.set("rep_key", "old")
    assert client.replace("rep_key", "new") is True
    assert client.get("rep_key") == "new"


def test_replace_missing(client: MetaClient) -> None:
    assert client.replace("rep_miss", "v") is False


# ------------------------------------------------------------------ #
# append / prepend                                                   #
# ------------------------------------------------------------------ #


def test_append(client: MetaClient) -> None:
    client.set("app_key", b"hello")
    assert client.append("app_key", b" world") is True
    assert client.get("app_key") == b"hello world"


def test_append_missing_key(client: MetaClient) -> None:
    assert client.append("app_miss", b"v") is False


def test_append_vivify(client: MetaClient) -> None:
    assert client.append("app_vivify", b"data", vivify_ttl=60) is True
    assert client.get("app_vivify") == b"data"


def test_prepend(client: MetaClient) -> None:
    client.set("pre_key", b"world")
    assert client.prepend("pre_key", b"hello ") is True
    assert client.get("pre_key") == b"hello world"


def test_prepend_missing_key(client: MetaClient) -> None:
    assert client.prepend("pre_miss", b"v") is False


def test_prepend_vivify(client: MetaClient) -> None:
    assert client.prepend("pre_vivify", b"data", vivify_ttl=60) is True
    assert client.get("pre_vivify") == b"data"


# ------------------------------------------------------------------ #
# cas                                                                #
# ------------------------------------------------------------------ #


def test_cas_success(client: MetaClient) -> None:
    client.set("cas_key", "initial")
    r = client.get_result("cas_key", return_cas=True)
    assert r is not None
    assert r.cas_token is not None
    assert client.cas("cas_key", "updated", r.cas_token) is True
    assert client.get("cas_key") == "updated"


def test_cas_conflict(client: MetaClient) -> None:
    client.set("cas_conf", "v")
    r = client.get_result("cas_conf", return_cas=True)
    assert r is not None
    assert r.cas_token is not None
    client.set("cas_conf", "modified")
    assert client.cas("cas_conf", "new", r.cas_token) is False
    assert client.get("cas_conf") == "modified"


def test_cas_missing_key(client: MetaClient) -> None:
    assert client.cas("cas_no_key", "v", 12345) is False


# ------------------------------------------------------------------ #
# delete                                                             #
# ------------------------------------------------------------------ #


def test_delete_existing(client: MetaClient) -> None:
    client.set("del_key", "v")
    assert client.delete("del_key") is True
    assert client.get("del_key") is None


def test_delete_missing(client: MetaClient) -> None:
    assert client.delete("del_miss") is False


def test_delete_with_cas(client: MetaClient) -> None:
    client.set("del_cas", "v")
    r = client.get_result("del_cas", return_cas=True)
    assert r is not None
    assert r.cas_token is not None
    assert client.delete("del_cas", cas_token=r.cas_token) is True


def test_delete_with_wrong_cas(client: MetaClient) -> None:
    client.set("del_cas_bad", "v")
    assert client.delete("del_cas_bad", cas_token=99999999) is False


# ------------------------------------------------------------------ #
# invalidate                                                         #
# ------------------------------------------------------------------ #


def test_invalidate(client: MetaClient) -> None:
    client.set("inv_key", "v")
    assert client.invalidate("inv_key") is True


def test_invalidate_missing(client: MetaClient) -> None:
    assert client.invalidate("inv_miss") is False


def test_invalidate_stale_flag(client: MetaClient) -> None:
    client.set("inv_stale", "v")
    client.invalidate("inv_stale", stale_ttl=10)
    # After invalidation, get_result should see is_stale=True
    r = client.get_result("inv_stale")
    assert r is not None
    assert r.is_stale is True


# ------------------------------------------------------------------ #
# incr / decr                                                        #
# ------------------------------------------------------------------ #


def test_incr(client: MetaClient) -> None:
    client.set("ctr", 10)
    assert client.incr("ctr") == 11


def test_incr_with_delta(client: MetaClient) -> None:
    client.set("ctr2", 5)
    assert client.incr("ctr2", 3) == 8


def test_incr_missing(client: MetaClient) -> None:
    with pytest.raises(MemcacheError):
        client.incr("incr_miss")


def test_incr_with_initial(client: MetaClient) -> None:
    # On miss with J flag, memcached creates item with initial value (delta not applied)
    assert client.incr("incr_init", initial=100, initial_ttl=60) == 100


def test_incr_with_initial_existing(client: MetaClient) -> None:
    client.set("incr_init_ex", 5)
    # initial is ignored when key exists
    assert client.incr("incr_init_ex", initial=100, initial_ttl=60) == 6


def test_decr(client: MetaClient) -> None:
    client.set("dctr", 10)
    assert client.decr("dctr") == 9


def test_decr_with_delta(client: MetaClient) -> None:
    client.set("dctr2", 10)
    assert client.decr("dctr2", 3) == 7


def test_decr_floor_zero(client: MetaClient) -> None:
    client.set("dctr3", 1)
    assert client.decr("dctr3", 5) == 0


def test_decr_missing(client: MetaClient) -> None:
    with pytest.raises(MemcacheError):
        client.decr("decr_miss")


def test_decr_with_initial(client: MetaClient) -> None:
    result = client.decr("decr_init", initial=50, initial_ttl=60)
    # On miss with J flag, memcached creates item with initial value (delta not applied)
    assert result == 50


# ------------------------------------------------------------------ #
# flush_all                                                          #
# ------------------------------------------------------------------ #


def test_flush_all(client: MetaClient) -> None:
    client.set("flush_k", "v")
    client.flush_all()
    assert client.get("flush_k") is None


def test_flush_all_with_delay(client: MetaClient) -> None:
    client.set("flush_d", "v")
    client.flush_all(delay=0)
    assert client.get("flush_d") is None


# ------------------------------------------------------------------ #
# execute_meta_command (low-level pass-through)                      #
# ------------------------------------------------------------------ #


def test_execute_meta_command(client: MetaClient) -> None:
    cmd = MetaCommand(cm=b"ms", key=b"raw_key", datalen=3, flags=[b"T60"], value=b"raw")
    result = client.execute_meta_command(cmd)
    assert result.rc == b"HD"

    cmd2 = MetaCommand(cm=b"mg", key=b"raw_key", flags=[b"v"])
    result2 = client.execute_meta_command(cmd2)
    assert result2.rc == b"VA"
    assert result2.value == b"raw"
