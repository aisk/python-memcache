"""A tour of MetaClient against a memcached started by this script.

Run it with a local memcached binary on PATH:

    $ python examples/meta_client_demo.py

Every section is self contained, so you can copy any one of them into your
own code. The only thing you need to replace is the address passed to the
client constructor.
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time
from contextlib import contextmanager
from collections.abc import Iterator

from memcache.experiment import (
    AlreadyExistsError,
    Arithmetic,
    ArithmeticResult,
    AsyncMetaClient,
    Delete,
    Get,
    GetResult,
    GetStatus,
    JsonSerializer,
    LeaseState,
    Meta,
    MetaClient,
    MutationStatus,
    NotFoundError,
    OperationFailedError,
    ResultValueError,
    Set,
)

# --------------------------------------------------------------------------
# Local server
# --------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def local_memcached() -> Iterator[tuple[str, int]]:
    """Start a throwaway memcached and yield its address."""
    binary = shutil.which("memcached")
    if binary is None:
        raise SystemExit(
            "memcached not found on PATH. Install it first:\n"
            "  Debian/Ubuntu: sudo apt install memcached\n"
            "  macOS:         brew install memcached\n"
            "  Docker:        docker run -p 11211:11211 memcached:1.6"
        )
    port = free_port()
    process = subprocess.Popen(
        [binary, "--listen=127.0.0.1", "--port=%d" % port, "--memory-limit=64"]
    )
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                if time.monotonic() > deadline or process.poll() is not None:
                    raise SystemExit("memcached failed to start on port %d" % port)
                time.sleep(0.05)
        print("memcached listening on 127.0.0.1:%d\n" % port)
        yield ("127.0.0.1", port)
    finally:
        process.terminate()
        process.wait(timeout=5)


def section(title: str) -> None:
    print("\n=== %s ===" % title)


# --------------------------------------------------------------------------
# Basics: set / get / delete
# --------------------------------------------------------------------------


def demo_basics(client: MetaClient) -> None:
    section("basics")

    # The default StrictSerializer stores bytes, int and str only. Leaving
    # ttl out stores the item with no expiry at all.
    client.set("greeting", "hello", ttl=60)
    client.set("visits", 41)
    client.set("blob", b"\x00\x01\x02")

    # An unconditional set has no semantic failure mode: it either stores or
    # never reaches the server, and the latter raises. So dropping the return
    # value here is safe. Conditional writes are different, see below.
    print("set returns:", client.set("greeting", "hello again", ttl=60).status)

    # value_or is the everyday read: one call for the value and its fallback.
    print("hit:", client.get("greeting").value_or("<default>"))
    print("miss:", client.get("nope").value_or("<default>"))

    # Every result also carries an explicit status when you need to branch,
    # and bool(result) is the happy path.
    result = client.get("greeting")
    print("status:", result.status, "truthy hit:", bool(result))
    print("miss status:", client.get("nope").status)

    # .value is the assertive read. Reach for it once you know it is a hit,
    # because on anything else it raises rather than inventing a value.
    try:
        client.get("nope").value
    except ResultValueError as error:
        print("reading .value on a miss raises:", error)

    deleted = client.delete("blob")
    print("delete:", deleted.status, "delete again:", client.delete("blob").status)


# --------------------------------------------------------------------------
# Item metadata
# --------------------------------------------------------------------------


def demo_metadata(client: MetaClient) -> None:
    section("metadata")

    client.set("doc", "body", ttl=120)

    # Ask for the extra fields you need; each flag is one wire flag.
    result = client.get("doc", meta=Meta.CAS | Meta.TTL | Meta.SIZE)
    print("cas:", result.item.cas, "ttl:", result.item.ttl, "size:", result.item.size)

    # inspect() reads metadata without transferring the value and without
    # bumping the LRU, which makes it safe for monitoring code.
    probe = client.inspect("doc", meta=Meta.TTL | Meta.SIZE | Meta.HIT_BEFORE)
    print("inspect ttl:", probe.item.ttl, "hit_before:", probe.item.hit_before)

    # touch() extends the TTL in place.
    touched = client.touch("doc", 600)
    print("touch:", touched.status, "-> ttl:", client.inspect("doc").item.ttl)


# --------------------------------------------------------------------------
# Conditional writes: add / cas / append / prepend
# --------------------------------------------------------------------------


def demo_conditional_writes(client: MetaClient) -> None:
    section("conditional writes")

    print("add new key:", client.add("user:1", "alice", ttl=60).status)
    print("add again:", client.add("user:1", "bob").status)

    current = client.get("user:1", meta=Meta.CAS)
    token = current.item.cas
    assert token is not None

    print("cas with fresh token:", client.cas("user:1", "alice2", token).status)

    # The stale token loses the race, which is the signal to re-read and retry.
    retry = client.cas("user:1", "alice3", token)
    print("cas with stale token:", retry.status)
    if retry.status is MutationStatus.CAS_MISMATCH:
        latest = client.get("user:1", meta=Meta.CAS)
        assert latest.item.cas is not None
        print("retried:", client.cas("user:1", "alice3", latest.item.cas).status)

    # Concatenation works on raw bytes and skips serialization entirely.
    client.set("log", b"first;")
    client.append("log", b"second;")
    client.prepend("log", b"zeroth;")
    print("concatenated:", client.get("log").value)

    # When "it must have worked" is the only acceptable outcome, say so with
    # check(). It returns the result, so it chains.
    print("checked add:", client.add("user:2", "carol", ttl=60).check().status)
    try:
        client.add("user:2", "dave").check()
    except AlreadyExistsError as error:
        print("checked add on a taken key raises:", error)


# --------------------------------------------------------------------------
# Counters
# --------------------------------------------------------------------------


def demo_counters(client: MetaClient) -> None:
    section("counters")

    # initial + initial_ttl vivify the counter on a miss, so no seeding write.
    # A vivified counter starts at `initial`; the delta only applies to the
    # calls that follow, so this prints 10 rather than 11.
    first = client.increment("hits", 1, initial=10, initial_ttl=60)
    print("vivified:", first.value, first.status)
    print("increment by 5:", client.increment("hits", 5).value)
    print("decrement by 3:", client.decrement("hits", 3, return_ttl=True).value)

    # Without initial, a missing counter is NOT_FOUND rather than an error.
    print("missing counter:", client.increment("absent").status)


# --------------------------------------------------------------------------
# unless_cas: skip the payload when nothing changed
# --------------------------------------------------------------------------


def demo_unless_cas(client: MetaClient) -> None:
    section("unless_cas")

    client.set("config", "v1")
    cached = client.get("config", meta=Meta.CAS)
    token = cached.item.cas
    assert token is not None

    again = client.get("config", unless_cas=token)
    print("unchanged:", again.status is GetStatus.UNCHANGED)

    client.set("config", "v2")
    changed = client.get("config", unless_cas=token)
    print("changed:", changed.status, changed.value)


# --------------------------------------------------------------------------
# Leases: stampede protection on a miss
# --------------------------------------------------------------------------


def build_report() -> str:
    return "expensive report"


def demo_lease(client: MetaClient) -> None:
    section("lease on miss")

    # The first caller wins the lease and gets a MISS it is expected to fill;
    # concurrent callers see BUSY and should back off or serve something else.
    winner = client.get_with_lease("report", lease_ttl=30)
    loser = client.get_with_lease("report", lease_ttl=30)
    print("winner:", winner.status, winner.lease_state)
    print("loser:", loser.status, loser.lease_state)

    if winner.lease_state is LeaseState.GRANTED:
        stored = winner.fulfill(build_report(), ttl=300)
        print("fulfill:", stored.status)

    print("after fulfill:", client.get("report").value)


def demo_recache(client: MetaClient) -> None:
    section("early recache")

    client.set("feed", "cached-body", ttl=5)

    # refresh_before asks the server to elect one caller to refresh the value
    # before it expires. Everyone else keeps serving the still valid value.
    # get_with_lease() is the statically typed door to the same feature;
    # client.get(..., lease_ttl=...) returns the same object at runtime.
    early = client.get_with_lease("feed", lease_ttl=30, refresh_before=10)
    other = client.get_with_lease("feed", lease_ttl=30, refresh_before=10)
    print("elected:", early.won_recache, "value:", early.value)
    print("other caller:", other.won_recache, "value:", other.value)

    if early.won_recache:
        print("refreshed:", early.fulfill("fresh-body", ttl=300).status)


def demo_invalidate(client: MetaClient) -> None:
    section("invalidate")

    client.set("page", "html", ttl=300)

    # Unlike delete, invalidate keeps the item around but marks it stale, so
    # readers can serve stale data while one of them recomputes.
    print("invalidate:", client.invalidate("page", stale_for=60).status)

    stale = client.get("page")
    print("is_stale:", stale.is_stale, "won_recache:", stale.won_recache)
    print("stale value still readable:", stale.value)
    print("second reader won_recache:", client.get("page").won_recache)


# --------------------------------------------------------------------------
# Batch: one round trip per server, input order preserved
# --------------------------------------------------------------------------


def demo_batch(client: MetaClient) -> None:
    section("batch")

    client.set("b:1", "one")
    client.set("b:2", "two")

    results = client.batch(
        [
            Get("b:1", meta=Meta.CAS),
            Get("b:missing"),
            Set("b:3", "three", ttl=60),
            Arithmetic("b:counter", 2, initial=0, initial_ttl=60),
            Delete("b:2"),
        ]
    )

    # Results come back in input order, one per operation, as the union of
    # result types. isinstance narrows them when you need the typed fields.
    hit, miss, stored, counter, deleted = results
    assert isinstance(hit, GetResult) and isinstance(counter, ArithmeticResult)
    print("get hit:", hit.status, hit.value, "cas:", hit.item.cas)
    print("get miss:", miss.status)
    print("set:", stored.status)
    print("arithmetic:", counter.status, counter.value)
    print("delete:", deleted.status)

    # Multi get is just a batch of Get operations.
    keys = ["b:1", "b:3", "b:2"]
    found = {
        key: result.value
        for key, result in zip(keys, client.batch([Get(key) for key in keys]))
        if isinstance(result, GetResult) and result.status is GetStatus.HIT
    }
    print("multi get:", found)


# --------------------------------------------------------------------------
# Failures
# --------------------------------------------------------------------------


def demo_failures(client: MetaClient) -> None:
    section("failures")

    # A single operation has nowhere to put "I never got an answer", so
    # infrastructure trouble leaves through the exception channel instead of
    # a status you could forget to read. Nothing here is silent.
    with MetaClient(("127.0.0.1", 1), timeout=0.2) as unreachable:
        try:
            unreachable.set("key", "value")
        except OperationFailedError as error:
            print("write to a dead server raises:", error)
            print("  cause:", type(error.__cause__).__name__)

    # Semantic outcomes are answers, so they stay in the status and only
    # raise where you ask them to.
    print("miss is not a failure:", client.get("absent").status)
    try:
        client.get("absent").check()
    except NotFoundError as error:
        print("until you check() it:", error)

    # A batch does have somewhere to put per-operation failure, so it keeps
    # reporting it as a status and lets one bad server spoil only its own
    # operations. raise_for_failures() opts the whole batch into raising.
    results = client.batch([Get("absent"), Set("present", "v")])
    print("failures in a healthy batch:", results.failures)
    print("raise_for_failures returns:", type(results.raise_for_failures()).__name__)


# --------------------------------------------------------------------------
# Serializers
# --------------------------------------------------------------------------


def demo_serializer(addr: tuple[str, int]) -> None:
    section("serializers")

    # StrictSerializer is the default and refuses anything but bytes/int/str.
    with MetaClient(addr) as strict:
        try:
            strict.set("obj", {"a": 1})
        except TypeError as error:
            print("strict serializer:", error)

    with MetaClient(addr, serializer=JsonSerializer()) as client:
        client.set("obj", {"a": 1, "b": [2, 3]}, ttl=60)
        print("json round trip:", client.get("obj").value)


# --------------------------------------------------------------------------
# Raw meta commands
# --------------------------------------------------------------------------


def demo_meta_namespace(client: MetaClient) -> None:
    section("client.meta (raw protocol)")

    # client.meta maps 1:1 to the wire commands and works on raw bytes: no
    # serialization, no status mapping, just lightly parsed responses.
    stored = client.meta.set("raw", b"payload", ttl=60, return_cas=True)
    print("ms ->", stored.rc, "cas:", stored.cas)

    got = client.meta.get("raw", return_cas=True, return_ttl=True)
    print("mg ->", got.rc, got.value, "cas:", got.cas, "ttl:", got.ttl)

    counter = client.meta.arithmetic("raw:counter", delta=2, initial=0, initial_ttl=60)
    print("ma ->", counter.rc, counter.value)
    print("me ->", client.meta.debug("raw"))
    print("md ->", client.meta.delete("raw").rc)

    # execute() is the escape hatch for flags the typed surface does not cover.
    raw = client.meta.execute(command="mg", key="raw:counter", flags=[b"v", b"t"])
    print("execute ->", raw.rc, raw.flags, raw.value)


# --------------------------------------------------------------------------
# Async client
# --------------------------------------------------------------------------


async def demo_async(addr: tuple[str, int]) -> None:
    section("async client")

    async with AsyncMetaClient(addr) as client:
        await client.set("async:key", "value", ttl=60)
        result = await client.get("async:key", meta=Meta.CAS)
        print("get:", result.status, result.value, "cas:", result.item.cas)

        results = await client.batch([Get("async:key"), Set("async:other", 1)])
        print("batch:", [item.status for item in results])

        lease = await client.get_with_lease("async:report", lease_ttl=30)
        if lease.lease_state is LeaseState.GRANTED:
            print("fulfill:", (await lease.fulfill("built", ttl=60)).status)
        print("after fulfill:", (await client.get("async:report")).value)


# --------------------------------------------------------------------------


def main() -> None:
    with local_memcached() as addr:
        with MetaClient(addr, timeout=1.0) as client:
            demo_basics(client)
            demo_metadata(client)
            demo_conditional_writes(client)
            demo_counters(client)
            demo_unless_cas(client)
            demo_lease(client)
            demo_recache(client)
            demo_invalidate(client)
            demo_batch(client)
            demo_failures(client)
            demo_meta_namespace(client)

            section("housekeeping")
            print("stored keys before flush:", client.get("greeting").status)
            client.flush_all()
            print("after flush_all:", client.get("greeting").status)

        demo_serializer(addr)
        asyncio.run(demo_async(addr))


if __name__ == "__main__":
    main()
