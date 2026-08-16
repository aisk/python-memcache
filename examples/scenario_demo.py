"""A tour of the scenario-level client against a throwaway memcached.

Run it with a local memcached binary on PATH:

    $ python examples/scenario_demo.py

Every section maps to one usage scenario and is self contained, so you can
copy any one of them into your own code. The only thing you need to replace
is the address passed to the client constructor.
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
    FOREVER,
    AsyncMemcache,
    JsonSerializer,
    Memcache,
    NotFoundError,
    OperationFailedError,
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
# Object cache: the everyday read-aside pattern
# --------------------------------------------------------------------------


def demo_object_cache(cache: Memcache) -> None:
    section("object cache")

    # A miss is a normal answer, not an error, so reads never need try/except.
    # Every write states its lifetime; FOREVER (0) is the explicit way to
    # store something that must not expire.
    cache.set("user:1", {"name": "alice"}, ttl=600)
    print("hit:", cache.get("user:1"))
    print("miss:", cache.get("user:2", default={"name": "guest"}))
    cache.set("pinned", "value", ttl=FOREVER)

    # delete reports whether it erased something; most callers ignore it.
    print("delete:", cache.delete("user:1"), "again:", cache.delete("user:1"))


# --------------------------------------------------------------------------
# Page aggregation: one round trip per server for many keys
# --------------------------------------------------------------------------


def demo_aggregation(cache: Memcache) -> None:
    section("aggregation")

    cache.set_many({"user:1": "alice", "user:2": "bob"}, ttl=600)
    found = cache.get_many(["user:1", "user:2", "user:3"])
    print("hits only, keyed by your keys:", found)
    cache.delete_many(["user:1", "user:2"])


# --------------------------------------------------------------------------
# Expensive computation: get with a factory is stampede-safe
# --------------------------------------------------------------------------


def demo_factory(cache: Memcache) -> None:
    section("factory (stampede-safe compute on miss)")

    calls = []

    def build_report() -> str:
        calls.append(1)
        time.sleep(0.05)  # pretend this is expensive
        return "quarterly numbers"

    # On a miss one caller is elected (server-side, across processes) to run
    # the factory; everyone else shares the result. Subsequent calls hit.
    print("first:", cache.get("report:q3", factory=build_report, ttl=3600))
    print("second:", cache.get("report:q3", factory=build_report, ttl=3600))
    print("factory ran %d time(s)" % len(calls))

    # refresh_ahead recomputes shortly before expiry so hot keys never
    # actually expire; readers keep getting the current value meanwhile.
    cache.get("home:feed", factory=lambda: "feed", ttl=300, refresh_ahead=30)


# --------------------------------------------------------------------------
# Content invalidation: hard and soft
# --------------------------------------------------------------------------


def demo_invalidation(cache: Memcache) -> None:
    section("invalidation")

    cache.set("article:7", "v1", ttl=3600)

    # Soft invalidation: for the grace window plain readers keep the old
    # copy, while one factory reader is elected to recompute. No miss storm
    # on every edit.
    cache.delete("article:7", grace=60)
    print("plain reader during grace:", cache.get("article:7"))
    print(
        "factory reader refreshes:",
        cache.get("article:7", factory=lambda: "v2", ttl=3600),
    )

    # Hard invalidation: the next reader pays a full miss. Use this when the
    # old data must not appear again (logout, revocation).
    cache.delete("article:7")
    print("after hard delete:", cache.get("article:7"))


# --------------------------------------------------------------------------
# Concurrent mutation: update owns the read-modify-write loop
# --------------------------------------------------------------------------


def demo_update(cache: Memcache) -> None:
    section("update (atomic read-modify-write)")

    # The library reads with a version, runs your pure function, and writes
    # back only if nothing changed in between, retrying on conflict. Version
    # tokens never appear in your code.
    print(
        "miss starts from default:",
        cache.update("cart:42", lambda c: c + ["shoes"], default=[], ttl=1800),
    )
    print("hit transforms:", cache.update("cart:42", lambda c: c + ["socks"], ttl=1800))

    try:
        cache.update("cart:absent", lambda c: c, ttl=1800)
    except NotFoundError as error:
        print("no default on a miss raises:", error)


# --------------------------------------------------------------------------
# Counters and rate limiting
# --------------------------------------------------------------------------


def demo_counters(cache: Memcache) -> None:
    section("counters")

    # A miss counts from zero, atomically, in one round trip. The ttl applies
    # only when the counter is created, which is exactly fixed-window rate
    # limiting; later increments never extend the window.
    print("rate:", cache.incr("rate:1.2.3.4", ttl=60))
    print("rate:", cache.incr("rate:1.2.3.4", ttl=60))
    print("decr saturates at zero:", cache.decr("rate:1.2.3.4", 10, ttl=60))


# --------------------------------------------------------------------------
# Claiming: single-instance jobs
# --------------------------------------------------------------------------


def demo_claim(cache: Memcache) -> None:
    section("add (claim once)")

    if cache.add("job:daily-report:2026-08-17", "1", ttl=86400):
        print("this instance runs the job")
    print("second claim:", cache.add("job:daily-report:2026-08-17", "1", ttl=86400))


# --------------------------------------------------------------------------
# Sessions: renew on read, write back without resurrecting
# --------------------------------------------------------------------------


def demo_sessions(cache: Memcache) -> None:
    section("sessions")

    cache.set("session:abc", {"uid": 1}, ttl=1800)

    # Read and slide the expiry in one round trip.
    print("read+renew:", cache.get("session:abc", extend_ttl=1800))

    # Write back only while the session still exists; a logout in between
    # must not be resurrected by the write.
    print("replace:", cache.replace("session:abc", {"uid": 1, "cart": 3}, ttl=1800))
    cache.delete("session:abc")
    print("replace after logout:", cache.replace("session:abc", {"uid": 1}, ttl=1800))

    # touch renews without transferring the value: for big payloads.
    cache.set("render:home", "x" * 1000, ttl=600)
    print("touch:", cache.touch("render:home", 3600))


# --------------------------------------------------------------------------
# Event buffers: byte streams with an atomic take
# --------------------------------------------------------------------------


def demo_buffers(cache: Memcache) -> None:
    section("event buffers")

    # append/prepend take bytes or str, bypass the serializer, and create the
    # buffer on a miss; the ttl only applies at creation (a rolling window).
    cache.append("events:1", b"login;", ttl=86400)
    cache.append("events:1", b"click;", ttl=86400)

    # pop takes the whole buffer and deletes it atomically: bytes appended
    # between the read and the delete are never lost.
    print("pop:", cache.pop("events:1"))
    print("pop again:", cache.pop("events:1", default=b""))


# --------------------------------------------------------------------------
# Request prelude: independent operations in one round trip
# --------------------------------------------------------------------------


def demo_pipeline(cache: Memcache) -> None:
    section("pipeline")

    cache.set("user:9", "alice", ttl=600)
    with cache.pipeline() as p:
        user = p.get("user:9")
        hits = p.incr("rate:9", ttl=60)
        p.touch("session:9", 1800)
    # Deferred results are readable once the with block exits.
    print("user:", user.value, "hits:", hits.value)


# --------------------------------------------------------------------------
# Observation: a probe that does not disturb what it measures
# --------------------------------------------------------------------------


def demo_inspect(cache: Memcache) -> None:
    section("inspect")

    cache.set("probe", "payload", ttl=300)
    info = cache.inspect("probe")
    assert info is not None
    print("ttl:", info.ttl, "size:", info.size, "hit_before:", info.hit_before)
    print("miss:", cache.inspect("absent"))


# --------------------------------------------------------------------------
# Failure policy: explicit raising or explicit degrading
# --------------------------------------------------------------------------


def demo_failure_policy() -> None:
    section("failure policy")

    # Default: infrastructure failures raise, loudly.
    with Memcache(("127.0.0.1", 1), timeout=0.2) as unreachable:
        try:
            unreachable.get("key")
        except OperationFailedError as error:
            print("raise mode:", error)

    # Degrade: a cache outage is not a site outage. Reads become misses,
    # blind writes are dropped, and every absorbed failure goes to the
    # on_failure hook. Operations whose answer feeds business decisions
    # (add, incr, update, pop) still raise: inventing an answer is worse.
    failures: list[BaseException] = []
    with Memcache(
        ("127.0.0.1", 1),
        on_error="degrade",
        on_failure=failures.append,
        timeout=0.2,
    ) as degraded:
        print("degraded get:", degraded.get("key", default="fallback"))
        print("degraded factory:", degraded.get("k", factory=lambda: "local", ttl=60))
        print("absorbed failures:", len(failures))


# --------------------------------------------------------------------------
# Escape hatch: the raw meta protocol surface
# --------------------------------------------------------------------------


def demo_meta_namespace(cache: Memcache) -> None:
    section("cache.meta (raw protocol)")

    # cache.meta maps 1:1 to the wire commands (mg/ms/md/ma/me) and works on
    # raw bytes: no serialization, no semantic mapping. Anything the scenario
    # layer does not cover lives here.
    stored = cache.meta.set("raw", b"payload", ttl=60, return_cas=True)
    print("ms ->", stored.rc, "cas:", stored.cas)
    got = cache.meta.get("raw", return_cas=True, return_ttl=True)
    print("mg ->", got.rc, got.value, "ttl:", got.ttl)

    # execute() is the flag-level escape hatch.
    raw = cache.meta.execute(command="mg", key="raw", flags=[b"v", b"t"])
    print("execute ->", raw.rc, raw.flags, raw.value)


# --------------------------------------------------------------------------
# Async client: the same table plus await
# --------------------------------------------------------------------------


async def demo_async(addr: tuple[str, int]) -> None:
    section("async client")

    # Used as a context manager, the client owns a task group: refresh-ahead
    # and stale-grace recomputations run as background tasks, so no request
    # ever pays the refresh latency. Factories may be sync or async.
    async with AsyncMemcache(addr, serializer=JsonSerializer()) as cache:
        await cache.set("async:user", {"uid": 1}, ttl=600)
        print("get:", await cache.get("async:user"))

        async def build() -> str:
            await asyncio.sleep(0.01)
            return "computed"

        print("factory:", await cache.get("async:report", factory=build, ttl=60))

        async with cache.pipeline() as p:
            user = p.get("async:user")
            hits = p.incr("async:rate", ttl=60)
        print("pipeline:", user.value, hits.value)


# --------------------------------------------------------------------------


def main() -> None:
    with local_memcached() as addr:
        with Memcache(addr, serializer=JsonSerializer()) as cache:
            demo_object_cache(cache)
            demo_aggregation(cache)
            demo_factory(cache)
            demo_invalidation(cache)
            demo_update(cache)
            demo_counters(cache)
            demo_claim(cache)
            demo_sessions(cache)
            demo_buffers(cache)
            demo_pipeline(cache)
            demo_inspect(cache)
            demo_meta_namespace(cache)
        demo_failure_policy()
        asyncio.run(demo_async(addr))


if __name__ == "__main__":
    main()
