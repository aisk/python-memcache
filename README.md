# memcache

Memcached client library for Python.

Key features:

- Based on memcached's new meta commands;
- Synchronous and asynchronous APIs;
- Asyncio and Trio support (via anyio);
- Type hints.

## Installation

```sh
$ pip install memcache
```

## Usage

### Basic Usage

```python
import memcache

client = memcache.Memcache(("localhost", 11211))

client.set("key", "value", expire=60)
value = client.get("key")
client.delete("key")

# Atomic counters
client.set("counter", 0)
client.incr("counter")       # 1
client.incr("counter", 5)    # 6
client.decr("counter", 2)    # 4

# Compare-and-swap
value, token = client.gets("key")
client.cas("key", "new_value", token)
```

Async usage mirrors the sync API with `AsyncMemcache` and `await`.

### Scenario client (Experimental)

> **Experimental.** The scenario client lives under `memcache.experiment` and its API
> may change in any minor release. If you depend on it, pin the **minor version**
> in your dependency spec. Patch releases (`x.y.Z`) will not introduce breaking
> changes, but minor releases (`x.Y.0`) might.
>
> **requirements.txt**
> ```
> memcache~=0.14.0   # allows 0.14.x, blocks 0.15+
> ```
>
> **pyproject.toml**
> ```toml
> [project]
> dependencies = [
>     "memcache>=0.14.0,<0.15",
> ]
> ```

`memcache.experiment.Memcache` is a scenario-level client built on memcached's meta protocol: one method per usage scenario, business values in and out. Reads answer a miss with a default instead of an error, failures leave through exceptions, and protocol concepts (CAS tokens, lease flags, stale markers) never appear in your code. Policies such as the serializer, a key prefix, and the failure behavior live in the constructor; every write states its lifetime (`FOREVER` for no expiry).

```python
from memcache.experiment import Memcache, JsonSerializer, FOREVER

cache = Memcache(("localhost", 11211), serializer=JsonSerializer())

# Object cache: a miss is a normal answer.
cache.set("user:1", {"name": "alice"}, ttl=600)
user = cache.get("user:1")
cache.delete("user:1")

# Compute on miss, stampede-safe: one winner recomputes per key while other
# readers share its result; refresh_ahead renews hot keys before they expire.
report = cache.get("report:q3", factory=build_report, ttl=3600, refresh_ahead=300)

# Soft invalidation: readers keep the old copy for the grace window while one
# factory reader is elected to recompute. Hard delete is a full miss.
cache.delete("article:7", grace=60)

# Atomic read-modify-write: version handling and conflict retries live in the
# library; fn must be pure because it can run more than once.
cache.update("cart:42", lambda cart: cart + [item], default=[], ttl=1800)

# Counters count from zero on a miss; ttl applies on create only, which is
# exactly a fixed-window rate limit.
if cache.incr(f"rate:{ip}", ttl=60) > 100:
    raise TooManyRequests

# Claim once, renew sessions, buffer events, take atomically.
cache.add("job:daily:2026-08-17", "1", ttl=86400)   # -> bool, True when claimed
cache.get("session:abc", extend_ttl=1800)           # read and slide expiry
cache.replace("session:abc", session, ttl=1800)     # write back, never resurrects
cache.append("events:1", b"login;", ttl=86400)      # byte-stream buffer
buffered = cache.pop("events:1")                    # atomic take and delete

# Independent operations in one round trip per server.
with cache.pipeline() as p:
    user = p.get("user:1")
    hits = p.incr(f"rate:{ip}", ttl=60)
    p.touch("session:abc", 1800)
print(user.value, hits.value)

# Non-intrusive probe for debugging: no value transfer, no LRU bump.
info = cache.inspect("report:q3")   # -> ItemInfo(ttl, size, last_access, hit_before) | None
```

`AsyncMemcache` is the same table of verbs plus `await`; `factory` and `fn` accept sync or async callables. Used as an async context manager, refresh-ahead and stale-grace recomputations run as background tasks owned by the client, so no request pays the refresh latency.

Failure behavior is a constructor policy, not a per-call decision. The default `on_error="raise"` surfaces infrastructure trouble as `OperationFailedError` (a sent-but-unacknowledged write raises `AmbiguousWriteError`, and that never degrades). `on_error="degrade"` decouples a cache outage from a site outage: reads become misses, a `get` with a factory computes locally, blind writes are dropped silently, while operations whose answer feeds business decisions (`add`, `replace`, `incr`, `update`, `pop`) still raise. Every absorbed failure goes to the `on_failure` hook (standard logging by default), so degrading business behavior never degrades observability.

For protocol experts, `cache.meta` maps the wire commands one-to-one (`get`/`set`/`delete`/`arithmetic`/`debug`, i.e. `mg`/`ms`/`md`/`ma`/`me`) with one keyword argument per protocol flag. It works on raw bytes and returns lightly parsed responses without serialization or semantic mapping:

```python
    stored = cache.meta.set("key", b"payload", ttl=60, return_cas=True, opaque=b"req1")
    got = cache.meta.get("key", return_cas=True, return_ttl=True)
    assert got.rc == b"VA" and got.cas == stored.cas

    # Framing-safe bytes-level escape hatch for anything not covered above.
    cache.meta.execute(command="mg", key="key", flags=[b"v", b"t"])
```

See `examples/scenario_demo.py` for a runnable tour of every scenario, and `docs/design-scenario-api.md` for the design rationale.

## About the Project

Memcache is &copy; 2020-2025 by [aisk](https://github.com/aisk).

### License

Memcache is distributed by a [MIT license](https://github.com/aisk/memcache/tree/master/LICENSE).
