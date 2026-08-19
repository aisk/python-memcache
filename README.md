# memcache

English | [简体中文](README.zh-CN.md)

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

## Basic Usage

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

## Scenario client (Experimental)

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

`memcache.experiment.Memcache` hides the wire protocol behind verbs named for what you are doing. The meta protocol's CAS tokens and leases never surface in caller code. Instead of reading a version and writing it back, call `update` with a transform function and the client runs the read, compare and swap, retry loop internally. Instead of building dogpile protection, call `get` with a `factory` and the client makes sure the value is computed once. When you do need the raw protocol, every meta command is still reachable through `cache.meta`.

### Conventions

- A miss is a normal answer, not an error. Reads return `default` (usually `None`) on a miss; exceptions are reserved for infrastructure failure. The two never mix.
- Values are business objects. The serializer is a constructor policy. The default `StrictSerializer` stores only bytes, int and str; `PickleSerializer` and `JsonSerializer` handle arbitrary objects, and `CompressedSerializer` wraps any of them.
- Keys are str or bytes, and both spellings name the same item. The constructor `prefix` namespaces every key, which also makes it a whole-cache version switch.
- Every method that stores a value takes a required `ttl`, with no client wide default. An int or `timedelta` is a duration from now, an aware `datetime` is the absolute moment of expiry, and `FOREVER` (0) stores without expiration, spelling that choice out at the call site. A negative duration, a naive datetime or a moment already in the past is an error. `grace` and `extend_ttl` accept the same forms. `refresh_ahead` is a window length and takes an int or `timedelta`.
- On verbs that auto create the key (`incr`, `decr`, `append`, `prepend`) the ttl applies only when the call creates the key. It never extends an existing key's lifetime.
- Parameter constraints fail loudly at the call site instead of being silently ignored. `ttl` and `refresh_ahead` require `factory`, `factory` requires `ttl`, and `extend_ttl` cannot be combined with `factory`.
- Empty serialized values are rejected, because memcached represents lease placeholders as zero byte items.

### Creating a client

```python
Memcache(
    *servers,                 # ("host", port) tuples; default localhost:11211
    serializer=None,          # StrictSerializer by default
    prefix="",                # key namespace, also a whole-cache version switch
    on_error="raise",         # or "degrade", see failure policy
    on_failure=None,          # observability hook, defaults to standard logging
    timeout=1.0,              # per-batch deadline in seconds
    username=None, password=None, max_idle=23,
)
```

```python
from memcache.experiment import Memcache, JsonSerializer

cache = Memcache(("cache1", 11211), ("cache2", 11211), serializer=JsonSerializer())
```

With multiple servers, keys are distributed by consistent hashing. Each server has an elastic connection pool; `max_idle` limits retained idle connections, not active requests. The client is a context manager and `close()` releases every connection.

### Reading

```python
cache.get(key, default=None)            # -> value
cache.get_many(keys)                    # -> {key: value}, hits only
cache.inspect(key)                      # -> ItemInfo | None
```

`get` reads one value; a miss returns `default`.

```python
user = cache.get(f"user:{uid}")
if user is None:
    user = db.load_user(uid)
    cache.set(f"user:{uid}", user, ttl=600)
```

`get_many` reads a set of keys in one round trip per backend and returns the hits, keyed by the very key objects the caller passed. A miss is expressed by key absence.

The `extend_ttl` modifier makes the same protocol command also slide the hit's expiration, which turns `get` into the read half of session renewal:

```python
session = cache.get(f"session:{sid}", extend_ttl=1800)
```

The slide is memcached's native touch and is blind: it extends whatever the read hits, including a value kept stale by a soft `delete`. A revocation that must stick goes through a hard `delete`.

`inspect` returns an item's metadata (remaining ttl, size, last access, whether it was ever hit) without transferring the value or bumping its LRU position. It is an observability tool for debugging; branching business logic on metadata is inherently racy and not a supported pattern.

### Writing

```python
cache.set(key, value, ttl)              # -> None
cache.set_many(mapping, ttl)            # -> None
cache.add(key, value, ttl)              # -> bool, True when this call won
cache.replace(key, value, ttl)          # -> bool, never resurrects
cache.touch(key, ttl)                   # -> bool
cache.delete(key, grace=0)              # -> bool
cache.delete_many(keys)                 # -> None
```

`set` unconditionally stores a value for `ttl`. `set_many` stores a batch in one round trip per backend, all sharing the same ttl.

`add` stores only when the key is absent and reports whether this caller won, which makes it a simple once-only guard for multi-instance deployments:

```python
if cache.add(f"job:daily-report:{today}", "1", ttl=86400):
    run_daily_report()
```

`replace` stores only when the key still exists and reports whether it did. It is the write half of session renewal: if the user logged out mid-request, an unconditional `set` would resurrect the dead session, `replace` will not.

```python
cache.replace(f"session:{sid}", session, ttl=1800)
```

`touch` extends a key's ttl without transferring its value, as one blind protocol command. It exists for large values (rendered pages, serialized reports) where reading the payload back just to renew it wastes bandwidth; when you are reading anyway, use `get(extend_ttl=...)`.

`delete` invalidates a key and reports whether there was something to erase; most callers ignore the result. With `grace=0` (the default) it erases outright and the next reader pays a full miss. With `grace > 0` it marks the value stale instead:

```python
cache.delete(f"article:{aid}")             # hard: old data must not reappear
cache.delete(f"article:{aid}", grace=60)   # soft: readers keep the old copy briefly
```

During the grace window plain readers keep getting the old copy, while a factory `get` elects one caller to recompute; afterwards the key decays into a normal miss. Soft invalidation pairs with factory-managed keys, and the `grace` bound holds only while nothing renews the key, since a touch slides it like any other expiration. Use the hard form when the old value must not be served for even a second.

### Get or compute

```python
cache.get(key, factory=build, ttl=3600, refresh_ahead=0)   # -> value
```

The highest frequency cache pattern as one modifier: `default` is the static fallback for a miss, `factory` is the computing one that also writes the result back.

```python
report = cache.get("report:q3", factory=build_report, ttl=3600)
```

On a miss, one caller across all processes wins a server side lease and runs the factory. Other callers in the same process wait on that result, and other processes wait briefly then compute locally without writing back. So a hot key expiring under a thousand concurrent requests costs one recomputation, not a thousand.

With `refresh_ahead`, a value whose remaining ttl has entered the window is served as is while one elected caller recomputes, so the curve never shows an expiry spike:

```python
feed = cache.get("home:feed", factory=build_feed, ttl=300, refresh_ahead=30)
```

The synchronous client's elected winner recomputes in place and pays one recomputation latency (the library owns no threads, so who pays what stays predictable). The async client's winner returns the current value immediately and recomputes as a background task owned by the client, so no request pays the refresh latency.

Every write back is conditional on the version observed at election, so a key deleted mid recompute is never resurrected. Write back failures never change what `get` returns; they go to the `on_failure` hook. A factory `get` never fails because coordination failed: every path ends in a value or the factory's own exception.

### Atomic modification

```python
cache.update(key, fn, default=..., ttl=...)   # -> new value
```

`update` atomically transforms a value. It reads the current value with its version, applies `fn`, writes back only if nothing changed in between, and retries on conflict. On a miss it starts from `default`; without one it raises `NotFoundError`. `fn` may run multiple times, so it must be pure. Raising any exception inside `fn` aborts the call, the entry is left unwritten and the exception propagates unchanged. If the retry loop keeps losing to concurrent writers, `update` raises `ConflictError`. A value kept stale by a soft `delete` counts as a miss, because transforming invalidated data would silently launder it back to fresh.

```python
cache.update("cart:42", lambda cart: cart + [item], default=[], ttl=1800)
```

```python
cache.incr(key, delta=1, ttl=...)   # -> int
cache.decr(key, delta=1, ttl=...)   # -> int
```

`incr` adds `delta` to a counter and returns the new value, creating the counter on a miss so the first request counts as `delta`. `decr` subtracts and saturates at zero. Since the ttl is fixed at creation and later calls never extend it, this is exactly fixed window rate limiting:

```python
if cache.incr(f"rate:{ip}", ttl=60) > 100:
    raise TooManyRequests
```

```python
cache.append(key, fragment, ttl)    # -> None
cache.prepend(key, fragment, ttl)   # -> None
cache.pop(key, default=None)        # -> value, atomic take and delete
```

`append` and `prepend` concatenate raw bytes (or str) onto a value, creating it on a miss; they bypass the serializer because this key family's value model is a delimited byte stream, not an object. `pop` atomically reads a value and deletes it, with no window in which concurrently appended bytes can be lost. Together they make a collect then drain pattern, such as buffering events per user and periodically taking the batch:

```python
cache.append(f"events:{uid}", b"login;", ttl=86400)
buffered = cache.pop(f"events:{uid}")   # bytes, split by the caller
```

`pop` is not limited to byte streams; taking a one-time token stored with `set` works the same way.

### Pipeline

```python
with cache.pipeline() as p:
    user = p.get(f"user:{uid}")
    hits = p.incr(f"rate:{ip}", ttl=60)
    p.touch(f"session:{sid}", ttl=1800)

if hits.value > 100:
    raise TooManyRequests
render(user.value)
```

A request prelude often needs several independent operations on different keys; a pipeline batches them into one round trip per server. The verbs, signatures and semantics inside are the same as the client's, the only difference being that each call returns a deferred result whose `.value` becomes readable once the with block exits. One failing operation only affects its own `.value`. Operations that are themselves multiple round trips (`get` with a factory, `update`, `pop`, the `_many` family) are not available inside a pipeline.

### Failure policy

By default every infrastructure failure surfaces as an exception (`OperationFailedError` with the original cause attached). The `on_error="degrade"` constructor policy decouples a cache outage from a site outage:

```python
cache = Memcache(*servers, on_error="degrade", on_failure=metrics.count)
```

Under degrade, reads report failures as misses, a `get` with a factory computes locally without writing back, and blind writes (`set`, `delete`, `touch`, `append`, ...) give up silently. Verbs whose answer feeds a business decision (`add`, `replace`, `incr`, `decr`, `update`, `pop`) keep failing loudly even under degrade, because inventing an answer is worse than failing. An `AmbiguousWriteError` (the write may have landed) always surfaces: degrading covers "the cache is down", never "the write may or may not have happened". Every absorbed failure still reaches the `on_failure` hook (standard logging by default), so degrading business behavior never degrades observability. The client never automatically retries a command after writing begins, since blindly retrying arithmetic or append could apply the mutation twice.

### Async client

`AsyncMemcache` is the same table of verbs plus `await`; `factory` and `fn` accept sync or async callables.

```python
async with AsyncMemcache(("localhost", 11211), serializer=JsonSerializer()) as cache:
    report = await cache.get("report:q3", factory=build_report, ttl=3600)
    async with cache.pipeline() as p:
        user = p.get(f"user:{uid}")
        hits = p.incr(f"rate:{ip}", ttl=60)
```

Used as an async context manager, the client owns a task group: refresh-ahead and stale-grace recomputations run as background tasks and are cancelled on close, a factory does not run inside any single caller's cancellation scope, and each waiter's own cancellation only ends its wait. Without the context manager the client still works, but background work runs inline in the calling coroutine.

### Protocol access

Everything the scenario verbs do not cover lives behind `cache.meta`, a 1:1 typed mapping of the meta protocol (`get`/`set`/`delete`/`arithmetic`/`debug`, i.e. `mg`/`ms`/`md`/`ma`/`me`) with one keyword argument per protocol flag. It works on raw bytes and returns lightly parsed responses without serialization or semantic mapping. The client's `prefix` applies here too, so the escape hatch sees the same keys the scenario layer stores.

```python
stored = cache.meta.set("key", b"payload", ttl=60, return_cas=True)
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
