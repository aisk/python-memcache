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

### MetaClient (Advanced)

> **Experimental.** `MetaClient` lives under `memcache.experiment` and its API
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

`MetaClient` is a high-level client for memcached's meta protocol. The core methods (`get`/`set`/`delete`/`increment`/`batch`) cover the full protocol surface, while convenience wrappers such as `add`, `cas`, `invalidate` and `get_with_lease` package common usage patterns with safer defaults. Every result has an explicit status.

```python
from memcache.experiment import Field, Get, GetStatus, MetaClient, Set

with MetaClient(("localhost", 11211)) as client:
    client.set("key", {"message": "value"}, ttl=60)

    result = client.get("key", fields=Field.CAS | Field.TTL | Field.SIZE)
    if result.status is GetStatus.HIT:
        print(result.value, result.item.cas, result.item.ttl)

    # Batch operations use a quiet pipeline and preserve input order.
    results = client.batch([Get("key"), Get("missing"), Set("other", b"x")])

    # C only transfers the value if it changed, in one round trip.
    latest = client.get("key", unless_cas=result.item.cas)
    if latest.status is GetStatus.UNCHANGED:
        use_local_copy()

    # Atomic cache-miss lease. Only one caller receives GRANTED.
    lease = client.get_with_lease("report", lease_ttl=30)
    if lease.lease_state.name == "GRANTED":
        lease.fulfill(build_report(), ttl=300)
```

`AsyncMetaClient` has the same concepts and call shape; its methods and lease `fulfill()` are awaited.

Failures never arrive silently. A single operation has nowhere to put "I never got an answer", so infrastructure trouble (a refused connection, a timeout, a malformed response, a value that will not deserialize) raises `OperationFailedError` with the original cause attached, and a write that was sent but never acknowledged raises `AmbiguousWriteError`. Semantic outcomes such as a miss, an `add` on a taken key or a CAS mismatch are real answers, so they stay in the returned status where you can branch on them. Call `check()` when success is the only outcome you accept:

```python
    client.add("lock", token, ttl=30).check()   # raises AlreadyExistsError if taken
    client.cas("key", value, token).check()     # raises CasMismatchError if it moved
    value = client.get("key").check().value
```

A batch does have somewhere to put per-operation failure, so one unreachable server spoils only its own operations and the rest of the results stand. Inspect them with `results.failures`, or opt the whole batch into raising with `results.raise_for_failures()`.

For protocol experts, `client.meta` maps the wire commands one-to-one (`get`/`set`/`delete`/`arithmetic`/`debug`, i.e. `mg`/`ms`/`md`/`ma`/`me`) with one keyword argument per protocol flag. It works on raw bytes and returns lightly parsed responses without serialization or semantic mapping:

```python
    stored = client.meta.set("key", b"payload", ttl=60, return_cas=True, opaque=b"req1")
    got = client.meta.get("key", return_cas=True, return_ttl=True)
    assert got.rc == b"VA" and got.cas == stored.cas

    # Framing-safe bytes-level escape hatch for anything not covered above.
    client.meta.execute(command="mg", key="key", flags=[b"v", b"t"])
```

## About the Project

Memcache is &copy; 2020-2025 by [aisk](https://github.com/aisk).

### License

Memcache is distributed by a [MIT license](https://github.com/aisk/memcache/tree/master/LICENSE).
