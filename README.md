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

`MetaClient` is an intent-oriented API for memcached's meta protocol. Common methods expose operations such as conditional reads, leases and conditional writes without exposing wire flags. Every result has an explicit status.

```python
from memcache.experiment import Get, GetStatus, Meta, MetaClient, Set

with MetaClient(("localhost", 11211)) as client:
    client.set("key", {"message": "value"}, ttl=60)

    result = client.get("key", meta=Meta.CAS | Meta.TTL | Meta.SIZE)
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

`AsyncMetaClient` has the same concepts and call shape; its methods and lease `fulfill()` are awaited. Protocol experts can use `client.raw.execute(...)` as a framing-safe escape hatch. See `docs/meta-client-design.md` for the complete state and failure model.

## About the Project

Memcache is &copy; 2020-2025 by [aisk](https://github.com/aisk).

### License

Memcache is distributed by a [MIT license](https://github.com/aisk/memcache/tree/master/LICENSE).
