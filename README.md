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

`MetaClient` exposes the full power of memcached's
[meta protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt),
including flags unavailable through the basic API.

```python
from memcache.experiment import MetaClient

client = MetaClient(("localhost", 11211))

# get returns a GetResult with rich metadata
result = client.get(
    "key",
    return_cas=True,
    return_ttl=True,
    return_hit_before=True,
)
if result is not None:
    print(result.value)
    print(result.cas_token)
    print(result.ttl)
    print(result.hit_before)

# Atomic get-and-touch (update TTL in the same round-trip)
value = client.gat("key", expire=120)

# Store only if key does not exist
client.add("key", "value", expire=60)

# Store only if key already exists
client.replace("key", "new_value")

# Increment with auto-create if missing
client.incr("counter", delta=1, initial=0, initial_ttl=3600)

# Flush with a delay
client.flush_all(delay=30)
```

`AsyncMetaClient` is the async counterpart with the same interface.

## About the Project

Memcache is &copy; 2020-2025 by [aisk](https://github.com/aisk).

### License

Memcache is distributed by a [MIT license](https://github.com/aisk/memcache/tree/master/LICENSE).
