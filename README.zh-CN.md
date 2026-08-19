# memcache

[English](README.md) | 简体中文

Python 的 memcached 客户端库。

主要特性：

- 基于 memcached 新的 meta 命令；
- 同步与异步 API；
- 支持 asyncio 和 Trio（通过 anyio）；
- 类型标注。

## 安装

```sh
$ pip install memcache
```

## 基本用法

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

异步用法与同步 API 一致，使用 `AsyncMemcache` 并加上 `await`。

## 场景客户端（实验性）

> **实验性。** 场景客户端位于 `memcache.experiment` 下，其 API 可能在任意次版本中变化。如果你依赖它，请在依赖声明中锁定**次版本号**。补丁版本（`x.y.Z`）不会引入破坏性变更，次版本（`x.Y.0`）则可能。
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

`memcache.experiment.Memcache` 把底层协议屏蔽在以使用场景命名的方法后面，meta 协议里的 CAS 和 lease 永远不会出现在调用方代码中。不必自己读出版本号再写回去，调用 `update` 传入一个变换函数，客户端在内部完成读取、比较交换、重试的循环。也不必自己实现防击穿，给 `get` 传一个 `factory`，客户端保证值只被计算一次。确实需要原始协议时，所有 meta 命令仍然可以通过 `cache.meta` 访问。

### 通用规则

- 未命中是正常的回答，不是错误。读操作在未命中时返回 `default`（通常是 `None`），异常只用来表达基础设施故障，两者永不混淆。
- 值是业务对象，序列化器是构造器级的策略。默认的 `StrictSerializer` 只存 bytes、int 和 str；`PickleSerializer` 和 `JsonSerializer` 处理任意对象，`CompressedSerializer` 可以包装它们中的任何一个。
- key 可以是 str 或 bytes，两种写法指向同一个条目。构造器的 `prefix` 给所有 key 加上命名空间，同时也是整个缓存的版本开关。
- 每个写入值的方法都要求提供 `ttl`，客户端没有全局默认值。int 或 `timedelta` 表示从现在起的时长，带时区的 `datetime` 表示过期的绝对时刻，`FOREVER`（0）表示永不过期，让这个选择在调用处一目了然。负的时长、没有时区的 datetime、已经过去的时刻都是错误。`grace` 和 `extend_ttl` 接受同样的形式，`refresh_ahead` 是窗口长度，接受 int 或 `timedelta`。
- 在会自动创建 key 的方法上（`incr`、`decr`、`append`、`prepend`），ttl 只在这次调用创建了 key 时生效，永远不会延长已存在 key 的寿命。
- 参数之间的约束在调用处大声报错而不是被静默忽略。`ttl` 和 `refresh_ahead` 依赖 `factory`，`factory` 又要求提供 `ttl`，`extend_ttl` 不能与 `factory` 同时使用。
- 序列化结果为空的值会被拒绝，因为 memcached 用零字节条目表示 lease 占位符。

### 创建客户端

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

多服务器时，key 通过一致性哈希分布。每个服务器有一个弹性连接池，`max_idle` 限制保留的空闲连接数而不是活跃请求数。客户端本身是上下文管理器，`close()` 释放所有连接。

### 读取

```python
cache.get(key, default=None)            # -> value
cache.get_many(keys)                    # -> {key: value}, hits only
cache.inspect(key)                      # -> ItemInfo | None
```

`get` 读取一个值，未命中返回 `default`。

```python
user = cache.get(f"user:{uid}")
if user is None:
    user = db.load_user(uid)
    cache.set(f"user:{uid}", user, ttl=600)
```

`get_many` 对每个后端一次往返批量读取一组 key，只返回命中的部分，字典的键就是调用方传入的那些 key 对象，未命中表现为键的缺失。

`extend_ttl` 修饰符让同一条协议命令在读取的同时顺延命中值的过期时间，这使 `get` 成为会话续期的读取一半：

```python
session = cache.get(f"session:{sid}", extend_ttl=1800)
```

这个顺延就是 memcached 原生的 touch，是盲目的：读到什么就延长什么，包括被软 `delete` 标记为过时的值。必须立刻生效的撤销要走硬 `delete`。

`inspect` 返回条目的元数据（剩余 ttl、大小、最近访问时间、是否曾被命中），不传输值也不影响它的 LRU 位置。它是排查问题用的观测工具；基于元数据做业务分支在并发下天然过期，不是受支持的用法。

### 写入

```python
cache.set(key, value, ttl)              # -> None
cache.set_many(mapping, ttl)            # -> None
cache.add(key, value, ttl)              # -> bool, True when this call won
cache.replace(key, value, ttl)          # -> bool, never resurrects
cache.touch(key, ttl)                   # -> bool
cache.delete(key, grace=0)              # -> bool
cache.delete_many(keys)                 # -> None
```

`set` 无条件存储一个值，有效期为 `ttl`。`set_many` 对每个后端一次往返批量存储，所有值共享同一个 ttl。

`add` 只在 key 不存在时存储，并报告本次调用是否抢到，可以直接当作多实例部署下只执行一次的保护：

```python
if cache.add(f"job:daily-report:{today}", "1", ttl=86400):
    run_daily_report()
```

`replace` 只在 key 仍然存在时存储，并报告是否写入了。它是会话续期的写入一半：如果用户在请求处理途中登出，无条件的 `set` 会把已注销的会话复活，`replace` 不会。

```python
cache.replace(f"session:{sid}", session, ttl=1800)
```

`touch` 只延长 key 的 ttl，不传输值，是一条盲目的协议命令。它服务于大值场景（渲染好的整页、序列化的报表），为了续命把 payload 读回来一遍纯属浪费带宽；反正要读的场合用 `get(extend_ttl=...)` 顺路续期。

`delete` 让一个 key 失效，并报告是否真的删掉了东西，多数调用者可以无视返回值。`grace=0`（默认）是硬失效，下一个读者付出一次完整的未命中。`grace > 0` 则把值标记为过时：

```python
cache.delete(f"article:{aid}")             # hard: old data must not reappear
cache.delete(f"article:{aid}", grace=60)   # soft: readers keep the old copy briefly
```

宽限期内普通读者继续拿到旧值，带 factory 的 `get` 会选出一个调用者重新计算；之后这个 key 衰变为正常的未命中。软失效与 factory 管理的 key 配套使用，且 `grace` 只在没有人续期这个 key 时才是上界，touch 会像顺延普通过期时间一样顺延它。旧值一秒都不能再出现的场合用硬失效。

### 读取或计算

```python
cache.get(key, factory=build, ttl=3600, refresh_ahead=0)   # -> value
```

最高频的缓存场景收拢为一个修饰符：`default` 是未命中时的静态兜底，`factory` 是会计算并写回的兜底。

```python
report = cache.get("report:q3", factory=build_report, ttl=3600)
```

未命中时，所有进程中只有一个调用者赢得服务端 lease 并运行 factory。同进程内的其他调用者等待这个结果，其他进程短暂等待后本地计算但不写回。因此一个热点 key 在一千个并发请求下过期，代价是一次重算，而不是一千次。

加上 `refresh_ahead` 后，剩余 ttl 进入窗口的值会被原样返回，同时选出一个调用者重新计算，曲线上永远看不到过期的尖峰：

```python
feed = cache.get("home:feed", factory=build_feed, ttl=300, refresh_ahead=30)
```

同步客户端的当选者就地重算，付出一次重算延迟（这个库不拥有线程，谁付出多少延迟完全可预测）。异步客户端的当选者立即返回现值，重算作为客户端持有的后台任务执行，没有任何请求付出刷新延迟。

所有写回都以选举时观察到的版本为条件，因此重算过程中被删除的 key 永远不会被复活。写回失败不改变 `get` 的返回值，只进入 `on_failure` 钩子。带 factory 的 `get` 不会因为协调失败而失败：每条路径的终点都是一个值，或者 factory 自己抛出的异常。

### 原子修改

```python
cache.update(key, fn, default=..., ttl=...)   # -> new value
```

`update` 原子地变换一个值。它带版本读出当前值，应用 `fn`，只在中间没有别人改动时写回，冲突则重试。未命中时以 `default` 为起点，没有提供 `default` 则抛出 `NotFoundError`。`fn` 可能运行多次，因此必须是纯函数。在 `fn` 中抛出任何异常都会中止本次调用，条目保持未写，异常原样传出。如果重试循环一直输给并发写入者，`update` 抛出 `ConflictError`。被软 `delete` 标记为过时的值按未命中处理，因为在已失效的数据上做变换会把它无声地洗回新鲜状态。

```python
cache.update("cart:42", lambda cart: cart + [item], default=[], ttl=1800)
```

```python
cache.incr(key, delta=1, ttl=...)   # -> int
cache.decr(key, delta=1, ttl=...)   # -> int
```

`incr` 给计数器加上 `delta` 并返回新值，未命中时创建计数器，所以第一次请求计为 `delta`。`decr` 做减法并在零处饱和。由于 ttl 在创建时固定，之后的调用不会延长它，这正是固定窗口限流需要的行为：

```python
if cache.incr(f"rate:{ip}", ttl=60) > 100:
    raise TooManyRequests
```

```python
cache.append(key, fragment, ttl)    # -> None
cache.prepend(key, fragment, ttl)   # -> None
cache.pop(key, default=None)        # -> value, atomic take and delete
```

`append` 和 `prepend` 把原始字节（或 str）拼接到值的尾部或头部，未命中时创建值。它们绕过序列化器，因为这类 key 的值模型是带分隔符的字节流而不是对象。`pop` 原子地读出一个值并删除它，不存在并发追加的字节被丢失的窗口。两者合起来构成收集再取走的模式，比如按用户缓冲事件并定期取走一批：

```python
cache.append(f"events:{uid}", b"login;", ttl=86400)
buffered = cache.pop(f"events:{uid}")   # bytes, split by the caller
```

`pop` 不限于字节流，取走一个用 `set` 存入的一次性令牌也是同样的用法。

### 管线

```python
with cache.pipeline() as p:
    user = p.get(f"user:{uid}")
    hits = p.incr(f"rate:{ip}", ttl=60)
    p.touch(f"session:{sid}", ttl=1800)

if hits.value > 100:
    raise TooManyRequests
render(user.value)
```

一个请求的前奏往往需要对不同 key 做几个互不依赖的操作，管线把它们合并为每个服务器一次往返。管线内的动词、签名和语义与客户端完全相同，唯一的区别是每次调用返回一个延迟结果，退出 with 块后它的 `.value` 才可读。单个操作失败只影响它自己的 `.value`。本身包含多轮往返的操作（带 factory 的 `get`、`update`、`pop`、`_many` 家族）不出现在管线里。

### 故障策略

默认情况下每个基础设施故障都以异常形式浮现（`OperationFailedError`，原始原因挂在 cause 上）。构造器策略 `on_error="degrade"` 把缓存故障与整站故障解耦：

```python
cache = Memcache(*servers, on_error="degrade", on_failure=metrics.count)
```

degrade 之下，读操作把故障报告为未命中，带 factory 的 `get` 本地计算但不写回，盲目的写操作（`set`、`delete`、`touch`、`append` 等）静默放弃。结果用于业务判断的方法（`add`、`replace`、`incr`、`decr`、`update`、`pop`）在 degrade 下仍然报错，因为编造一个答案比报错更危险。`AmbiguousWriteError`（写入可能已经生效）总是浮现：degrade 降的是"缓存不可用"，绝不是"不知道写没写进去"。每个被吸收的故障仍然会到达 `on_failure` 钩子（默认走标准 logging），降级业务行为绝不降级可观测性。写入开始后客户端永远不会自动重试命令，因为盲目重试算术或追加可能让变更生效两次。

### 异步客户端

`AsyncMemcache` 是同一张动词表加上 `await`；`factory` 和 `fn` 接受同步或异步的可调用对象。

```python
async with AsyncMemcache(("localhost", 11211), serializer=JsonSerializer()) as cache:
    report = await cache.get("report:q3", factory=build_report, ttl=3600)
    async with cache.pipeline() as p:
        user = p.get(f"user:{uid}")
        hits = p.incr(f"rate:{ip}", ttl=60)
```

以异步上下文管理器使用时，客户端持有一个任务组：refresh_ahead 与软失效宽限期的重算作为后台任务运行并在 close 时全部取消，factory 不运行在任何单个调用者的取消范围内，每个等待者自己的取消只结束它的等待。不进上下文管理器客户端也能工作，只是后台工作退化为在调用协程内就地执行。

### 协议层

场景动词没有覆盖的一切都在 `cache.meta` 后面，它是 meta 协议的 1:1 类型化映射（`get`/`set`/`delete`/`arithmetic`/`debug`，即 `mg`/`ms`/`md`/`ma`/`me`），每个协议 flag 对应一个关键字参数。它工作在原始字节上，返回轻度解析的响应，不做序列化也不做语义映射。客户端的 `prefix` 在这里同样生效，逃生舱看到的 key 与场景层存入的一致。

```python
stored = cache.meta.set("key", b"payload", ttl=60, return_cas=True)
got = cache.meta.get("key", return_cas=True, return_ttl=True)
assert got.rc == b"VA" and got.cas == stored.cas

# Framing-safe bytes-level escape hatch for anything not covered above.
cache.meta.execute(command="mg", key="key", flags=[b"v", b"t"])
```

每个场景的可运行导览见 `examples/scenario_demo.py`，设计依据见 `docs/design-scenario-api.md`。

## 关于项目

Memcache 版权归 [aisk](https://github.com/aisk) 所有（2020-2025）。

### 许可证

Memcache 以 [MIT 许可证](https://github.com/aisk/memcache/tree/master/LICENSE)分发。
