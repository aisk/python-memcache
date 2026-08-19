# 面向场景的高层 API 设计

日期：2026-08-16 · 范围：下一版高层客户端 API 的完整形态。姊妹篇 [design-review-experiment.md](design-review-experiment.md) 是对现状的宏观 review，本文不重复其结论。

**本稿是第二稿。** 第一稿完成后，同一组公理和场景在 go-memcache 里跑了一轮参考实施（推导记录见该仓库的 design-scenario-api-go.md，经九次修订，实现已落地），实施暴露的语义漏洞与更优解回传到本稿。回传遵循一条过滤规则：由 Go 语言事实驱动的结论不迁移，Python 有关键字参数和默认值，场景相近的门继续合并，不学 Go 拆动词（`Delete`/`Invalidate` 两个方法在这里仍是 `delete(grace=)`，`pipeline` 仍然存在，因为 Python 没有廉价的并发原语替代它）；与语言无关的设计发现全部吸收。本稿吸收的要点：S11 消费端两步取走的丢数据竞态改为原子的 `pop`；factory 的写回统一为条件写；vivify 占位符引出的 0 字节规则；`update` 对 stale 条目按 miss 处理；构造器级 `default_ttl` 撤销，ttl 成为每个写操作的必要参数；`touch` 与 `extend_ttl` 的盲目性从含糊变为写明的契约；degrade 表补上歧义写穿透与观测钩子；`ItemInfo` 撤下 version；异步版的刷新路径后台化。逐项理由见各节。

## 前提

**experiment 下的一切 API 随时可以改，不需要考虑兼容性。** 它是试验场，没有任何用户承诺，本文的设计如果被采纳，可以直接替换掉 exp 现有的高层 surface，不留过渡层、不留别名。

**本设计只从第一性原理出发。** 不继承 python-memcached、pymemcache 的接口惯例，不继承 meta 协议的命令结构，也不把 examples/ 下的现有例子当作需求（例子是照着旧 API 写的，拿它当需求源头是循环论证）。每个 API 的存在都必须由一个具体用户场景支撑，推导路径是：场景 → 语义 → 签名。协议只在最后回答"这能不能实现"，不参与"该长什么样"。

## 第一性原理

缓存是什么：一份放在旁路的、随时可能丢失的数据副本，真相在别处（数据库、计算过程），被大量并发访问，目的是用它换延迟和容量。从这个本质推出四条公理，整个 API 的形态都是它们的推论。

1. **缓存里的数据随时可能不在。** 驱逐、过期、重启、扩缩容都会让它消失。所以 miss 是正常答案而不是错误，所有读 API 必须让"没有"的处理和"有"一样顺手。
2. **每份缓存数据在别处都有真相和重算路径。** 用户拿到 miss 之后下一步永远是"去算一遍再写回来"。所以最高频的场景不是 get 而是"get 否则算"，API 应该把重算路径（factory）纳入自己的语义，而不是留给每个用户在外面拼 if。
3. **同一份数据被并发共享。** 热点 key 的重算风暴、读改写的相互覆盖，这些并发协调问题用户手写要么写错要么重复。协调是库的责任，协调用到的机制（谁当选、版本号比对）是实现细节，不该出现在用户代码里。
4. **缓存是可用性优化，不是真相来源。** 缓存集群故障不应自动等于业务故障，但静默把故障装成 miss 会掩盖事故。故障时的行为必须是用户显式选择的策略，不能是库偷偷决定的默认。

由此派生的风格规则：

- API 返回业务值（value、bool、int），不返回需要用户拆解的结果对象。答案用返回值表达，故障用异常表达。
- 协议概念（cas token、lease 状态、元数据 flag）不出现在场景层的任何签名和返回值里。
- 策略（序列化、key 前缀、故障行为）集中在构造器。调用点只表达业务意图，同一个应用里这些策略本来就不该逐调用变化。ttl 不在此列：生存期是每次写操作的必要成分而不是应用级策略，理由见 API 总表。
- 概念尽量由协议原生能力直接映射，协议不天然支持的语义不用多条命令在客户端模拟。操作固有的组合性不算模拟（`update` 的版本循环、`pop` 的条件删除保留），响应 flag 的零成本处理也保留。这条规则来自 Go 实施，在 `touch` 与 `extend_ttl` 的盲目性上兑现（见 S5/S9）。
- 没有场景支撑的能力不进场景层。需要它的人去协议层（`client.meta`），那里永远 1:1 全覆盖。
- 场景相近的门用关键字参数合并成一个，但只在三个条件同时成立时合并：动词（意图）相同、返回类型相同、新参数是正交修饰而不是模式开关。所以"给我这个值"的各种姿态（有无 factory、要不要顺延 ttl）合并进 `get`，"让这个 key 失效"的软硬两档合并进 `delete`；而 `add` 不并入 `set`（返回类型从 None 变 bool，意图从"写入"变"抢占"），`get_many` 不并入 `get`（返回类型从值变字典）。这条判据同时是防线：旧 API 的教训不是"参数多"，而是参数携带协议词汇、还会改变返回形态。

## 场景与 API

### S1 对象缓存

最普通的旁路缓存：用户资料、商品详情，读多写少，源头是数据库。

```python
user = cache.get(f"user:{uid}")
if user is None:
    user = db.load_user(uid)
    cache.set(f"user:{uid}", user, ttl=600)

cache.delete(f"user:{uid}")  # after the profile is edited
```

语义要点：`get` miss 返回 `default`（默认 None），不抛异常（公理 1）。`set` 成功无返回值，到达不了服务器才抛异常。`delete` 幂等，返回是否真的删掉了东西，多数调用者可以无视返回值。

### S2 页面聚合

一次渲染需要几十个对象，逐个 get 的往返延迟不可接受。

```python
found = cache.get_many(keys)                     # {key: value}, hits only
missing = [k for k in keys if k not in found]
loaded = db.load_users(missing)
cache.set_many(loaded, ttl=600)
found.update(loaded)
```

语义要点：`get_many` 返回只含命中的字典，键就是调用者传入的键对象。一次调用一轮往返（每台服务器一条管线），这是它存在的全部理由。本场景只覆盖读聚合；上面的示例本身就是公理 2 反对的"用户在库外手拼 get 否则算"，且几十个 key 同时过期时没有任何防击穿协调，这个缺口的立项见待评估问题（`get_many` 的 factory）。

### S3 昂贵计算与防击穿

报表、推荐结果、首页 feed，一次计算秒级。热点 key 过期的瞬间会有成百上千个请求同时发现 miss，如果各算各的，源头会被打垮。这是公理 2 和公理 3 的交汇点，也是本设计的核心增量。它不是新方法，而是 `get` 的 `factory` 参数：`default` 是静态兜底，`factory` 是会写回的计算兜底，两者是同一个概念的两档：

```python
report = cache.get("report:q3", factory=build_report, ttl=3600)
```

语义要点：命中直接返回。miss 时在服务端抢单飞资格，当选者跑 `build_report` 并写回；同一进程内的其余调用者挂在当选者的结果上，一个进程最多一个请求在等网络（进程内合并叠加在服务端竞选之上，来自 Go 实施）；其他进程的请求短暂等待后重试读取，等不到就本地降级自己算一份返回（不写回，避免写风暴）。任何路径最终都返回一个值，带 factory 的 `get` 不会因为协调失败而失败（公理 4）。完整状态机见下文专节。`ttl` 与下面的 `refresh_ahead` 只在给了 factory 时有意义，裸 `get` 传它们会直接报错而不是被忽略；反过来给了 factory 就必须给 `ttl`（写操作必须声明生存期，见 API 总表）。

### S4 平滑过期

S3 解决了 miss 风暴，但过期瞬间仍有一个"人人撞上 miss"的尖峰。对延迟敏感的场景希望根本不出现这个瞬间：

```python
feed = cache.get("home:feed", factory=build_feed, ttl=300, refresh_ahead=30)
```

语义要点：进入过期前 30 秒的窗口后，服务端从并发读者中选出一个提前重算并写回，其余读者继续拿现值。曲线上看 ttl 永不真正到期，重算成本始终只有一份。当选者付不付延迟由客户端形态决定：同步客户端的当选者同步重算，付一次延迟（库不拥有线程，第一稿的理由对同步版依然成立）；`AsyncMemcache` 的当选者立即拿现值返回，重算作为后台任务执行写回，没有任何请求付出刷新延迟。这是对第一稿"一律同步重算"的修订，刷新场景的当选者手上就有可返回的现值，阻塞它恰恰制造本场景要消灭的尖峰，Go 实施先验证了后台化的可行性，异步版有事件循环，同样的理由成立。

### S5 内容更新后的失效

编辑器保存了文章，缓存里的旧版必须失效。这是同一个动词的两种力度，用默认参数合并：

```python
cache.delete(f"article:{aid}")             # hard: next reader pays a full miss
cache.delete(f"article:{aid}", grace=60)   # soft: readers keep the old copy briefly
```

语义要点：`grace=0`（默认）是硬失效，适合"旧数据一秒都不能再出现"的场合。`grace>0` 是软失效，宽限期内带 factory 的读者继续拿旧值，同时选出一人重算，重算完成后所有人切到新值。写热点内容的失效用软的，避免每次编辑都触发一轮击穿。`grace` 到期后仍无人重算，则退化为普通 miss。裸 `get` 在宽限期内同样拿旧值，按普通命中返回、不带任何标记，这不是泄漏而是软失效的本义（读者暂时继续用旧值）；需要感知新旧的读者用带 factory 的 `get`，旧值一秒都不能出现的用硬 `delete`。`update` 对 stale 的处理相反（见 update 专节），因为读旧值可以接受、基于旧值生产新值不可以。还有一条盲目续期的边界：`touch` 和 `get(extend_ttl=)` 是协议原生的无条件续期，会把 stale 条目的宽限一并顺延，所以 `grace` 是"无人续期时的上界"。约束由契约承载：软失效与 factory 管理的可重算键配套，滑动过期键（会话等）的撤销走硬 `delete`，两族键不相交，混用是类别错误。

### S6 并发修改共享结构

购物车、已读集合、合并去重的列表。两个请求同时"读出、改、写回"会互相覆盖。协调机制（版本号比对加重试）按公理 3 收进库里：

```python
cache.update("cart:42", lambda cart: cart + [item], default=[], ttl=1800)
```

语义要点：库内部完成"带版本读、跑函数、条件写回、冲突则重读重试"的完整循环。`default` 定义了 miss 时的起点。函数必须纯（可能被调用多次）。重试耗尽抛 `ConflictError`，这属于"负载已经异常"的故障信号而不是正常答案。版本号在用户代码中零出现。

### S7 计数与限流

接口限流、活动计数。要求原子，且 miss 时自动从零开始，因为"先 set 0 再 incr"存在竞态而且多一轮往返：

```python
n = cache.incr(f"rate:{ip}", ttl=60)
if n > 100:
    raise TooManyRequests
```

语义要点：miss 时自动以 0 为起点加上 delta，`ttl` 只在这次创建时设置、后续递增不刷新，这恰好就是固定窗口限流的语义。自动创建在协议上依赖携带 TTL 的 N flag，所以 `ttl` 必填；永久计数器（全局统计）是真实需求，显式写 `ttl=FOREVER`。返回递增后的整数。`decr` 对称，减到 0 为止不下穿。`incr`/`decr` 不合并为一个带符号 delta 的方法（第一稿的待评估问题 7，Go 实施同一裁决）：两者协议边界行为不同（incr 回绕、decr 到 0 饱和），符号驱动的行为不连续是坑。

### S8 抢占与单实例任务

多实例部署的定时任务只允许一个实例执行；同一封邮件只发一次。需要的是"仅当不存在时写入"的原子判定：

```python
if cache.add(f"job:daily-report:{today}", "1", ttl=86400):
    run_daily_report()
```

语义要点：返回 bool，抢到为 True。就这么多，不提供释放、续租、fencing，那是分布式锁服务的领域，缓存只承诺"大多数时候只有一个"，要强保证的场景本来就不该用 memcached。

### S9 会话：续期与写回

看真实的会话流程会发现，续期从来不是孤立动作：每个请求进来先读 session，读到了才谈得上顺延窗口。所以它不是一个 `touch` 方法，而是 `get` 的一个修饰参数，读值和续期一轮往返完成：

```python
session = cache.get(f"session:{sid}", extend_ttl=1800)
```

会话的写半边有一个容易被忽略的约束：请求处理中修改了 session 内容要写回，但如果用户在这中间登出（session 已被 delete），无条件 `set` 会把已注销的会话复活。写回必须是"仅当还存在才写"：

```python
cache.replace(f"session:{sid}", session, ttl=1800)   # -> bool, never resurrects
```

语义要点：`get(extend_ttl=)` 命中则返回值并顺延 ttl，miss 则返回 default，自然表示"会话已过期请重新登录"。续期在协议上就是读命令带 T flag，一轮往返、盲目续期，没有"只有 fresh 才续期"的门控形态；在客户端用读回值再条件写回去模拟这个门控，等于让最热路径（每请求一次的会话续期）付双倍往返和值的双向传输，去防御一个本身已是类别错误的用法（会话键不该被软失效，见 S5），按协议原生映射的规则不做。会话的撤销走 `delete` 而不是软失效。`replace` 返回是否写成，False 意味着会话已经不在，调用方通常直接无视（用户都登出了）。同样的"不复活"语义也服务后台刷新任务：重算的值只写回还活着的 key，被驱逐的说明没人读，不再塞回去占内存。

续期还有一个不读值的变体。值很大时（渲染好的整页 HTML、序列化的报表或模型，几百 KB 起步），为了续命把 payload 传回来一遍纯属浪费带宽，这时需要独立的 `touch`：

```python
cache.touch(f"render:{page}", ttl=3600)   # -> bool, extends ttl without transferring the value
```

`get(extend_ttl=)` 与 `touch` 的分工由值的大小和是否顺路决定：反正要读的顺路续期，只想续命的不传值。这也暴露了合并判据的一个隐含前提：修饰参数不能改变操作的成本结构，大 value 场景下"搭车读"的代价打破了这个前提，所以 touch 值得一扇独立的门。

### S10 故障与降级

memcached 集群故障时业务怎么办（公理 4）。这是构造器级策略，不是每个调用点的判断：

```python
cache = Memcache(servers, on_error="raise")     # explicit failures (default)
cache = Memcache(servers, on_error="degrade")   # cache outage != site outage
```

degrade 模式下各操作的行为：

| 操作 | degrade 行为 |
|---|---|
| get / get_many | 当作 miss。给了 factory 就直接跑 factory 返回其结果（不写回），否则返回 default |
| inspect | 当作 miss，返回 None（诊断工具自己会发现连不上） |
| set / set_many / delete / delete_many / touch / append / prepend | 静默放弃（delete/touch 返回 False） |
| add / replace | 无法仲裁"存在与否"，静默返回 False 会让抢占场景全体沉默、返回 True 会全体放行，都是错的，仍然抛异常 |
| update / pop / incr / decr | 结果被业务决策消费（计数进限流判断、pop 取走的数据进后续处理），编造一个答案比报错更危险，仍然抛异常 |

第一稿把 update 的 degrade 行为列为待评估，Go 实施拍板为仍抛异常：原子性无法降级，这让 degrade "绝不因缓存故障抛错"的承诺带上几个例外，但例外全部落在"答案会被业务决策消费"的动词上，全局开关的危险性被动词级规则封了底。另外两条边界也在实施中定型：

- **歧义写是唯一穿透 degrade 的错误类别。** "请求已写出但结果不可观测"的故障不能被降级成静默放弃，"静默放弃"只适用于确定没生效的故障。degrade 降的是"缓存不可用"，不是"不知道写没写进去"。
- **观测钩子不再"形式待定"。** 构造器接受 `on_failure=callable`（默认记标准 logging），三类事件进它：degrade 吞掉的故障、异步后台重算的失败、factory 内部写回的失败（含歧义写，写回失败不影响 `get` 的返回值，见 factory 专节）。degrade 降的是业务行为，不是可观测性。

### S11 事件缓冲

每个用户一条行为流、每次请求往审计缓冲追一条记录，消费者定期整条取走再清空。用 `update` 做这件事要读回整个缓冲、反序列化、追加、条件写回，缓冲越大越贵还会冲突重试；这个场景要的是 O(增量) 的原子追加：

```python
cache.append(f"events:{uid}", b"login;", ttl=86400)
...
buffered = cache.pop(f"events:{uid}")   # atomic take and delete; bytes, split by the caller
```

语义要点：这类 key 的值模型是带分隔符的字节流而不是对象，`append`/`prepend` 只接受 bytes/str 且绕过序列化器，读回后由调用方切分；这与 `set` 的对象模型是刻意不同的两个世界，混用（先 set 一个对象再 append）是使用错误。miss 时自动创建，`ttl` 与 `incr` 同一语义，只在本次追加创建了 key 时生效，后续追加不刷新，天然构成滚动收集窗口。`prepend` 对称，服务"最新在前"的读取端。

消费端是 `pop`，对第一稿的一处修正：第一稿的消费端是先 `get` 再 `delete` 两步，两步之间新追加的字节会被 delete 无声吞掉，这是真实的丢数据竞态，Go 实施发现第一稿在这里漏掉了自己公理 3 的推论（协调是库的责任）。`pop` 在库内用"带版本读、条件删除（版本匹配才删）、冲突重试"的循环保证取走和删除之间没有缝隙，语义就是 `dict.pop`：返回值并删除，miss 返回 `default`（默认 None）。它不并入 `get`：破坏性读改变的是操作本身而不是修饰，在合并判据里是模式开关，所以是独立的一扇门。`pop` 不限于字节流模型，对象 key 上"取走并处理一次"（如一次性令牌）同样成立，读步的反序列化规则与 `get` 相同。

### S12 一次往返的请求前奏

一个 web 请求进门先做三件事：读用户资料、限流计数加一、会话续期。三个 key、三种操作、互不依赖，逐个调用要付三轮往返，本该同行：

```python
with cache.pipeline() as p:
    user = p.get(f"user:{uid}")
    hits = p.incr(f"rate:{ip}", ttl=60)
    p.touch(f"session:{sid}", ttl=1800)

if hits.value > 100:
    raise TooManyRequests
render(user.value)
```

语义要点：`pipeline()` 里的动词、签名、语义与主表完全相同，唯一区别是返回值变成延迟结果，退出 with 时按服务器分组、每台一轮往返执行完毕，之后 `.value` 可读（with 内读取报错，因为还没执行）。延迟结果只是"晚一点到的返回值"，不是需要拆解的状态机对象，风格规则不因此破例。这也是它与被裁掉的操作对象式混合 batch 的区别：后者让用户构造命令列表（`batch([Get(...), Set(...)])`），是协议思维；这里是同一套场景动词攒着一起发。单个操作失败只影响它自己的 `.value`（抛对应异常，degrade 模式按 S10 的表降级），不连坐同批其他操作。

### S13 观测与排查

线上某个 key 行为异常：命中率骤降、疑似被驱逐、不知道还剩多久过期。排查工具（管理后台的缓存页、debug 接口、REPL）需要程序化地看到条目的元信息，而观测不能扰动被观测对象：读一下不该续它的命、不该动它的 LRU 位置、更不该把几 MB 的值传回来。这是一支纯探针，和 `get` 是两个操作：

```python
info = cache.inspect(f"report:q3")
if info is None:
    print("not cached")
else:
    print(info.ttl, info.size, info.last_access, info.hit_before)
```

语义要点：不传值、不碰 LRU。返回只读的 `ItemInfo`，miss 返回 None。字段一次全带，没有挑选参数，理由同前：元数据统共几十字节，挑选本身就是多余的自由度。`ItemInfo` 是这个场景的业务值而不是状态机对象，风格规则不因此破例。第一稿曾让 `version` 随行展示，本稿撤下（Go 实施的裁决回传）：一个可读的版本号会诱惑用户把它传回协议层某个接受版本的入口，跨层走私在运行时拦不住，最干净的防线是场景层根本不出现这个数字，要看版本走 `cache.meta`。边界也要写明：诊断之外的程序逻辑不应依赖 inspect，"读元数据再决定"的模式在并发下天然过期，需要按条目状态分支的场景应该回到 S3/S4/S5 的服务端协调机制。

inspect 覆盖不了的组合（某个新协议 flag、批量探测、debug 命令）永远有协议层：`cache.meta` 提供每个 meta 命令的类型化入口和任意拼装 flag 的 `execute()`，那里就是"用户自己组装"的地方，场景层不需要再为长尾开口子。

## API 总表

```python
cache = Memcache(
    ("cache1", 11211), ("cache2", 11211),
    serializer=JsonSerializer(),
    prefix="myapp:",        # namespace, also the whole-cache version switch
    on_error="raise",       # or "degrade"
    on_failure=None,        # observability hook, defaults to logging
    timeout=1.0,
)

# reading, one verb for S1/S3/S4/S9
cache.get(key, default=None)                            # plain read
cache.get(key, factory=build, ttl=3600, refresh_ahead=0)  # compute on miss, stampede-safe; ttl required
cache.get(key, extend_ttl=1800)                         # read and slide expiry, blind touch
# all forms return a value; factory/extend_ttl are orthogonal modifiers

# writing and invalidation, S1/S5; every write states its lifetime, FOREVER for no expiry
cache.set(key, value, ttl)          # -> None
cache.delete(key, grace=0)          # -> bool; grace>0 marks stale instead

# aggregation, S2
cache.get_many(keys)                # -> {key: value}
cache.set_many(mapping, ttl)        # -> None
cache.delete_many(keys)             # -> None

# concurrent mutation, S6
cache.update(key, fn, default=..., ttl=...)   # -> new value

# counters, S7; ttl applies on create only
cache.incr(key, delta=1, ttl=...)   # -> int
cache.decr(key, delta=1, ttl=...)   # -> int

# conditional blind writes, S8/S9: set is unconditional,
# add writes only if absent, replace only if present
cache.add(key, value, ttl)          # -> bool
cache.replace(key, value, ttl)      # -> bool

# ttl extension without payload transfer, S9
cache.touch(key, ttl)                 # -> bool

# event buffers, byte-stream value model, S11; ttl applies on create only
cache.append(key, fragment, ttl)      # -> None
cache.prepend(key, fragment, ttl)     # -> None
cache.pop(key, default=None)          # -> value; atomic take and delete

# one round trip for independent ops, S12
with cache.pipeline() as p:
    r = p.get(key)      # same verbs as above, deferred results
r.value

# non-intrusive probe, S13
cache.inspect(key)      # -> ItemInfo | None; no value transfer, no LRU bump

# escape hatch, 1:1 protocol surface: typed commands plus raw assembly
cache.meta.get(return_ttl=True, ...)
cache.meta.execute(command="mg", key=key, flags=[b"v", b"t"])
```

`AsyncMemcache` 是同一张表加 await，`get` 与 `update` 的 factory/fn 接受同步或异步函数；刷新路径的行为差异（后台化）见 factory 专节。

最终 16 个方法加一个 `pipeline` 入口。`fetch`、`invalidate` 按风格规则里那条判据收编为 `get`/`delete` 的关键字参数；`touch` 也曾被收编进 `get(extend_ttl=)`，后因大 value 续期场景恢复独立（见 S9），两者并存、按值大小分工。`add`/`replace` 不并入 `set`，同样出自这条判据：返回类型从 None 变 bool，且"存在与否"对条件写是答案而对无条件写压根不是问题。`pop` 不并入 `get`：破坏性读是模式开关不是修饰（见 S11）。参数间的约束要在运行时大声报错而不是静默忽略：`ttl`/`refresh_ahead` 依赖 `factory`，`default` 与 `factory` 同时给出时 factory 优先（degrade 模式除外，见 S10）。

**ttl 没有静默兜底。** 每个写操作（`set` 族、带 factory 的 `get`、`update`、`add`/`replace`、`incr`/`decr`、`append`/`prepend`、`touch`）的 `ttl` 必填，负值报错，永不过期是合法需求但必须显式写 `FOREVER`（值为 0，与协议一致）。第一稿的构造器级 `default_ttl` 撤销，这是 Go 实施回传的裁决而且理由与语言无关：默认 TTL 有意义的粒度是数据族（Django 的 per-alias TIMEOUT、Spring 的 per-cache 配置都在这一档），本设计第一版没有族级策略挂载点，只剩全局一档，而全局默认几乎不会被真实项目使用（session、feed、计数器共用一个 TTL 不成立），却制造了"调用点不传 ttl 的含义取决于远处构造器配置"的坏耦合。规则由此收敛成一句话：写操作必须声明生存期。`incr`/`append` 的 ttl 只在创建时生效的窗口语义不变（见 S7/S11）。

## get 带 factory 的完整语义

带 factory 的 `get` 是唯一有实质状态机的路径，值得单独写清楚。每次调用落在下表的一格里：

| 服务端状态 | 当选者（每个状态至多一人） | 其余请求 |
|---|---|---|
| 新鲜命中 | 返回现值 | 返回现值 |
| 临过期（refresh_ahead 窗口内） | 跑 factory，写回，返回新值；异步版立即返回现值、后台重算写回 | 返回现值 |
| 陈旧（软 delete 的 grace 内） | 跑 factory，写回，返回新值；异步版立即返回旧值、后台重算写回 | 返回旧值 |
| miss | 跑 factory，写回，返回新值 | 进程内挂在当选者结果上；跨进程等待后重试读，超时则本地跑 factory 返回，不写回 |

设计决定：

- 同步客户端的当选者同步重算。库不拥有线程，后台刷新属于应用层的选择，同步重算让"谁付出了多少延迟"完全可预测。`AsyncMemcache` 没有这个约束，刷新路径（临过期与陈旧，当选者手上有可返回的现值）立即返回、重算作为后台任务执行写回；miss 路径两种客户端都同步，不算就没有东西可返回。后台任务由客户端持有，close 时全部取消。这是对第一稿"一律同步重算"的修订，理由见 S4。
- **所有写回都是条件写。** 当选者写回（前台 miss 路径和后台刷新路径都是）携带竞选时读到的版本，写回被拒（期间发生了 `delete`、`set` 或再次软失效）就静默放弃并进 `on_failure`。第一稿只在 S9 用一句"不复活"带过，本稿把它上升为写回的统一规则：后台化拉大了"重算开始"到"写回落盘"的时间窗，用户在窗内登出并 delete 了 key，无条件写回会把死数据复活。
- **写回失败不影响返回值。** 当选者要返回的值在写回前就已确定（现值、旧值或 factory 结果），写回的任何失败（含歧义写）只进 `on_failure`。这与 S10 的"歧义写穿透 degrade"不冲突：穿透规则约束的是用户直接发起的写，factory 的内部写回对用户而言不是一次写操作。
- **进程内合并叠加在服务端竞选之上。** 服务端 lease 管跨进程，进程内合并管进程内（多线程或多协程），一个进程对一个 key 最多一个在途 factory，其余调用者挂在它的结果上。合并键是调用方传入的完整 key，先到者的 factory 和参数全赢，后到者不同的 ttl/factory 被忽略，这是公理 2 的推论（同一份数据只该有一条重算路径），文档写明即可，不做成配置。
- **异步版的 factory 不跑在单个调用方的取消范围内。** 结果被全体等待者共享，某个短超时调用方当选后被取消不该连坐全体（singleflight 组合的经典事故形态），后台路径里调用方更是早已返回。每个等待者各自尊重自己的超时，超时或取消就退出等待、本地跑 factory 兜底（不写回）。契约的另一半：factory 不得依赖请求级的上下文状态。
- 输家的等待与重试参数（初版定为最多等约 1 秒，指数退避轮询）不暴露。等待上限本质上由 factory 的典型耗时决定，用户很难比库选得更好，先给定值，真实需求出现再考虑构造器级参数。
- factory 抛异常时按路径分：同步路径原样向上抛（`get` 不吞业务错误），异步后台路径没有调用方，进 `on_failure`。当选者失败后单飞资格随 lease ttl 自然释放，后续请求重新竞选。
- factory 的结果无论是什么值（包括 None）都会写回，"算出来是 None"与"miss"由此区分开。
- **0 字节规则。** vivify 在服务端创建的占位符是 0 字节条目，而协议只对携带竞选 flag 的请求返回竞选标记，裸 `get` 看到的占位符就是一个空值的普通命中，不处理就会把协调内脏漏给用户。所以立两条规则：序列化器不得输出 0 字节（`set`/`add`/`replace` 校验，违者报错），场景层所有读（`get`/`get_many`/`update` 与 `pop` 的读步）把 0 字节值一律折算为 miss。这条规则来自 Go 实施，第一稿没有覆盖。
- **故障边界。** 当选者进程崩溃或客户端 close 撤销后台任务时无人写回，其余读者继续拿现值或旧值直到条目自然过期或宽限耗尽，然后退化为 miss 路径重新竞选。S4"读者不付刷新延迟"和 S5 的宽限承诺在这个故障下有界降级，不需要也没有补救机制。

底层由 meta 协议的 lease/vivify（miss 单飞）、recache（临过期竞选）、stale 标记（软失效）、条件写（版本比对）直接支撑，无需客户端侧的锁。这些机制是本设计选择建立在 meta 协议上的根本原因，但它们的词汇全部止步于实现内部。

## update 与 pop 的完整语义

`update`：

1. 带版本读取，三种起点。命中：现值。miss：以 `default` 为当前值，未提供 `default` 的 miss 抛 `NotFoundError`（没有起点就没有"更新"可言）。stale（软失效的宽限期内）：同 miss 处理。stale 规则来自 Go 实施：`fn` 是变换不是从源头重算，把 `fn` 应用在已失效的数据上再写回，会把 stale 派生值洗成 fresh 值，无声抵消软失效的意图（S5 的正文软失效和 S6 的计数更新同族使用时必撞）。
2. 调用 `fn(current)` 得到新值。
3. 条件写回：从 miss 起步用"仅当仍不存在"写入，从命中起步用"仅当版本未变"写入，从 stale 起步用读到的版本条件覆盖（stale 条目仍然存在，"仅当不存在"必败会活锁）。
4. 写回被拒绝说明有并发修改，回到第 1 步。最多 8 次，耗尽抛 `ConflictError`。

`pop` 是同一循环的删除版：带版本读，miss 返回 `default`；"仅当版本未变"才删除（协议直接支持条件删除，memcached 的版本号是服务端单调计数器，无 ABA 问题）；被拒说明删除前值又被写过，重读重删。取走和删除之间没有丢失窗口（S11）。

约束：`fn` 必须是纯函数，不能在里面做副作用（可能被执行多次）。这一点写进 docstring 并在文档中反复强调。

## 分层

```
场景层   Memcache / AsyncMemcache     一个场景一扇门，返回业务值
协议层   cache.meta                   meta 协议 1:1：类型化命令 + execute() 自由组装 flag
引擎     连接池、管线化、一致性哈希、quiet 改写、歧义归因（现 exp 的水下部分）
```

一个能力该进哪层的判据只有一条：说得出用户场景的进场景层，说不出的留在协议层。中间不再存在"类型化协议全覆盖"的第三层，那一层没有对应任何用户。

场景层的承诺需要引擎兑现，这里只列要求不设计实现：

1. **lease/recache/stale/条件写与条件删除的完整支持。** factory 状态机、`update`/`pop` 循环直接建立在其上。
2. **歧义写归因。** "请求已写出但结果不可观测"必须与"确定没生效"可区分，它是公理 4 的一部分：用户直发的写原样报出且穿透 degrade（S10），factory 内部写回的歧义进 `on_failure` 不影响返回值（factory 专节）。
3. **绝不自动重试已开始写出的命令。** 盲目重试算术或追加会让变更生效两次，重试只允许发生在确定未写出的阶段。
4. **后台任务生命周期（异步版）。** 客户端持有可取消的根任务域，后台刷新从它派生，close 时全部取消。

## 明确不提供的东西

按同一判据被裁掉的能力，列出来是为了防止它们以"顺手加一下"的方式回流：

- **元数据选择器**（`fields=Field.CAS | Field.TTL`）：字段挑选是多余的自由度，元数据要么不读、要么全读。（inspect 曾整个列于此，后因 S13 排查工具场景以无选择参数的探针形态恢复。）
- **暴露 cas token 的读**（gets 之类）：版本号的消费者只有三个且全在库内，`update` 与 `pop` 的循环、factory 的条件写回；`ItemInfo` 也不再携带（见 S13）。跨请求的乐观并发（读和写分属两个 HTTP 请求）理论上存在，但那是数据库加表单版本号的领域，等真实需求出现再议。
- **结果对象与状态枚举**：GetStatus、MutationStatus、`result.check()` 是把协议状态机转嫁给用户。场景层的每个方法自己消化状态，只输出业务值或异常。
- **操作对象式的混合 batch**：让用户构造 `batch([Get(...), Set(...)])` 命令列表是协议思维。同一轮往返的场景化形态是 S12 的 `pipeline()`，同质批量则有 `_many` 家族。
- （append/prepend 与 replace 都曾列于此，分别因 S11 事件缓冲与 S9 写回场景恢复为场景层方法。）
- **lease / recache / stale 的直接操作**：全部是 factory 读与软 delete 的内脏。
- **独立的 fetch / invalidate 方法**：曾是本文草案的一部分，按合并判据收编为 `get(factory=)`、`delete(grace=)`。（touch 也曾被收编进 `get(extend_ttl=)`，后因大 value 续期场景恢复独立，见 S9。）
- **构造器级 `default_ttl`**：第一稿曾提供，本稿撤销。默认 TTL 有意义的粒度是数据族，本设计没有族级挂载点，全局一档制造远距离耦合，写操作的 ttl 一律必填（理由见 API 总表）。

## 待评估的问题

第一稿的四个问题已由 Go 实施拍板并并入正文，不再列出：`on_error` 默认 `"raise"`（降级必须显式选择，Go 版按此实施无异议）；degrade 下 `update` 仍抛异常（S10）；`incr`/`decr` 不合并（S7）；`ItemInfo` 撤下 version（S13）。仍开放的问题：

1. `prefix` 是否进第一版。场景真实（多应用共享集群、整体换代失效），实现廉价，倾向进。Go 版把前缀连同族级策略一起推迟，理由是它那里只剩全局一档；本设计的构造器一档恰好对应多应用共享集群这个真实场景，结论不迁移。
2. `get` 区分"存了 None"与 miss。factory 路径已天然区分，裸 `get` 用自定义 sentinel 作 `default` 也能区分，倾向不再为此加机制。
3. 命名。关键字参数取代方法名后，压力转移到参数名上：`factory` 备选 `loader`；`grace` 备选 `stale_for`；`update` 备选 `mutate`；`pop` 备选 `take`（Go 版取名 `Take`，Python 里 `dict.pop` 的语感更近）；观测钩子 `on_failure` 与模式开关 `on_error` 名字相近，备选 `on_degrade`/`observer`。`extend_ttl` 不宜叫 `touch`，独立的 `touch` 方法恢复后会撞名。
4. `get` 的参数簇边界。合并后 `get` 有 `default`/`factory`/`ttl`/`refresh_ahead`/`extend_ttl` 五个关键字参数，仍满足合并判据（返回类型始终是值，参数全部是修饰），但这是它的上限：任何会改变返回形态或引入协议词汇的参数（版本号、元数据、批量）都不得再进入 `get`，宁可开新门。
5. `incr`（以及同语义的 `append`）的 ttl 只在创建时生效这一点是否足够显眼。它是正确的窗口语义，但和 `set` 的 ttl 直觉不同，至少要在 docstring 里用限流和事件缓冲的例子讲清楚。Go 版曾为此把参数取名 `Window`，最终仍回归普通 ttl 加文档承载，可作参照。
6. `pipeline` 的延迟结果形态。`.value` 属性最少概念，备选显式 `p.execute()` 返回结果列表（更接近惯例但引入了顺序对应的心智负担）。degrade 模式下延迟结果按表降级、with 内读取直接报错这两点已定，需要确认的是同一 pipeline 内是否允许 `get(factory=)` 这类本身含多轮往返的操作，倾向第一版禁止。
7. 批量防击穿（`get_many` 的 factory）。S2 的示例正是"用户在库外手拼 get 否则算"，几十个 key 同时过期时每个实例都全量重查源头，单 key 有 factory 而多 key 一直缺位（Go 版同缺，已在其待评估问题立项 `FetchMany`）。Python 形态天然是 `get_many(keys, factory=load_missing, ttl=...)`，factory 收 miss 的 key 列表、返回字典，满足合并判据（动词相同、返回类型仍是字典、factory 是修饰）。倾向立项但放第二版，先把单 key 状态机做对。
8. degrade 下 `get_many` 的部分故障。两台服务器挂一台，是整体降级还是只把挂掉那台的 key 当 miss。倾向按服务器局部降级，公理 4 的"缓存故障不等于业务故障"本来就该以最小爆炸半径成立。
9. 同步客户端的刷新路径。异步版后台化之后，同步版的当选者仍要付一次重算延迟（S4）。是否给同步版一个构造器显式开启的后台线程池来对齐，倾向不做：同步库不该偷偷拥有线程，一次可预测的延迟是同步形态的诚实代价。
