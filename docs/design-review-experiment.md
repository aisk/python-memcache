# experiment 模块设计 Review（宏观）

日期：2026-07-27 · 范围：`memcache/experiment/`（meta_api.py、_meta_core.py、meta_client.py、async_meta_client.py、operation.py、result.py）及其与父包的关系。关注设计层面问题，不逐行找 bug。

## 架构概览

分层是清晰的，自下而上：

1. `meta_api.py` — 协议 1:1 映射：纯函数构造 mg/ms/md/ma/me 命令、解析响应 flag。无 IO、无序列化。
2. `_meta_core.py` (`MetaProtocol`) — 语义层：`Operation` → 命令（含 quiet/opaque 管线化改写）、响应 → `Result`（含 barrier/ambiguity 归因）。传输无关。
3. `meta_client.py` / `async_meta_client.py` — 传输层：连接池 `_Server`、一致性哈希路由、批处理执行器，外加两个 API surface（高层便捷方法 + `client.meta` flag-for-flag namespace）。
4. `operation.py` / `result.py` — 请求/结果数据模型。

另外一个关键事实：**主包的 `Memcache`/`AsyncMemcache` 已经是 experiment `MetaClient` 的门面**（memcache/memcache.py:5）。

## 发现

### F1. "experiment" 已成为稳定 API 的地基，命名与地位矛盾（高）

`Memcache` 与 `AsyncMemcache` 完全委托给 `experiment.meta_client.MetaClient`。这意味着 experiment 不再是可以随意 break 的试验场——它的任何行为变化都会透过稳定 API 泄漏（错误类型、超时语义、池行为）。要么尽快让它毕业（改名、定契约），要么把稳定客户端依赖的最小内核抽出 experiment。目前的状态是最差组合：名字承诺"随时会变"，地位却是"动它就 break 用户"。

### F2. sync/async 双镜像 ≈ 1600 行手工同步负担（高）

`meta_client.py`（842 行）与 `async_meta_client.py`（760 行）是近乎逐字的镜像：`_Server`、`MetaNamespace`、全部高层便捷方法（get/set/add/cas/append/…）的签名与 docstring 都重复两份。共享的只有 `MetaProtocol` 与 `meta_api`。协议每加一个 flag，需要同步修改约 7 处：`build_X`、`MetaNamespace.X`、`AsyncMetaNamespace.X`、`operation.py` 的 dataclass、`_prepare_X`、两个 client 的便捷方法。这类库最常见的腐化路径就是 sync/async 语义悄悄漂移（现在已经出现了，见 F7）。建议评估 unasync 式代码生成（async 为源生成 sync），或至少把两个 Namespace 的参数装配收敛为共享的 build 调用表。

### F3. `except BaseException` 与控制流异常冲突（高，已修）

已修：见文末补记。确立的规则正是本条建议的那条，只捕获 `Exception`，控制流异常在清理连接后原样重抛。以下为原始记录。

sync 侧确认成立：`_record_parsed`（_meta_core.py:322）与 `_run_pipelines`（meta_client.py:502-508）把 `KeyboardInterrupt`/`SystemExit` 折叠进 per-key FAILED 结果，batch 正常返回——用户按 Ctrl-C 得到的是一批 FAILED result 而不是中断，控制流上无再抛点。

async 侧初版判断有误，经验证修正：`_run_group`（async_meta_client.py:451）吞掉取消异常并**不会**让 `batch()` 以伪造结果正常返回——anyio/trio 的取消是 level-triggered，宿主任务在 task group `__aexit__` 的 checkpoint 上会再次收到取消并向外抛，最终取消传播是有保证的，代价只是取消被推迟到 `_resolve_group` 跑完。真正违反 anyio/trio 规范的点在更底层：`async_connection.py:103-105` 把包括取消异常在内的 `BaseException` 包装进 `PipelineError`（一个普通 `Exception`），在 shield 或单次投递场景下会真吞取消。

设计上需要一条明确规则：哪些 BaseException 属于"这台 server 的故障"（可折叠进结果），哪些属于"调用者的控制流"（必须重抛）。建议只捕获 `Exception`，取消/中断类异常在丢弃连接后一律重抛。

### F4. 双重错误模型：故障返回值化 vs 异常（中高，主体已由 6e6b589 修复）

**本条大部分已过时。** commit `6e6b589`（2026-08-12，晚于本文撰写日期）让单键 API 的基础设施故障走异常通道，实测 `get`/`set` 在 server 宕机时抛 `OperationFailedError`，稳定层 `Memcache.get`/`Memcache.set` 同样抛出。"故障静默降级为 miss""写入无声丢失"均不再成立。仍成立的部分：`batch` 里 FAILED 依然是返回值（这是刻意的，一批操作需要逐条归因），以及这条分界线仍未写进 README。以下为原始记录。

同一个 client 上有两套错误语义：高层 API 把网络故障做成 `FAILED` 结果（只有 AMBIGUOUS 才抛 `AmbiguousWriteError`），`client.meta.*` 则直接抛异常。后果：`get()` 在 memcached 整体宕机时不抛任何异常，`__bool__` 为 False，与 miss 走同一条 if 分支——故障被静默降级为 miss 是缓存客户端可以接受的哲学，但它必须是显式契约（docstring/README 层面），且"AMBIGUOUS 抛、FAILED 不抛"的不对称目前只存在于私有方法 `_one` 的实现里，任何文档都没有说明。用户很难推断出"写失败要么返回 FAILED 要么抛 Ambiguous"这条规则。盲评方进一步指出这在稳定层被放大：`Memcache.get` 对非 HIT 一律返回 None、`Memcache.set` 直接丢弃返回值，于是 memcached 整体宕机对稳定 API 用户表现为"全部 miss、写入无声丢失"，与 miss 完全不可区分。README 与 docs 中对 FAILED/AMBIGUOUS 契约零提及（grep 验证）。

### F5. write-all-then-read 管线的 TCP 缓冲死锁窗口（原标中，实测应为高，已修）

已修，详见文末补记。实测确认可复现，严重性高于原判：整批以 TimeoutError 失败而非变慢，且阈值随 socket 缓冲瞬时状态浮动，同样规模时好时坏。以下为原始记录。

批处理是先把整个 pipeline 写完再开始读（connection.py:send_pipeline → receive_pipeline），批次足够大时会出现经典管线死锁：server 的响应填满两端 socket 缓冲后阻塞写回，client 的 `sendall` 也阻塞，双方互等直到超时——批次以超时失败而不是变慢。触发门槛由**请求侧字节量**决定（约等于两端 socket 缓冲之和，数百 KB ≈ 单 server 分片数千条 mg 命令），主要风险场景是超大 multiget；quiet set 批次因 q 抑制了 HD 响应、server 几乎不写回，基本免疫（初版"大 value get 批次更易触发"的表述不准确——value 大小只影响 server 端阻塞早晚，不改变请求侧门槛）。sync 版叠加一层：对 N 台 server 顺序写完全部 pipeline 再顺序读，第一台的写阻塞会推迟后面所有 server 的开始时间。设计上缺一个 chunk 上限（按命令数或请求字节数分段，写一段读一段）或至少文档化批次规模上限。

### F6. 连接池无总量上限（中）

`_Server` 池的卖点是"borrower 永不阻塞"：池空就新建连接。这意味着并发尖峰会无上限地建连接，风暴过后又只保留 `max_idle=23`（魔数，无注释解释）个。memcached 是每连接常驻内存、`-c` 有硬上限的服务，客户端无界连接数是运维风险（一个慢 server 让请求堆积 → 连接爆炸 → 顶到 server conn limit 影响同集群其他客户端）。至少应提供可选的 max_active + 等待/快速失败策略；"永不阻塞"可以是默认，但不该是唯一选项。另外池内空闲连接没有健康检查/最大存活时间，仅靠 FIFO 复用保活的论证依赖流量持续不断。

### F7. sync 与 async 的超时与错误面已经漂移（中，async flush_all 无超时已修）

部分已修：`_Server.flush` 现在接收 timeout 并用 `anyio.fail_after` 包裹，`flush_all` 传入 `default_timeout`，与其他操作一致。实测同一挂起的 server，修复前 async 版无限挂起（只有外层 cancel scope 能停），修复后 0.3s 抛 `TimeoutError`。本条其余内容（整批共享 deadline vs 每 server 独立、idle 超时 vs 硬性总时限、错误面差异）未处理，仍待 F2 的收敛策略一并解决。以下为原始记录。

同一 API 两种超时语义：sync batch 用整批共享 deadline（`_deadline`/`_remaining`，跨 server 递减），async 则是每台 server 独立 `anyio.fail_after(timeout)`。错误面也不同：`meta.batch` sync 版逐 server 检查 outcome 并抛第一个 error，async 版在 task group 里抛 `ProtocolError` 会取消其他 server 的进行中管线（甚至可能以 ExceptionGroup 形式浮出）。这正是 F2 预言的漂移：两份实现各自演化，行为差异没有任何文档记录，用户从 sync 迁到 async 时契约悄悄变了。对抗验证与盲评补充了两处：async `flush_all` 完全没有超时（`_Server.flush` 不接收 timeout，`AsyncConnection.flush_all` 无 fail_after，可无限挂起）；async `execute_pipeline` 会把取消异常也包进 `PipelineError`（async_connection.py:103-105），sync 无此行为。另外 sync 单命令的 timeout 是每次 socket 操作的 idle 超时（慢速滴流 server 可无限拉长总时间），async 是硬性总时限——同名参数第三种语义。

### F8. bytes 与 str 键路由不一致（中，已修）

已修：`routing_key` 改为 `key_bytes(key).decode("latin-1")`，两种表示先归一到线上真正发送的字节，再做唯一一次 latin-1 转换。ASCII 键路由不变（5000 个随机 ASCII 键实测零迁移），现有部署不会因此触发缓存失效。回归测试 `test_str_and_bytes_keys_route_to_the_same_server` 在三节点环上断言 300 个非 ASCII 键的 str/bytes 落点一致。以下为原始记录。

`_server_for` 对 bytes 键用 latin-1 解码后喂 hashring。同一个逻辑键的两种表示会路由到不同 server：`"café"` 直接参与哈希，而 `"café".encode("utf-8")` 经 latin-1 解码变成 `"cafÃ©"`——set 用 str、get 用 bytes 的调用方会稳定地 miss（多 server 时）。key_bytes 层已经确立了 "str 即 utf-8 bytes" 的等价关系，路由层却不遵守。路由键应统一走 `key_bytes()` 的输出。

### F9. MetaProtocol 继承 + LeaseResult 闭包回调的耦合（中低，经对抗验证修正）

客户端 IS-A 协议编解码器（`MetaClient(MetaProtocol)`）的继承关系并不自然，`_lease_fulfill` 作为 NotImplementedError 钩子返回 `Callable[..., Any]`：sync 返回 `MutationResult`、async 返回 coroutine，同一个 `LeaseResult.fulfill` 的返回类型只能是 `Any`，静态类型在这条最需要引导用户的路径（lease/recache 正确用法）上完全失效——sync 下误 `await`、async 下漏 `await` 都无静态提示。初版关于"close 后 fulfill 行为无定义"的说法有误：fulfill 走 `self.set` → `batch`，首行有 `_closed` 检查并抛 `RuntimeError`，行为是定义良好的。组合（把 MetaProtocol 作为独立协作对象注入）+ sync/async 各自的 LeaseResult 类型会让签名清晰。

### F10. GetResult 状态空间过大且 API 未定型（中低）

`GetStatus`(6) × `ValueState`(3) × `LeaseState`(3) + `error` + `item` 的组合空间远大于实际合法状态集合，合法组合矩阵只存在于 `_parse_get` 的实现里，类型系统与文档都不表达。叠加"transitional aliases"（cas_token/ttl/is_stale/won_recache/…）——注释自己承认是迁移期兼容——以及从未被绑定的 `Generic[T]`（所有方法都返回 `GetResult[Any]`，泛型纯装饰），信号是结果模型还没定稿。毕业前应：砍别名、写出合法状态矩阵、要么让序列化器参数化 T 要么去掉泛型。

### F11. 两套词汇表无对照文档（低）

协议层与高层刻意用两套命名：`vivify_ttl`/`lease_ttl`、`recache_ttl`/`refresh_before`、`new_cas`/`version`、`compare_cas`/`unless_cas`、`invalidate+ttl`/`stale_for`。分层动机成立（协议名 vs 意图名），但映射关系散落在 `_prepare_*` 的实现里，没有任何一处对照表。跨层调试（wire 抓包对高层调用）和学习成本都靠读源码。一张 flag ↔ 协议名 ↔ 高层名的表就能解决。

### F12. 单键操作强制走 batch 管线（低，待测量）

`_one` 把每个单键请求包成 batch：多付出 opaque flag、q 改写、`mn` barrier 和 `_resolve_group` 的成本。统一执行路径换来单一错误处理是合理交换，但 memcached 客户端的单键 get 是最热路径，值得基准测量后再下结论；若开销可见，可为单键保留直连快路径。对抗验证补充了一个让测量更有必要的事实：全库未设置 `TCP_NODELAY`，且 sync `send_pipeline` 对命令与 `mn` barrier 分两次 `sendall`——quiet miss 的单键 get（miss 时无响应、直到 MN 才有回包）可能吃到 Nagle + delayed-ACK 的叠加延迟。

### F13. `GetResult.value` 在 FAILED 时掩盖真实错误（低，已修）

已修：改为 `raise ... from self.error`，FAILED 结果上访问 `.value` 时 `__cause__` 现在是真实的 `ConnectionRefusedError`/`TimeoutError`，之前是 `None`。以下为原始记录。

FAILED 结果上访问 `.value` 抛出通用的 `ResultValueError("value is only available on a HIT result …")`，不链接 `self.error` 里的真实异常（如 ConnectionRefusedError），把基础设施故障伪装成用法错误，排障时丢失关键信息。建议 `raise ... from self.error`，或 FAILED 时直接抛 `self.error`。

## 值得肯定的设计

- meta_api 纯函数层的 1:1 协议映射、"拒绝协议会静默忽略的组合"的校验哲学（如 append+ttl、initial 无 initial_ttl），注释直接引用真实 server 行为验证（如 me+b64 的实测）。
- ambiguity 归因模型：`written`/`barrier`/`side_effect` 三元组把"哪些写操作结果不可知"精确到单条命令，`AmbiguousWriteError` 与"绝不重连重放"（connection.py:73 注释）的配套是很多成熟客户端都没做对的事。
- quiet + opaque + mn barrier 的管线协议用法正统，且 `needs_success_response` 的判定（Arithmetic、Set+return_cas 不 quiet）与 protocol.txt 语义吻合（md+q 只隐藏 HD，NF 仍返回，None→STORED 推断成立）。
- 连接借还纪律：任何未完成完整请求/响应周期的连接一律丢弃不回池，杜绝脏字节毒害下一个 borrower。

## 对抗性 review 结论（第一轮：宏观设计）

流程：两个独立 subagent 并行——一个对 F1–F12 逐条对抗性反驳（核对代码行号、协议文档、anyio 源码、hashring 实现），一个不看本文件做盲评，交叉比对。

反驳方判定：**8 条 CONFIRMED（F1、F2、F4、F6、F7、F8、F10、F11、F12），4 条 WEAKENED（F3、F5、F9 及 F2 的一处措辞），0 条 REFUTED**；无一条建议方案劣于现状。WEAKENED 的修正已回写进上文对应条目：

- F3：async 取消被吞不会让 `batch()` 伪造结果返回（anyio/trio 取消是 level-triggered，宿主在 task group 出口重新收到取消），真实违规点是 `async_connection.py:103-105` 把取消包进普通 `PipelineError`；sync 吞 KeyboardInterrupt 的部分维持成立。
- F5：死锁门槛由请求侧字节量决定（数千条 mg），与 value 大小无关；quiet set 批次基本免疫。
- F9：close 后 fulfill 有 `_closed` 检查抛 RuntimeError，"行为无定义"子论点撤回；类型失效与继承耦合维持成立。
- F8 补充验证：hashring 内部 `md5(key.encode('utf-8'))`，`"café"` 与 `"café".encode()` 的哈希输入确不相同，交叉 miss 实锤；仅影响非 ASCII 键 + 多 server 场景。

盲评方 12 条发现与本文 F1–F12 高度收敛（独立复现了 F1/F2/F3(sync)/F4/F6/F7/F8/F9/F10/F11/F12 的实质内容），并补充了本文初版遗漏、已并入上文的：稳定层 `Memcache.set` 丢弃返回值放大 F4（并入 F4）、async `flush_all` 无超时与 `PipelineError` 包裹取消（并入 F7）、`GetResult.value` 不链接真实错误（新增 F13）、`get(lease_ttl=...)` 值依赖返回类型多态（留待第二轮 API 专项）。

两轮独立视角均认可的"做对了"结论一致：ambiguity 归因模型 + 不重连重放、quiet/opaque/mn 的协议运用、连接借还纪律、三层分层本身。

---

# 第二轮：API 设计专项

聚焦用户可见的 API surface：签名、命名、返回类型、类型系统承诺、异常契约、发现性。宏观结构问题（分层、sync/async 重复等）见第一轮，不重复。

### A1. `get(lease_ttl=...)` 的值依赖返回类型多态（高，加重事实已过时）

对抗验证补充的那个"加重事实"已被 `6e6b589` 消除：`get_with_lease` 的 FAILED 路径不再返回裸 `GetResult`，而是直接抛 `OperationFailedError`，对返回值调 `.fulfill` 的 AttributeError 场景不存在了。值依赖返回类型多态的主论点未受影响，仍待处理。以下为原始记录。

传了 `lease_ttl` 时 `get()` 实际返回 `LeaseResult`，静态签名仍是 `GetResult[Any]`——返回类型随参数**值**变化，类型检查器无法表达，用户要么盲目 isinstance，要么错过 `fulfill`。应该从 `get()` 中拒绝 `lease_ttl`/`refresh_before` 参数，一条能力一个入口（批处理场景仍可用 `Get(lease_ttl=...)` operation 表达）。现状是同一能力两个入口、其中一个类型不安全。对抗验证发现一个加重事实：`get_with_lease()` 自己的 `LeaseResult` 签名在 FAILED 路径上也是假的——`_failure`（_meta_core.py:169-171）对 Get 一律返回裸 `GetResult`（没有 `fulfill`），网络故障时对返回值调 `.fulfill` 是 AttributeError。缓解因素：docstring、README 示例与测试都只引导用户走 `get_with_lease`。

### A2. 异常契约未定义：调用方不知道要 catch 什么（高）

`client.meta.*` 与构造/借连接路径会把原始 `OSError`（ConnectionRefusedError 等）、`TimeoutError`、`MemcacheError` 家族直接抛给调用方，没有统一的异常基类包装；高层 API 又把大部分故障值化为 FAILED 结果（F4）。一个想写 `except <什么>` 的用户在文档和类型里都找不到答案。成熟客户端库的基本契约是"本库抛出的一切可预期异常都是 `MemcacheError` 的子类"（连接失败包装成 ConnectError 之类），或者明确文档化会透传 OSError。现状两个 surface 各自含糊。

### A3. 结果对象的 `__bool__` 无法区分 FAILED 与 MISS（中，单键路径已由 6e6b589 修复）

单键 API 上 FAILED 已经抛异常而非返回结果，`bool`/`value_or` 与 MISS 不可区分的问题在这条路径上消失。仅 `batch` 返回的 FAILED 结果仍适用。以下为原始记录。

`GetResult` 有五种消费方式：`bool(r)`、`r.status`、`r.value`（非 HIT 抛 ResultValueError）、`r.value_or(default)`、`r.error`。初版"缺主推路径"的批评被部分驳回：`__bool__ == HIT` 是自洽的"有可用值"谓词，UNCHANGED/PENDING 本就无值、返回 False 是正确设计；AMBIGUOUS 在高层单键 API 上不可达（`_one` 直接抛 AmbiguousWriteError）；README 示例统一用 `result.status is GetStatus.HIT` 显式检查，主推路径事实上存在、只是没写成规范文字。维持成立的核心：**FAILED 经 `bool`/`value_or` 与 MISS 不可区分**（F4 的 API 层根源），这需要一个设计决策而非顺其自然。

### A4. `mode: str` 魔法字符串与 `value: Any` 的运行时陷阱（中）

`set(mode="append")` 的 mode 是裸 str，拼错要到运行时才 ValueError——应为 `Literal["set","add","replace","append","prepend"]`，零成本获得 IDE 补全与静态检查。更深一层：`set(value: Any)` 但 append/prepend 模式运行时要求 bytes（否则 TypeError），类型签名无法表达"value 的合法类型取决于 mode 的值"——这正说明 append/prepend 不该是 set 的一个 mode 参数，独立方法（已存在 `append()`/`prepend()`，签名正确地要求 bytes）才是唯一入口，`set()` 应只保留 set/add/replace。

### A5. 便捷方法之间三重重叠（中低，经对抗验证降级）

同一件事有三个入口：`add(key, v)` ≡ `set(key, v, mode="add")` ≡ `meta.set(key, raw, mode="add")`；`cas()` ≡ `set(compare_cas=...)`；每个便捷方法背着 8–10 个参数（`increment` 含 timeout 有 10 个），`version`、`vivify_ttl` 等小众旗标出现在全部签名里。对抗验证指出初版定性有误：README 明确文档化了两梯度设计（core methods 全协议覆盖 + convenience wrappers 安全默认值），这是有意决策而非腐化；且"长尾走 namespace"有隐藏成本——namespace 只收 raw bytes 不做序列化。重叠与签名膨胀的事实保留，作为"接受的取舍"记录，但收敛建议需与既定设计哲学一起重新权衡，不是显然的改进。

### A6. `version` 参数缺少危险性警示（低，经对抗验证改判）

初版认为高层把 E flag 叫 `version` 是"危险旗标配无害名"，被 protocol.txt 反驳：协议文档自己对 E flag 的描述就是 "useful for using an external system to version cache data (row versions, clocks, etc.)"——`version` 恰是协议给出的用途命名，改回 `new_cas` 反而丢失意图。维持成立的部分：它直接覆写服务器 CAS、用错会破坏其他调用方的 CAS 并发控制，而高层 docstring 对此零警示——补文档，不改名。

### A7. `Field` IntFlag 与 namespace 布尔参数两种选择风格（低，经对抗验证收窄）

高层用 `fields=Field.CAS | Field.TTL`，namespace 层用 `return_cas=True, return_ttl=True`，两种风格并存增加跨层学习成本——这部分维持。初版"None 无法区分没请求和服务器没返回"被驳回：调用方永远知道自己请求了什么，且 mg 的元数据 flag 是请求驱动回显、请求了的 HIT 响应必然带回，"请求了但没返回"的正常场景不存在。IntFlag 还有一个初版没算的收益：`_prepare_get` 用 `requested |= Field.CAS` 为 lease/unless_cas 自动追加依赖。风格统一仍值得考虑，优先级降低。

### A8. `BatchResult` 是个没有增值的 tuple 包装（中低）

`BatchResult` 只实现了 Sequence 协议，`__getitem__` 返回 `Any`；输入 `Get` 得到 `GetResult` 这个对应关系静态上完全丢失，用户按索引取回来还要 cast。批处理是这个客户端的招牌能力，结果容器却没有：按 key 索引、`.failures`/`.ok` 过滤、与输入 operation 的 zip、类型化的 overload（`batch(*ops: Get) -> tuple[GetResult, ...]` 之类）。要么给它增值方法，要么直接返回 tuple 别引入新类型。

### A9. 二进制 key 自动 base64（大部分被对抗验证驳回，仅余文档缺口）

初版三个论点被驳回两个半：(1) 超长 key 并不会被 base64，而是直接抛 MemcacheError（meta_command.py:31-35），只有含空格/控制字符的 key 才 b64；(2) "服务器端看到的 key 与传入不同"与协议相反——b flag 让服务器先解码再查找，存储的就是原始二进制 key，lru_crawler mgdump 也原生支持该格式，b64 透传是协议的标准机制；(3) 行为已在 meta_api.py 与 meta_command.py 的 docstring 里说明。"显式 opt-in"的建议劣于现状，撤回。剩余成立：README 层面没有提及这一行为；`me` debug 对 base64 key 的服务器端缺陷客户端已正确防御（build_debug 显式拒绝并注明 1.6.45 实测）。

### A10. `default_timeout` 是可变公共属性，`timeout=None` 语义过载（低）

`client.default_timeout` 可随时改（无校验）；每个方法的 `timeout: float | None = None` 里 None 表示"用默认值"，导致用户无法表达"这一次调用不要超时"——None 被占用了，真正的"无限等待"只能靠把 default_timeout 设成 None 全局生效。惯用解法是 sentinel 默认值（`timeout: float | None | Unset = UNSET`）。

## 对抗性 review 结论（第二轮：API 设计）

流程同第一轮：一个 subagent 对 A1–A10 逐条反驳（核对代码、README、tests、protocol.txt），一个不看本文件做盲评 API surface。

反驳方判定：**5 条 CONFIRMED（A1、A2、A4、A8、A10），5 条 WEAKENED（A3、A5、A6、A7、A9），0 条 REFUTED**。修正已回写进上文对应条目，其中改动最大的：A6（`version` 命名有协议文档背书，改判为只缺警示文档）、A9（base64 语义初版理解有误，大部分撤回）、A5（两梯度 API 是 README 文档化的有意设计，降级为已接受的取舍）。反驳方还发现一个加重 A1 的新事实：`get_with_lease` 的 FAILED 路径返回没有 `fulfill` 的裸 `GetResult`，其静态签名同样不成立。

盲评方独立复现了 A1、A2（异常契约速查表更完整）、A4、A8 的实质内容，并贡献了本文没有的发现，按严重度并入如下：

### B4. `get_many` 返回 dict 的键与调用方传入的 key 不一致（中，已修）

已修：`get_many` 直接用 `r.key` 作为结果 dict 的键（`r.key` 本就是调用方传入的原始对象），返回标注相应放宽为 `dict[bytes | str, Any]`。sync 与 async 两侧同步修改。以下为原始记录。

bytes key 被 latin-1 解码成 str 作为结果 dict 的键（memcache.py:139），`get_many([b"\xff-key"])` 的调用者用原始 key 查不到结果，且与 F8 的路由问题纠缠。应以调用方传入的原始 key 对象为键。

### B5. 稳定层与实验层默认序列化器安全姿态相反（中低）

`Memcache` 默认 pickle（可反序列化任意外来数据），`MetaClient` 默认 StrictSerializer（拒绝外来 pickle）。同一个库两个入口的安全默认值相反，至少需要醒目文档。

### B6. 命名摩擦清单（低）

`get(value: bool)` 与 `set(key, value)` 的 value 撞名，建议 `include_value`；便捷方法有 add/cas/append/prepend 却缺 `replace()`，不对称。

已修：原本列在这里的 `Meta` IntFlag 与 `client.meta` namespace 撞名。协议自己的词汇在本库都已另有所指（`flags` 指 item 的 client flags，`token` 已被 `result.cas_token` 占用），所以按语义命名：IntFlag 改为 `Field`，参数改为 `fields=`，承载返回值的 `ItemMeta` 改为 `ItemFields`，请求端 `Field.CAS` 与响应端 `result.item.cas` 一一对应。`value` 将来若并入 `Field.VALUE`，可顺带解决上一条撞名。

## 总结（两轮合并后的行动优先级）

**改行为（毕业前必须决策）**，四项中三项已了结，详见文末三则补记。已修：F8+B4（key 路由归一化）、F3（吞掉中断与取消）。已由 6e6b589 提前修复：F4 的单键 FAILED 抛异常与稳定层吞错、A1 的 FAILED 路径子问题。仍待决策：A2（异常基类统一）、稳定层超时、A1 主论点（`get` 撤下 lease 参数）、F4 剩下的文档缺口（FAILED/AMBIGUOUS 契约进 README）。

**改结构（趁 experiment 名分还在）**：F1（experiment 毕业或内核下沉）、F2+F7（sync/async 收敛策略 + 签名一致性测试护栏）、F9/盲评#4（拆 AsyncLeaseResult）、F10+A8（结果模型定稿：砍别名、泛型去留、BatchResult 增值）。

**低成本高收益**：A4（Literal mode）、A10（UNSET sentinel）、~~F13（value 错误链）~~ 已修、F11+A6+B5+B6（文档：词汇对照表、错误契约、E flag 警示、序列化器差异）、F6（池上限选项）、~~F5（batch 分块上限）~~ 已修，且实测后从本档升级为高危。

## 补记：F8 + B4 已修（2026-08-13）

验证过程与结论：

问题不在 hashring。`gen_key` 对传入的 str 做 `md5(key.encode('utf-8'))`，行为确定且自洽，它唯一的约束是只接受 str（传 bytes 直接 AttributeError）。根因是本库为满足这个约束而在路由层单独做 `bytes --latin-1--> str`，与 wire 层的 `str --utf-8--> bytes` 不构成往返，于是同一逻辑键的两种表示在环上分叉。

实测比原始记录更严重。两台 memcached 上，300 个非 ASCII 键有 140 个路由分叉；且不止是"稳定 miss"，而是同一 key 在两台上各存一份互不可见的副本，`set(str)` 之后 `set(bytes)` 不构成覆写，`delete(str)` 也清不掉 bytes 那份，缓存失效协议（先写库再删缓存）在混用表示时会彻底失灵。

修复范围：`_meta_core.routing_key` 一处，`Memcache.get_many` 与 `AsyncMemcache.get_many` 各一处。新增 3 个回归测试，已确认回滚修复后它们全部失败。全量 118 passed，mypy clean。

兼容性：`get_many` 返回类型从 `dict[str, Any]` 放宽为 `dict[bytes | str, Any]`，依赖旧的"bytes 键静默变 str 键"行为的调用方需要跟进。

原始记录中"路由键应统一走 `key_bytes()` 的输出"的建议成立，已按此实施。反方向（让 bytes 向 str 靠拢，`key_bytes(key).decode("utf-8")`）行不通，任意二进制 key 无法 utf-8 解码。

## 补记：F3 已修（2026-08-13）

原始记录对 sync 侧的判断成立，且实测比记录更直白。用一个只 accept 不回应的 server 加真实 SIGINT 复现：`batch` 吞掉 `KeyboardInterrupt` 正常返回两条 FAILED，程序继续往下跑，Ctrl-C 完全失效；单键 `get` 则把 `KeyboardInterrupt` 重分类成 `OperationFailedError` 抛出，异常类型被掉包。async 侧 `execute_pipeline` 实测把 `CancelledError` 包进 `PipelineError`（普通 `Exception`），确认违反结构化并发规范。

修复按本条建议的规则统一：只有 `Exception` 算"这台 server 的故障"，可以被值化成 FAILED 结果或包装成 `PipelineError`；非 `Exception` 的 `BaseException`（`KeyboardInterrupt`、`SystemExit`、`CancelledError`、`trio.Cancelled`）属于调用者的控制流，在丢弃连接、标记断连等清理动作做完后原样重抛。

改动覆盖 7 处吞异常点：`connection.send_pipeline`、`connection.receive_pipeline`、`async_connection.execute_pipeline`、`_InFlight.finish`、`_run_pipelines`、`_record_parsed`、`_run_group`，外加 4 处 `close` 清理路径上的 `except BaseException: pass`。审计后剩余的 `except BaseException` 全部是"清理后无条件重抛"的正确形态。新增 3 个回归测试，已确认回滚后全部失败。121 passed，mypy clean。

## 补记：早期 commit 已修的条目（复核于 2026-08-13）

本文写于 2026-07-27，此后的几个 commit 已经修掉了部分结论，复核时逐条实测确认：

`6e6b589 Raise infrastructure failures instead of hiding them in a status`（2026-08-12）让单键 API 的基础设施故障走异常通道。实测 `MetaClient.get`/`set` 与稳定层 `Memcache.get`/`set` 在 server 宕机时一律抛 `OperationFailedError`。这直接推翻了 F4 的核心论断（故障静默降级为 miss、稳定层写入无声丢失），消除了 A3 在单键路径上的全部问题，也消除了 A1 那个"`get_with_lease` 的 FAILED 路径返回没有 `fulfill` 的裸 `GetResult`"的加重事实。`batch` 返回值里保留 FAILED 是刻意设计，不在此列。

`1057bbb Rename the item metadata selector from Meta to Field`（2026-08-13）修掉了 B6 记录的 `Meta` 与 `client.meta` 撞名，该条已在原位标注。

结论：原文的"改行为（毕业前必须决策）"清单现已只剩 F4 的文档缺口（FAILED/AMBIGUOUS 契约未进 README）、A2（异常基类统一）、A1 主论点（`get` 撤下 lease 参数）。

## 补记：F5 已修（2026-08-14）

原始记录判定为"中"，实测应为高危。用 236 字节 key 加 1 KiB value 在单台 memcached 上复现，请求侧约 3.8 MiB（16000 条 mg）即触发，整批返回 FAILED 且 error 全是 TimeoutError：

```
16000 mg (~3.8 MiB req) -> 16000 FAILED / TimeoutError  5.81s
20000 mg (~4.8 MiB req) -> 20000 FAILED / TimeoutError  6.00s
```

这台机器 `SO_SNDBUF` 2.6 MiB、server `SO_RCVBUF` 128 KiB，与"门槛约等于两端缓冲之和"的判断吻合。三点原始记录没有涵盖的观察：

阈值不稳定。同样 16000 条，在一串递增批次之后跑是 0.78s 全 HIT，单独跑就死锁，取决于 socket 缓冲的瞬时状态。这意味着故障在生产中表现为偶发而非确定性，更难归因。

失败模式是整批超时，不是降级。原始记录已指出这点，实测确认，且 sync 的超时是每次 socket 操作的 idle 超时（F7），所以实际挂起时间可以远超名义 timeout。

高命中率放大风险。全 miss 的批次因 quiet 抑制回写而免疫，只有 server 大量回写时才死锁，而高命中率正是缓存的正常状态。

修复按原始记录建议的方向实施，按请求字节数分块（`MAX_PIPELINE_CHUNK_BYTES` 512 KiB），写一块读一块。sync 侧 `start_pipeline` 只写第一块以保持跨 server 交错，其余块在 `_InFlight.finish` 里边读边写；async 侧直接在 `execute_pipeline` 内循环。

归因模型的扩展是这次改动的关键风险点。原先 `_resolve_group` 用单一的 `barrier` 布尔判断"缺响应是 quiet 抑制还是失败"，分块后一个 batch 有多个 barrier，早期块可能已完全应答而后续块失败。为此 `PipelineError` 增加 `confirmed` 字段记录已过 barrier 的命令前缀，`_resolve_group` 对 `position < confirmed` 的命令按 barrier 语义处理。回归测试 `test_chunks_already_answered_are_not_reported_ambiguous` 专门覆盖这条：移除 `confirmed` 判断后，早期块的已确认结果全部被误报为 FAILED/AMBIGUOUS，测试失败。

修复后同一复现全部通过，并压测到 60000 条 / 14.3 MiB 请求 / 41 MiB 响应无死锁，async 侧同样。压测中出现的 MISS 经 `stats` 确认是 memcached 的 LRU 驱逐（`evictions 18005`，64 MB 上限），与客户端无关。125 passed，mypy clean。

## 补记：F7（部分）与 F13 已修（2026-08-14）

F13 是一行改动：`GetResult.value` 在非 HIT 时 `raise ResultValueError(...) from self.error`。修复前 FAILED 结果上访问 `.value` 抛出的 `ResultValueError` 其 `__cause__` 与 `__context__` 都是 `None`，真实的 `ConnectionRefusedError` 只存在于 `result.error` 属性里，traceback 读起来像用法错误。注意 `6e6b589` 之后单键 API 的 FAILED 已走异常通道，所以这条现在只影响 `batch` 返回的结果，影响面比原始记录小。

F7 只修了其中最像 bug 的一条。`_Server.flush` 原本连 timeout 参数都不接收，`AsyncConnection.flush_all` 也没有任何 `fail_after`，于是 async 的 `flush_all` 是全库唯一没有截止时间的操作。实测对一个只 accept 不回应的 server，sync 版 1.0s 抛 `TimeoutError`，async 版 3s 后仍在挂，只有外层 cancel scope 能停下它。现在改为与 `pipeline`/`execute` 相同的写法，并由 `flush_all` 传入 `default_timeout`。

回归测试用一个只 accept 不回应的监听 socket，断言 `flush_all` 在 0.3s 内抛 `TimeoutError`。这个测试的有效性验证方式特别直接：回滚修复后跑它，测试进程直接挂死到超时被杀，正是原 bug 的表现。

F7 剩下的部分没有动，它们不是孤立 bug 而是 sync/async 两份实现各自演化的结果，需要与 F2 的收敛策略一起决策：整批共享 deadline（sync）对每 server 独立 `fail_after`（async）、每次 socket 操作的 idle 超时（sync）对硬性总时限（async）、以及 `meta.batch` 两侧错误面的差异。

127 passed，mypy clean。
