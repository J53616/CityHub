<div align="center">
  <h1>CityHub - 本地生活服务平台</h1>
  <p>类"大众点评"本地生活服务平台 · 面向高并发秒杀场景的缓存与消息队列深度优化实践</p>
  <p>
    <img src="https://img.shields.io/badge/Spring%20Boot-3.2-green" alt="Spring Boot">
    <img src="https://img.shields.io/badge/MySQL-8-blue" alt="MySQL">
    <img src="https://img.shields.io/badge/Redis-7-red" alt="Redis">
    <img src="https://img.shields.io/badge/MyBatis--Plus-3.5-orange" alt="MyBatis-Plus">
    <img src="https://img.shields.io/badge/RocketMQ-5.1-brightgreen" alt="RocketMQ">
    <img src="https://img.shields.io/badge/Redisson-3.13-purple" alt="Redisson">
    <img src="https://img.shields.io/badge/License-MIT-yellow" alt="License">
  </p>
</div>

---

## 目录

- [项目简介](#项目简介)
- [技术栈](#技术栈)
- [功能特性](#功能特性)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [核心设计](#核心设计)
- [性能与稳定性](#性能与稳定性)
- [实现状态清单](#实现状态清单)
- [后续计划](#后续计划)
- [License](#license)

---

## 项目简介

CityHub 是一个类"大众点评"的本地生活服务平台，提供商家信息查询、优惠券秒杀、推广信息、用户互动（点赞 / 关注 / 签到 / 附近商户）等能力。

项目的重点不在于业务功能本身，而在于 **高并发秒杀场景下，如何把单体系统从"能用"优化到"抗压"**。围绕秒杀链路的性能瓶颈，基于 **缓存架构 + 消息队列** 进行了深度优化，解决了超卖、缓存穿透 / 击穿、数据一致性等核心问题。

### 秒杀链路优化演进

| 阶段 | 方案 | 问题 |
|---|---|---|
| 1.0 | 数据库悲观锁（`select ... for update`） | 请求串行化，性能极差，连接池瞬间被打满 |
| 2.0 | 数据库乐观锁（`update ... where stock > 0`） | 解决超卖，但 DB 扛不住高并发；"一人一单"在集群下失效 |
| 3.0 | **Redis + Lua 原子扣减 + RocketMQ 异步落库（最终方案）** | 毫秒级 Redis 操作完成资格校验与扣减，MQ 削峰异步写库 |

---

## 技术栈

| 组件 | 版本 | 用途 |
|---|---|---|
| Spring Boot | 3.2.5 | 应用框架（JDK 17+） |
| MySQL | 8.x | 业务数据存储 |
| Redis | 6+ / 7 | 缓存、秒杀库存、限流、分布式锁、GEO、BitMap、HyperLogLog |
| Lua | - | 秒杀库存扣减、滑动窗口限流的原子脚本 |
| MyBatis-Plus | 3.5.5 | ORM，简化 CRUD 与分页 |
| RocketMQ | 4.x / 5.x（starter 2.3.3） | 秒杀下单异步解耦、延迟消息 |
| Redisson | 3.13.6 | 分布式锁（可重入 + 看门狗自动续期） |

---

## 功能特性

### 业务功能

- 商家信息查询 / 新增 / 更新，按类型、按名称、按地理位置（GEO 5km 内）查询
- 优惠券秒杀（核心），支持秒杀时间校验、一人一单、不超卖
- 推广博客：发布 / 点赞（ZSet 排行榜）/ 关注 / 好友 Feed 流（推模式）
- 用户体系：短信验证码登录、Token 刷新、签到（BitMap）、UV 统计（HyperLogLog）

### 高并发优化亮点

| 亮点 | 方案 | 代码位置 |
|---|---|---|
| 不超卖 + 一人一单 | Redis + Lua 原子脚本 | `seckill.lua` |
| 下单异步削峰 | RocketMQ 异步消息解耦落库（立即投递） | `VoucherOrderServiceImpl`、`SeckillVoucherListener` |
| 缓存击穿 | 逻辑过期 + 互斥锁 + 线程池异步重建 | `CacheClient#queryWithLogicalExpire` |
| 缓存穿透 | 缓存空值 + 短 TTL | `CacheClient#queryWithPassThrough` |
| 接口限流 | 滑动窗口（注解 + AOP + Lua），支持全局 / IP / 用户维度 | `RateLimitAspect`、`rateLimit.lua` |
| 超时关单 | RocketMQ 延迟消息精准触发 + Spring Task 定时扫描兜底，乐观锁释放库存 | `OrderTimeoutListener`、`OrderTimeoutTask` |
| 数据一致性对账 | 定时比对 Redis 与 DB，以 Redis 为准修正 | `StockReconcileTask` |
| 缓存删除补偿 | 删缓存失败发 RocketMQ 重试 + 物理 TTL 兜底 | `ShopServiceImpl#update`、`CacheDeleteListener` |
| 支付回调 | 乐观锁状态流转，与超时关单并发互斥 | `VoucherOrderServiceImpl#payCallback` |
| 全局唯一 ID | 时间戳 + 自增序列号（64 位） | `RedisIdWorker` |
| 分布式锁 | 自研 SETNX 锁 → Redisson（可重入 + 看门狗） | `SimpleRedisLock`、`RedissonConfig` |

---

## 快速开始

### 环境要求

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 17+（仓库用 21 验证） | Lombok 需 ≥ 1.18.30 才支持 JDK 21 |
| Maven | 3.x | 可直接使用 IDEA 内置 Maven |
| MySQL | 8.x | 库名 `dingping`，脚本见 `src/main/resources/db/hmdp.sql` |
| Redis | 6+ | 若设了密码，需同步到配置 |
| RocketMQ | 4.x / 5.x | NameServer `127.0.0.1:9876` |

### 启动步骤

1. 启动 RocketMQ NameServer + Broker（端口 9876 / 10911）。
2. 启动 MySQL、Redis。
3. 执行 `src/main/resources/db/hmdp.sql` 初始化数据库。
4. 修改 `src/main/resources/application.yaml` 中的数据库 / Redis / RocketMQ 连接信息。
5. 运行 `com.hmdp.HmDianPingApplication`，访问 `http://localhost:8081`。

---

## 项目结构

```
src/main/java/com/hmdp/
├── annotation/        # 自定义注解（@RateLimit）
├── aspect/            # AOP 切面（RateLimitAspect 滑动窗口限流）
├── config/            # 配置类（Mvc / MyBatis / Redisson / 全局异常）
├── controller/        # Web 层（Shop / Voucher / VoucherOrder / User / Blog ...）
├── dto/               # 传输对象（Result / UserDTO ...）
├── entity/            # 实体（Shop / Voucher / VoucherOrder / SeckillVoucher ...）
├── enums/             # 枚举（LimitType 限流维度）
├── exception/         # 业务异常（RateLimitException）
├── interceptor/       # 拦截器（登录校验 / Token 刷新）
├── job/               # 定时任务（OrderTimeoutTask 关单 / StockReconcileTask 对账）
├── listener/          # MQ 消费者（SeckillVoucherListener 秒杀订单落库）
├── mapper/            # MyBatis-Plus Mapper
├── service/           # 业务接口 + 实现
├── utils/             # 工具类（CacheClient / RedisIdWorker / RedisConstants / UserHolder ...）
└── resources/
    ├── db/hmdp.sql    # 数据库脚本
    ├── seckill.lua    # 秒杀原子扣减脚本
    ├── rateLimit.lua  # 滑动窗口限流脚本
    └── application.yaml
```

---

## 核心设计

### 1. 秒杀：Redis + Lua 原子扣减 + RocketMQ 异步落库

秒杀链路拆成"判断资格（Redis）→ 异步落库（MQ）"两步，核心逻辑：

```lua
-- seckill.lua：原子执行，天然避免并发竞态
local stock  = redis.call('get', stockKey)               -- 库存判断
if (stock <= 0) then return 1 end
if (redis.call('sismember', orderKey, userId) == 1) then -- 一人一单
    return 2
end
redis.call('incrby', stockKey, -1)                       -- 扣库存
redis.call('sadd', orderKey, userId)                     -- 记录用户
```

- **库存**：`seckill:stock:{voucherId}`（String）；**已购用户**：`seckill:order:{voucherId}`（Set）。
- Lua 在 Redis 中原子执行"库存判断 + 一人一单 + 扣减 + 记录"，**无需分布式锁**。
- 校验通过后发 RocketMQ 落库消息（**立即投递**，仅解耦削峰），立即返回订单号给前端，消费者异步落库。

### 2. 消息可靠性：幂等、丢失、最终一致

- **幂等**：订单 ID 为主键，重复消费写入失败天然跳过。
- **丢失兜底**：核心约束（不超卖 + 一人一单）由 Redis + Lua 保证，不受 MQ 丢失影响；DB 副本漂移由 **对账任务** 定期修正（见 §6）。
- **落库不延迟（关键取舍）**：落库消息 `delayLevel=0` 立即投递，订单尽快落库可见——否则用户秒付时 DB 尚无订单，支付回调会查不到订单。真正需要"延迟"的只有超时关单（见 §5，`delayLevel=14`）。原 RabbitMQ 版的 10s 延时落库为历史遗留，RocketMQ 版已去掉。

### 3. 缓存：穿透 / 击穿 / 雪崩

| 问题 | 方案 | 取舍 |
|---|---|---|
| 穿透（查不存在 key） | 缓存空值 + 短 TTL（`CACHE_NULL_TTL`） | 相比布隆过滤器无误判率、无新组件依赖 |
| 击穿（热点 key 失效瞬间） | 逻辑过期（TTL 存 value）+ 互斥锁 + 线程池异步重建 | 保可用性，牺牲瞬时一致性（AP） |
| 雪崩（大量 key 同时失效） | TTL 加随机值 + 热点逻辑过期 + 降级 | - |

### 4. 滑动窗口限流（注解 + AOP + Lua）

```java
@RateLimit(time = 1, count = 5, limitType = LimitType.IP) // 单 IP 每秒最多 5 次
public Result seckillVoucher(@PathVariable("id") Long voucherId) { ... }
```

- 切面 `RateLimitAspect` 拦截 `@RateLimit` 注解，执行 `rateLimit.lua`。
- Lua 滑动窗口：`ZREMRANGEBYSCORE` 清窗口外 → `ZADD` 记当前时间戳 → `ZCARD` 统计，原子计数，杜绝并发超限。
- 支持 `DEFAULT`（全局）/ `IP` / `USER` 三维度，超限返回 HTTP 429。

### 5. 超时订单自动关闭（延迟消息 + 定时兜底）

**双保险机制**：

1. **RocketMQ 延迟消息精准触发（主）**：秒杀下单时发送一条 `delayLevel=14`（≈10 分钟）的延迟消息到 `seckill-order-close-topic`，到期由 `OrderTimeoutListener` 消费并检查关单——在超时时刻精准触发，避免定时轮询的轮询间隔误差。
2. **Spring Task 定时扫描（兜底）**：`OrderTimeoutTask` 每 1 分钟扫描超时（10 分钟）未支付订单，兜底延迟消息丢失 / 消费失败的场景。

两路统一走 `IVoucherOrderService#closeTimeoutOrder`（**乐观锁 + 幂等**）三步释放库存：

1. `update tb_voucher_order set status=4 where id=? and status=1` —— 与支付回调互斥，只关未支付
2. `update tb_seckill_voucher set stock=stock+1 where voucher_id=?` —— DB 库存回补
3. Redis 回补：`INCR seckill:stock` + `SREM seckill:order` —— 释放一人一单资格

定时扫描依赖 `tb_voucher_order(status, create_time)` 联合索引，避免全表扫描。

### 6. 数据一致性对账

`StockReconcileTask` 每 10 分钟比对所有秒杀券的 Redis 与 DB：

- **比对项**：Redis 库存 / Redis 下单集合大小 vs DB 库存 / DB 有效订单数（未取消）。
- **修正**：以 Redis 为准回写 DB 库存（Redis 是不超卖的唯一权威）。
- **告警**：Redis 下单数 > DB 有效订单数 → 疑似 MQ 消息丢失，输出 ERROR 日志人工复核。

### 7. 缓存删除失败补偿（Cache Aside 一致性）

更新 DB 后删除缓存失败会导致旧数据长期驻留（本仓库店铺缓存用的是逻辑过期，无物理 TTL，风险更大）。`ShopServiceImpl#update` 的删除缓存步骤做了三层兜底：

1. **同步删除**：DB 更新后立即 `delete` 缓存。
2. **RocketMQ 补偿**：删除失败时发消息到 `cache-delete-topic`，`CacheDeleteListener` 消费后重试删除，失败抛异常由 RocketMQ 重投。
3. **物理 TTL 兜底**：写入逻辑过期缓存时同时设置"逻辑过期 + 60s 余量"的物理 TTL（`CacheClient#setWithLogicalExpire`），即使补偿也失败，缓存最终也会被物理过期淘汰，保证最终一致。

### 8. 支付回调乐观锁（并发状态流转）

支付成功回调与超时关单可能并发。利用 **数据库行锁 + CAS 条件** 实现"二选一"：

- 支付回调：`update tb_voucher_order set status=2 where id=? and status=1`（未支付 → 已支付）
- 超时关单：`update tb_voucher_order set status=4 where id=? and status=1`（未支付 → 已取消）

两个 SQL 同时执行时只有一方更新成功。若支付回调落在已取消订单上（影响 0 行），触发**原路退回**（退款），避免"用户已付款但库存被释放"的超卖风险。参见 `VoucherOrderServiceImpl#payCallback`。

### 9. 全局唯一 ID 与分布式锁

- **RedisIdWorker**：41 位时间戳 + 按天自增序列号组成的 64 位 ID（类雪花），替代数据库自增，避免分布式重复。
- **分布式锁**：自研 `SimpleRedisLock`（SETNX + Lua 释放）→ 演进为 Redisson（可重入 + 看门狗自动续期）。秒杀下单链路因 Lua 原子性不再需要分布式锁，锁仍用于其他需串行的场景。

---

## 性能与稳定性

- **削峰**：秒杀写路径从"两次 DB 写"变为"一次 Redis 原子操作 + 一条 MQ 消息"，核心接口吞吐量不再受 DB 连接池瓶颈制约。
- **抗超卖**：库存扣减与一人一单校验在 Redis 原子完成，不依赖 DB 行锁。
- **缓存保护**：穿透 / 击穿均有兜底，热点 key 失效不击穿 DB。
- **限流防护**：刷券、爬虫等异常流量在入口被滑动窗口拦截，防止系统过载。

> 说明：仓库不内置压测脚本与性能数据。若要量化"优化效果"，建议配合 JMeter 对本机秒杀接口做并发压测，对比 MySQL 直连与 Redis+Lua 两版的吞吐 / RT 数据。

---

## 实现状态清单

> 与仓库代码严格对应，如实区分「已实现」与「设计思考」。

| 设计 | 状态 |
|---|---|
| Redis + Lua 秒杀（不超卖 / 一人一单） | ✅ 已实现（`seckill.lua` + `VoucherOrderServiceImpl`） |
| RocketMQ 异步下单（异步落库削峰） | ✅ 已实现（`RocketMQTemplate` + `SeckillVoucherListener`） |
| 全局唯一 ID | ✅ 已实现（`RedisIdWorker`） |
| 逻辑过期缓存（防击穿） | ✅ 已实现（`CacheClient#queryWithLogicalExpire`） |
| 缓存空值（防穿透） | ✅ 已实现（`CacheClient#queryWithPassThrough`） |
| 滑动窗口限流（注解 + 切面 + Lua） | ✅ 已实现（`RateLimitAspect` + `rateLimit.lua`） |
| 超时订单定时关单（Spring Task） | ✅ 已实现（`OrderTimeoutTask`） |
| 秒杀数据对账（Redis 权威修正） | ✅ 已实现（`StockReconcileTask`） |
| 缓存删除失败 MQ 补偿 + 物理 TTL 兜底 | ✅ 已实现（`ShopServiceImpl#update` + `CacheDeleteListener`） |
| 支付回调乐观锁（状态流转 / 退款） | ✅ 已实现（`VoucherOrderServiceImpl#payCallback`） |
| 分布式锁（SETNX → Redisson） | ✅ 已实现（`SimpleRedisLock` + Redisson） |
| 登录 / 点赞 / 关注 / 附近 / 签到 / UV | ✅ 已实现 |
| Caffeine 本地二级缓存 | ⬜ 设计思考，代码未实现（见「后续计划」） |

---

## 后续计划

以下方案针对 **更大规模部署场景**（多实例、更高并发、更强风控），当前单体架构下暂未落地，README 记录设计取舍，可随时按需实现：

- **Caffeine 本地二级缓存**：Redis 前加本地缓存，进一步降低热点 key 的 Redis 压力。
- **RocketMQ 事务消息 + 本地消息表**：将"下单"与"发消息"强一致，替代当前对账兜底。
- **支付渠道对接**：支付回调目前为模拟接口，未接真实支付宝 / 微信的验签、退款等能力。

---

## License

本项目为学习 / 面试用途，业务逻辑参考经典"大众点评"系统。基于 [MIT](LICENSE) 协议开源。
