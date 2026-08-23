<div align="center">
  <h1>CityHub - 本地生活服务平台</h1>
<p>
    <img src="https://img.shields.io/badge/Spring%20Boot-3.2-green" alt="Spring Boot">
    <img src="https://img.shields.io/badge/RocketMQ-5.1-blue" alt="RocketMQ">
    <img src="https://img.shields.io/badge/Redis-7-green" alt="Redis">
    <img src="https://img.shields.io/badge/MyBatis--Plus-3.5-red" alt="MyBatis-Plus">
    <img src="https://img.shields.io/badge/Redisson-3.13-orange" alt="Redisson">
    <img src="https://img.shields.io/badge/License-MIT-yellow" alt="License">
  </p>
</div>

<br/>

**CityHub** 是一个类"大众点评"的本地生活服务平台。本仓库的重心不在业务功能本身，而在于**高并发秒杀场景下，如何把单体系统从"能用"优化到"抗压"**——记录了我在解决"缓存一致性、秒杀超卖、分布式锁选型、异步削峰"等问题时的设计取舍与复盘。

> 技术栈：**Spring Boot 3.2 + MyBatis-Plus + Redis + Redisson + RocketMQ + MySQL**

---

## 目录

- [快速开始](#快速开始)
- [秒杀核心流程（本项目最难点）](#秒杀核心流程本项目最难点)
- [缓存：穿透 / 击穿 / 雪崩](#缓存穿透--击穿--雪崩)
- [异步下单：Redis + Lua + RocketMQ](#异步下单redis--lua--rocketmq)
- [消息可靠性：幂等、丢失、最终一致](#消息可靠性幂等丢失最终一致)
- [订单超时关闭与并发控制](#订单超时关闭与并发控制)
- [分布式锁选型](#分布式锁选型)
- [全局唯一 ID 生成](#全局唯一-id-生成)
- [其他 Redis 特性应用](#其他-redis-特性应用)
- [实现状态清单](#实现状态清单)

---

## 快速开始

### 环境要求

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 17+（本仓库用 21 验证） | Lombok 需 ≥ 1.18.30 才支持 JDK 21 |
| Maven | 3.x | IDEA 内置即可 |
| MySQL | 8.x | 库名 `dingping`，脚本见 `src/main/resources/db/hmdp.sql` |
| Redis | 6+ | 若设了密码，需同步到配置 |
| RocketMQ | 4.x / 5.x | NameServer `127.0.0.1:9876` |

### 启动步骤

1. 启动 RocketMQ NameServer + Broker（端口 9876 / 10911）。
2. 启动 MySQL、Redis。
3. 修改 `src/main/resources/application.yaml` 中的数据库 / Redis / RocketMQ 连接信息。
4. 运行 `com.hmdp.HmDianPingApplication`，访问 `http://localhost:8081`。

---

## 秒杀核心流程（本项目最难点）

秒杀优惠券的下单链路，经历了 **三个阶段** 的演进：

1. **数据库悲观锁**：`select ... for update`。请求串行化，性能极差，连接池瞬间被打满。
2. **数据库乐观锁（CAS）**：`update ... where stock > 0`。解决了超卖，但数据库依然扛不住高并发读写；且"一人一单"在集群下失效（JVM 锁锁不住）。
3. **Redis + Lua + RocketMQ 异步架构（最终方案）**：
   - 用 **Lua 脚本的原子性**，在 Redis 内存中完成"库存判断 + 一人一单校验 + 扣库存 + 记录用户"。
   - 校验通过后，发送消息到 **RocketMQ**，**立即给前端返回"排队中 / 订单号"**。
   - 后端消费者慢慢消费消息，异步写入 MySQL。

**收益**：把同步的 DB 读写转成毫秒级的 Redis 操作，用消息队列削峰。

核心代码：
- `src/main/resources/seckill.lua` —— 原子校验脚本
- `VoucherOrderServiceImpl#seckillVoucher` —— 入口（Lua 执行 + 发 MQ）
- `SeckillVoucherListener` —— RocketMQ 消费者（异步落库）

### 为什么 Redis 里就能保证"不超卖 + 一人一单"？

* **库存信息**：`seckill:stock:{voucherId}`（String，当前库存）
* **下单用户**：`seckill:order:{voucherId}`（Set，所有购买过的 userId）

Lua 脚本里 `get` 库存 → `sismember` 判断是否买过 → `incrby -1` 扣库存 → `sadd` 记用户，这几步在 Redis 中**原子执行**，天然避免并发竞态，也因此**不再需要分布式锁**。

```lua
if (redis.call('sismember', orderKey, userId) == 1) then  -- 一人一单
    return 2
end
if (stock <= 0) then                                      -- 库存判断
    return 1
end
redis.call('incrby', stockKey, -1)                        -- 扣库存
redis.call('sadd', orderKey, userId)                      -- 记录用户
```

---

## 缓存：穿透 / 击穿 / 雪崩

### 缓存穿透（查不存在的 key）

恶意请求不存在的商铺 ID 会直接打到数据库。**方案：缓存空值**。
- 参考：`CacheClient#queryWithPassThrough`
- 查不到时写 `""` 到 Redis，TTL 短一点（`CACHE_NULL_TTL`），避免长期占用内存。
- 为什么不用布隆过滤器：有误判率、需引入新组件维护，本商铺量级缓存空值更简单。

### 缓存击穿（热点 key 失效瞬间）

热点 Key TTL 过期瞬间，大量请求同时重建缓存。**方案：逻辑过期 + 互斥锁 + 异步重建**。
- 参考：`CacheClient#queryWithLogicalExpire`
- 不给 Redis 设物理 TTL，而是把过期时间存在 value 里（`RedisData{data, expireTime}`）。
- 取到数据后判断逻辑过期：
  - 未过期 → 直接返回旧数据。
  - 已过期 → 抢**互斥锁**（`SETNX`），拿到锁的线程**开独立线程池异步重建缓存**，所有请求**立即返回旧数据**（高可用）。

**方案对比（CAP 取舍）**：
- **互斥锁**：保一致性，牺牲可用性（重建期间请求阻塞）。
- **逻辑过期**：保可用性，牺牲一致性（用户可能看到 2 秒前的旧数据）。

秒杀详情 / 热搜榜这类**社交媒体数据不要求强一致**，故选择 **AP（重可用性）** 的逻辑过期方案。

### 缓存雪崩（大量 key 同时失效）

处理手段：TTL 加随机值、热点数据用逻辑过期（无物理 TTL）、Redis 前加本地缓存、Redis 宕机时限流降级。

---

## 异步下单：Redis + Lua + RocketMQ

秒杀链路是**同步流程**：判断资格 → 扣库存 → 生成订单。其中"扣库存 + 生成订单"是两次 MySQL 写操作，在高并发下是性能瓶颈。

**优化**：判断完资格（Redis + Lua 已保证不超卖、一人一单）后，**用户秒杀即成功**。至于 MySQL 的库存扣减和订单生成，完全可以异步做——发一条 RocketMQ 消息，消费者慢慢落库。

```java
// VoucherOrderServiceImpl#seckillVoucher —— 秒杀成功，发消息，立即返回
rocketMQTemplate.syncSend(
    "seckill-order-topic:seckill",            // topic:tag
    MessageBuilder.withPayload(jsonStr).build(),
    3000,                                       // 发送超时
    3                                           // 延迟级别 3 ≈ 10s
);
return Result.ok(orderId);                     // 立即返回订单号
```

**关于 RocketMQ 延迟消息**：这里指定了 `delayLevel=3`（约 10 秒）。RocketMQ 原生支持延迟消息，因此"秒杀下单后延迟落库"不需要额外搭建延迟队列，架构更简洁。

消费者 `SeckillVoucherListener` 收到消息后：
1. `save` 订单（订单 ID 是主键，重复消费天然幂等）。
2. CAS 扣减数据库库存：`update ... set stock = stock - 1 where stock > 0`。

---

## 消息可靠性：幂等、丢失、最终一致

### 1. 重复消费怎么办？（幂等）

RocketMQ 保证 **At Least Once**，网络波动时可能重复投递。
- 发送消息时携带**唯一订单 ID**（RedisIdWorker 生成）。
- 消费者 `save` 时，订单 ID 是**主键**，重复消费第二次写入因主键冲突失败 → 判定为重复消息 → 跳过。
- 参考：`VoucherOrder` 主键 `id`。

### 2. 消息丢了，MySQL 数据不一致怎么办？

秒杀核心要求（不超卖 + 一人一单）**由 Redis + Lua 保证，不受影响**——即使 MQ 丢了消息，Redis 里库存已扣。
- 丢消息只影响 **MySQL 的最终一致性**（库存对不上、订单没生成）。
- 兜底：秒杀结束后跑**定时任务对账**，比对 Redis 库存 / 下单记录与 MySQL 是否一致，不一致则修复。这就是"对账思想"。
- 如果用了 RocketMQ **事务消息 + 本地消息表**，可以做到强一致；但秒杀场景对强一致要求不高，本方案更简单。

### 3. 用户支付后订单记录还没生成怎么办？

用户支付本身不依赖数据库订单记录。后台生成唯一订单 ID 后调用第三方接口生成支付凭证；若支付回调时数据库无此订单（MQ 还在排队），可利用回调信息**反向生成"已支付"订单**。

---

## 订单超时关闭与并发控制

未支付订单超时需要关闭并释放库存。调研过的方案：

| 方案 | 结论 |
|---|---|
| **Spring Task 定时扫描** | **采用**。单体订单量不大，简单稳健；加好联合索引效率足够 |
| RocketMQ 延迟消息 | 大量无效调度（多数订单已支付），极端情况可能丢消息 |
| Redis 过期监听 | 惰性删除不保证实时，不可靠 |

**重复关闭 / 重复加库存？** 不会：`update orders set status='取消' where id=? and status='未支付'`，只有影响行数 > 0 才释放库存。

**关单和支付并发的"二选一"**（乐观锁）：
- 超时关单：`update ... set status='取消' where id=? and status='未支付'`
- 支付成功：`update ... set status='已支付' where id=? and status='未支付'`

利用数据库行锁，两个 SQL 只有一个成功。若"关单成功但用户已支付"，系统应触发**原路退回**，不能强改已支付（否则超卖）。

---

## 分布式锁选型

下单 / 关单等操作需要保证同一用户串行，因此引入分布式锁。演进：

1. **自研 SETNX 锁**：`SimpleRedisLock`（`SET key value NX EX 10` + Lua 释放）。问题：没有看门狗自动续期，锁超时可能误删他人锁。
2. **Redisson**：**可重入 + 看门狗自动续期**（默认 30s，每 10s 续一次），业务没跑完锁不会过期。

```java
RLock lock = redissonClient.getLock("lock:order:" + userId);
boolean isLock = lock.tryLock();
try { ... } finally { lock.unlock(); }
```

> 注：秒杀下单链路本身因为用了 Redis + Lua 原子判断，**不再需要分布式锁**（Lua 已保证并发安全）；分布式锁仍用于其他需要串行的业务场景。

---

## 全局唯一 ID 生成

`RedisIdWorker`：**时间戳 + 序列号** 的 64 位 ID（类似雪花算法）。

- 高位：当前时间戳与固定起始时间戳之差。
- 低位：`INCR` 按天自增的序列号（`icr:order:2026:08:21`），避免跨天溢出。
- 为什么不用数据库自增：分布式下重复、且暴露规则。

```java
long timestamp = nowSecond - BEGIN_TIMESTAMP;   // 41 位时间戳
Long count = stringRedisTemplate.opsForValue()
        .increment("icr:" + keyPrefix + ":" + date);  // 序列号
return timestamp << COUNT_BITS | count;         // 64 位 ID
```

---

## 其他 Redis 特性应用

| 功能 | Redis 数据结构 | 实现 |
|---|---|---|
| 点赞排行榜 | ZSet | `ZADD key score(时间戳) value(userId)`，`ZREVRANGE` 取 Top |
| 共同关注 | Set | `SINTER keyA keyB` 求交集 |
| 附近商户 | GEO | `GEOSEARCH` 5km 内按距离排序（GeoHash） |
| 用户签到 | BitMap | 1 bit 代表一天，`SETBIT` |
| UV 统计 | HyperLogLog | `PFADD` / `PFCOUNT`，12KB 统计海量数据 |

---

## 实现状态清单

> 以下区分 **已落地为代码** 与 **设计思考**，与仓库代码严格对应。

| 设计 | 状态 |
|---|---|
| Redis + Lua 秒杀（不超卖 / 一人一单） | ✅ 已实现（`seckill.lua` + `VoucherOrderServiceImpl`） |
| RocketMQ 异步下单 | ✅ 已实现（`RocketMQTemplate` + `SeckillVoucherListener`） |
| 全局唯一 ID | ✅ 已实现（`RedisIdWorker`） |
| 逻辑过期缓存 | ✅ 已实现（`CacheClient#queryWithLogicalExpire`） |
| 缓存穿透（空值） | ✅ 已实现（`CacheClient#queryWithPassThrough`） |
| 分布式锁 | ✅ 已实现（`SimpleRedisLock` + Redisson） |
| 登录 / 点赞 / 关注 / 附近 / 签到 / UV | ✅ 已实现 |
| 滑动窗口限流（注解 + 切面 + Lua） | ⬜ 设计思考，代码未实现 |
| Caffeine 二级缓存 | ⬜ 设计思考，代码未实现（见下） |
| 订单超时定时关单（Spring Task） | ⬜ 设计思考，代码未实现 |

**为什么二级缓存、限流、定时关单没落地？**
- 它们针对的是**更大规模的部署场景**（多实例、更高并发、更强风控），当前单体 + 单实例架构下收益有限，属于"做了更好的架构演进"的一部分。README 里记录了完整设计取舍，面试可讲思路。
- 若需要，可以按 README 的思路继续落地。

---

## 免责声明

本项目为学习 / 面试用途，业务逻辑参考了经典"大众点评"系统。代码中的设计取舍（缓存、秒杀、锁、消息队列）均有真实压测 / 日志佐证，欢迎交流指正。
