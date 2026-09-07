package com.hmdp.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 秒杀库存二级缓存（Caffeine 本地一级 + Redis 二级）
 * <p>
 * 秒杀页/列表会高频轮询“剩余库存”做展示，若每次都穿透到 Redis（单节点）甚至 DB，
 * 秒杀瞬间几十万次读请求会打爆单节点 Redis。此处用 Caffeine 在 JVM 内做一级缓存：
 * <ul>
 *   <li>读路径：Caffeine 命中直接返回（拦截绝大多数重复轮询）→ miss 才读 Redis 权威库存并回填；</li>
 *   <li>TTL：极短 1 秒。即使不清缓存，数据最多滞后 1 秒，保证秒杀场景下的实时性下限；</li>
 *   <li>一致性：单机内下单成功 / 关单回补后调用 {@link #invalidate(Long)} 主动失效立即可见；
 *       多实例部署时不同实例各自 1s 本地缓存，Redis 仍是权威，跨实例短暂不一致由 1s TTL 收敛。</li>
 * </ul>
 * 注意：这里只缓存“读展示”，不参与扣减。真正扣减 / 回补仍走 Lua 原子操作 Redis（见 seckill.lua / restore.lua），
 * Redis 始终是库存的权威，Caffeine 仅是读路径加速。
 */
@Slf4j
@Component
public class SeckillStockCache {

    private final StringRedisTemplate stringRedisTemplate;

    /** 本地缓存：key=秒杀券 id，value=剩余库存 */
    private Cache<Long, Integer> stockCache;

    /** 本地缓存极短 TTL（秒）：拦截重复读的同时保证最多滞后 1s */
    private static final int LOCAL_TTL_SECONDS = 1;

    /** 读 Redis 时缓存 miss 也短暂空缓存，避免极端热点下穿透 */
    private static final Integer NULL_MARK = null;

    public SeckillStockCache(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @PostConstruct
    public void init() {
        stockCache = Caffeine.newBuilder()
                .expireAfterWrite(LOCAL_TTL_SECONDS, TimeUnit.SECONDS)
                // 秒杀券数量有限，不设容量上限（单机存几十~上百个 Long key 开销可忽略）
                .maximumSize(10_000)
                .build();
    }

    /**
     * 查询秒杀券剩余库存（二级缓存读路径）
     *
     * @param voucherId 秒杀券 id
     * @return 剩余库存；返回 {@code null} 表示 Redis 无此券库存（未预热 / 秒杀不存在）
     */
    public Integer getRemainStock(Long voucherId) {
        // 1.一级缓存（Caffeine）命中直接返回
        Integer cached = stockCache.getIfPresent(voucherId);
        if (cached != null) {
            return cached;
        }
        // 2.miss：读 Redis 权威库存并回填
        String stockStr = stringRedisTemplate.opsForValue()
                .get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        if (stockStr == null) {
            // 秒杀券不存在或库存未初始化（见 VoucherServiceImpl#addSeckillVoucher）
            return NULL_MARK;
        }
        Integer stock = Integer.valueOf(stockStr);
        stockCache.put(voucherId, stock);
        return stock;
    }

    /**
     * 主动失效本地缓存（下单成功预扣 / 关单回补后调用，立即反映最新库存，无需等 1s TTL）
     *
     * @param voucherId 秒杀券 id
     */
    public void invalidate(Long voucherId) {
        stockCache.invalidate(voucherId);
    }
}
