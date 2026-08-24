package com.hmdp.job;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 秒杀数据对账任务
 * <p>
 * 秒杀链路中，Redis 是库存扣减的权威（Lua 原子执行），MySQL 是异步落库的副本。
 * 若 RocketMQ 消息丢失、关单回补失败等，会导致 Redis 与 DB 的库存 / 有效订单数发生漂移。
 * 本任务定期（默认 10 分钟）扫描所有秒杀券，比对 Redis 与 DB 的库存及有效订单数，
 * 发现不一致时以 Redis 为准修正 DB 库存，并输出差异告警日志。
 * </p>
 * <p>
 * 说明：对账只负责"修正库存 + 暴露差异"。若 Redis 下单数大于 DB 有效订单数（疑似 MQ 丢消息），
 * 仅记录告警，不自动反向补单，避免与在途消息重复消费冲突。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockReconcileTask {

    private final ISeckillVoucherService seckillVoucherService;
    private final IVoucherOrderService voucherOrderService;
    private final StringRedisTemplate stringRedisTemplate;

    /** 订单状态常量：已取消（不占用库存） */
    private static final int STATUS_CANCELED = 4;

    /**
     * 每 10 分钟执行一次
     */
    @Scheduled(fixedDelay = 600_000, initialDelay = 60_000)
    public void reconcile() {
        List<SeckillVoucher> vouchers = seckillVoucherService.list();
        if (vouchers.isEmpty()) {
            return;
        }
        for (SeckillVoucher voucher : vouchers) {
            try {
                reconcileOne(voucher);
            } catch (Exception e) {
                log.error("对账失败，voucherId: {}", voucher.getVoucherId(), e);
            }
        }
    }

    private void reconcileOne(SeckillVoucher voucher) {
        Long voucherId = voucher.getVoucherId();
        Integer dbStock = voucher.getStock();

        // 1.Redis 权威数据：库存 + 已下单用户集合
        String stockStr = stringRedisTemplate.opsForValue()
                .get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
        if (stockStr == null) {
            // Redis 无此券库存（未预热 / 已过期清理），跳过
            return;
        }
        long redisStock = Long.parseLong(stockStr);
        Long redisOrderCount = stringRedisTemplate.opsForSet()
                .size(RedisConstants.SECKILL_ORDER_KEY + voucherId);
        long redisOrder = redisOrderCount == null ? 0L : redisOrderCount;

        // 2.DB 数据：有效订单数（未取消的订单才占用库存）
        long dbValidOrders = voucherOrderService.query()
                .eq("voucher_id", voucherId)
                .ne("status", STATUS_CANCELED)
                .count();

        // 3.差异判断
        boolean stockDiff = dbStock != null && dbStock != redisStock;
        boolean orderDiff = redisOrder != dbValidOrders;
        if (!stockDiff && !orderDiff) {
            return;
        }

        // 4.不一致：以 Redis 为准修正 DB 库存
        log.warn("秒杀对账发现不一致，voucherId: {}, DB库存: {}, Redis库存: {}, Redis下单: {}, DB有效订单: {}",
                voucherId, dbStock, redisStock, redisOrder, dbValidOrders);
        seckillVoucherService.update()
                .set("stock", redisStock)
                .eq("voucher_id", voucherId)
                .update();
        log.info("秒杀对账已修正 DB 库存，voucherId: {}, stock -> {}", voucherId, redisStock);

        // 5.下单数不一致：提示疑似消息丢失，需人工复核 / 重放
        if (redisOrder > dbValidOrders) {
            log.error("voucherId: {} 疑似 RocketMQ 消息丢失，Redis 下单 {} 笔 > DB 有效订单 {} 笔，请人工补单",
                    voucherId, redisOrder, dbValidOrders);
        }
    }
}
