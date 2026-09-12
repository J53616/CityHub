package com.hmdp.job;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 超时未支付订单定时扫描（兜底）
 * <p>
 * 主关单链路是 RocketMQ 延迟消息（{@code OrderTimeoutListener}，下单时按超时阈值发送），
 * 若延迟消息丢失 / 消费失败，本任务兜底扫描"超时（1 分钟）且仍为未支付"的订单关闭。
 * 关单逻辑统一走 {@link IVoucherOrderService#closeTimeoutOrder(VoucherOrder)}（乐观锁 + 幂等），
 * 与支付回调互斥，不误关已支付订单。
 * </p>
 * <p>防“频繁扫库”：扫描前先用 Redis 预检是否有在途订单（见 {@link #hasPendingOrders()}），
 * 无单时段直接跳过 DB 扫描，将扫库频率从“时间驱动”降为“积压驱动”。</p>
 * <p>定时扫描依赖 {@code tb_voucher_order(status, create_time)} 联合索引，见 {@code db/hmdp.sql} 末尾。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutTask {

    private final IVoucherOrderService voucherOrderService;
    private final StringRedisTemplate stringRedisTemplate;

    /** 超时阈值：1 分钟未支付则关闭（与 RocketMQ 延迟消息级别 delayLevel=5 一致，秒杀场景压缩超时窗口） */
    private static final int TIMEOUT_MINUTES = 1;

    /** 订单状态：未支付（与 tb_voucher_order.status 对应） */
    private static final int STATUS_UNPAID = 1;

    /** 单批处理上限，避免一次查询拖垮数据库 */
    private static final int BATCH_SIZE = 100;

    /**
     * 每 30 秒执行一次（fixedDelay：上次执行完再等 30s，避免任务堆叠）。
     * <p>秒杀超时窗口压缩到 1 分钟后，扫描需更及时才能兜住延迟消息丢失的场景；
     * 但每次执行前经 Redis 积压预检过滤，无单时段不会真正扫库。</p>
     */
    @Scheduled(fixedDelay = 30_000, initialDelay = 15_000)
    public void closeTimeoutOrders() {
        // 0.Redis 积压预检：没有任何秒杀券存在下单集合时，必然不存在在途未支付订单，跳过扫库
        if (!hasPendingOrders()) {
            return;
        }
        LocalDateTime timeoutTime = LocalDateTime.now().minusMinutes(TIMEOUT_MINUTES);
        // 1.查出超时未支付订单（仅未支付状态，limit 控制批次大小）
        List<VoucherOrder> orders = voucherOrderService.query()
                .eq("status", STATUS_UNPAID)
                .lt("create_time", timeoutTime)
                .last("limit " + BATCH_SIZE)
                .list();
        if (orders.isEmpty()) {
            return;
        }
        log.info("定时关单兜底：扫描到 {} 笔超时未支付订单", orders.size());
        int closed = 0;
        for (VoucherOrder order : orders) {
            try {
                if (voucherOrderService.closeTimeoutOrder(order)) {
                    closed++;
                }
            } catch (Exception e) {
                log.error("关单失败，orderId: {}", order.getId(), e);
            }
        }
        log.info("定时关单兜底完成：成功关闭 {} / {} 笔", closed, orders.size());
    }

    /**
     * Redis 积压预检：是否存在任何秒杀券有“在途下单”（下单时 sadd，关单时 srem）。
     * <p>
     * 只要任一 {@code seckill:order:{voucherId}} 集合非空，就可能存在未支付订单，值得扫库；
     * 全部为空则必然无未支付订单（下单必入集合），跳过 DB 扫描。此为“粗筛”：已支付订单
     * 在关单前也留在集合中，故会多扫而不会漏扫，安全性不受影响。
     * </p>
     *
     * @return true 表示可能有在途未支付订单，需扫库
     */
    private boolean hasPendingOrders() {
        try {
            // 用 SCAN 游标增量遍历替代 KEYS：KEYS 会全库阻塞扫描，在单线程 Redis 上是大忌（生产禁用命令）。
            // SCAN 可能返回重复 key，用 Set 去重；秒杀券 key 数量级很小，一次收集开销可忽略。
            Set<String> keys = new HashSet<>();
            try (Cursor<String> cursor = stringRedisTemplate.scan(
                    ScanOptions.scanOptions()
                            .match(RedisConstants.SECKILL_ORDER_KEY + "*")
                            .count(200)
                            .build())) {
                while (cursor.hasNext()) {
                    keys.add(cursor.next());
                }
            }
            if (keys.isEmpty()) {
                return false;
            }
            for (String key : keys) {
                Long size = stringRedisTemplate.opsForSet().size(key);
                if (size != null && size > 0) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            // 预检失败（如 Redis 抖动）：宁可扫库也不漏关单，返回 true 走 DB 扫描兜底
            log.warn("Redis 积压预检失败，回退为直接扫库", e);
            return true;
        }
    }
}
