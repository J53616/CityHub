package com.hmdp.job;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 超时未支付订单定时关单任务
 * <p>
 * 秒杀下单后如果用户一直不支付，订单会长期占用库存。本任务每 1 分钟扫描一次
 * "超时（默认 15 分钟）且仍为未支付"的订单，利用乐观锁（CAS）关闭并释放库存：
 * </p>
 * <ul>
 *   <li><b>关单 CAS</b>：{@code update tb_voucher_order set status=4 where id=? and status=1}，
 *       只有未支付（status=1）的订单才会被更新为已取消（status=4），重复执行 / 已支付订单均影响 0 行，天然幂等。</li>
 *   <li><b>加库存 CAS</b>：{@code update tb_seckill_voucher set stock=stock+1 where voucher_id=?}
 *       与支付回调扣库存的 {@code where stock>0} 互斥，不会出现"超卖补回 / 补加超卖"的并发问题。</li>
 *   <li><b>释放 Redis 预扣</b>：移除 {@code seckill:order:{voucherId}} 集合中的用户，恢复"一人一单"的购买资格；
 *       补偿 {@code seckill:stock:{voucherId}} 加 1，与 DB 双写保持一致（Redis 以 Lua 扣减为权威，此处仅回补）。</li>
 * </ul>
 * <p>
 * 定时扫描依赖 {@code tb_voucher_order(status, create_time)} 联合索引，见 {@code db/hmdp.sql} 末尾。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutTask {

    private final IVoucherOrderService voucherOrderService;
    private final ISeckillVoucherService seckillVoucherService;
    private final StringRedisTemplate stringRedisTemplate;

    /** 超时阈值：15 分钟未支付则关闭 */
    private static final int TIMEOUT_MINUTES = 15;

    /** 订单状态常量（与 tb_voucher_order.status 对应） */
    private static final int STATUS_UNPAID = 1;
    private static final int STATUS_CANCELED = 4;

    /** 单批处理上限，避免一次查询拖垮数据库 */
    private static final int BATCH_SIZE = 100;

    /**
     * 每 1 分钟执行一次（fixedDelay：上次执行完再等 60s，避免任务堆叠）
     */
    @Scheduled(fixedDelay = 60_000, initialDelay = 30_000)
    public void closeTimeoutOrders() {
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
        log.info("定时关单：扫描到 {} 笔超时未支付订单", orders.size());
        int closed = 0;
        for (VoucherOrder order : orders) {
            try {
                if (closeOrder(order)) {
                    closed++;
                }
            } catch (Exception e) {
                log.error("关单失败，orderId: {}", order.getId(), e);
            }
        }
        log.info("定时关单完成：成功关闭 {} / {} 笔", closed, orders.size());
    }

    /**
     * 关闭单笔订单并释放库存。
     *
     * @return 是否真正关单成功（影响行数 > 0，即状态确实是未支付）
     */
    private boolean closeOrder(VoucherOrder order) {
        // 1.乐观锁关单：只有 status=未支付 才会被更新为已取消，与支付回调互斥
        boolean closed = voucherOrderService.update()
                .set("status", STATUS_CANCELED)
                .set("pay_time", null)
                .eq("id", order.getId())
                .eq("status", STATUS_UNPAID)
                .update();
        if (!closed) {
            // 期间已被支付或已关闭，跳过
            return false;
        }
        // 2.数据库秒杀库存回补（CAS：stock = stock + 1）
        seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", order.getVoucherId())
                .update();
        // 3.释放 Redis 预扣：补库存 + 移除下单用户记录（恢复一人一单资格）
        stringRedisTemplate.opsForValue()
                .increment(RedisConstants.SECKILL_STOCK_KEY + order.getVoucherId());
        stringRedisTemplate.opsForSet()
                .remove(RedisConstants.SECKILL_ORDER_KEY + order.getVoucherId(), order.getUserId().toString());
        return true;
    }
}
