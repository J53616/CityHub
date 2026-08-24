package com.hmdp.job;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 超时未支付订单定时扫描（兜底）
 * <p>
 * 主关单链路是 RocketMQ 延迟消息（{@code OrderTimeoutListener}，下单时按超时阈值发送），
 * 若延迟消息丢失 / 消费失败，本任务每 1 分钟扫描"超时（10 分钟）且仍为未支付"的订单兜底关闭。
 * 关单逻辑统一走 {@link IVoucherOrderService#closeTimeoutOrder(VoucherOrder)}（乐观锁 + 幂等），
 * 与支付回调互斥，不误关已支付订单。
 * </p>
 * <p>定时扫描依赖 {@code tb_voucher_order(status, create_time)} 联合索引，见 {@code db/hmdp.sql} 末尾。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutTask {

    private final IVoucherOrderService voucherOrderService;

    /** 超时阈值：10 分钟未支付则关闭（与 RocketMQ 延迟消息级别 delayLevel=14 一致） */
    private static final int TIMEOUT_MINUTES = 10;

    /** 订单状态：未支付（与 tb_voucher_order.status 对应） */
    private static final int STATUS_UNPAID = 1;

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
}
