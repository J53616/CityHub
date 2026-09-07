package com.hmdp.listener;

import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 超时订单关单消费者（延迟消息驱动）
 * <p>
 * 秒杀下单时发送一条 RocketMQ 延迟消息（delayLevel=5 ≈ 1 分钟，秒杀场景压缩超时窗口），
 * 到期后由本消费者触发关单检查：若订单仍为未支付则乐观锁关闭并释放库存（Redis 回补走 Lua
 * 原子执行，见 restore.lua）。相比定时轮询，延迟消息能在超时时刻"精准触发"；若延迟消息丢失 /
 * 消费失败，由 {@code OrderTimeoutTask} 定时扫描兜底，双保险保证超时订单最终都会被关闭。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "seckill-order-close-topic",
        consumerGroup = "seckill-order-close-consumer",
        selectorExpression = "close"
)
public class OrderTimeoutListener implements RocketMQListener<String> {

    private final IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(String orderIdStr) {
        Long orderId = Long.valueOf(orderIdStr);
        boolean closed = voucherOrderService.closeTimeoutOrder(orderId);
        log.info("延迟关单消息处理完成，orderId: {}, 关单结果: {}", orderId, closed);
    }
}
