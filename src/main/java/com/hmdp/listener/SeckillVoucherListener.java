package com.hmdp.listener;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单异步消费者
 * <p>
 * 原 RabbitMQ 版通过 QA(10s TTL 死信 → QD)实现延时落库；RocketMQ 版本落库消息立即投递
 * （delayLevel=0），仅借异步解耦削峰，消费者直接落库 + CAS 扣库存，保证订单尽快可见。
 * </p>
 */
@Component
@RequiredArgsConstructor
@Slf4j
@RocketMQMessageListener(
        topic = "seckill-order-topic",
        consumerGroup = "seckill-order-consumer",
        selectorExpression = "seckill"
)
public class SeckillVoucherListener implements RocketMQListener<String> {

    private final IVoucherOrderService voucherOrderService;
    private final ISeckillVoucherService seckillVoucherService;

    @Override
    public void onMessage(String msg) {
        log.info("收到秒杀订单消息: {}", msg);
        VoucherOrder voucherOrder = JSONUtil.toBean(msg, VoucherOrder.class);
        // 订单 ID 为主键，重复消费会命中主键唯一约束冲突。
        // 这是"幂等命中"的**正常语义，不是消费失败**：必须捕获并直接返回，否则 RocketMQ 会判定消费失败
        // 并反复重投（默认 maxReconsumeTimes=16 次后进死信队列）。同时必须 return，避免重复扣减 DB 库存。
        try {
            voucherOrderService.save(voucherOrder);
        } catch (DuplicateKeyException e) {
            log.info("订单已存在，幂等命中，跳过落库与扣减，orderId: {}", voucherOrder.getId());
            return;
        }
        // 数据库秒杀库存减一（CAS，stock > 0）
        seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
    }
}
