package com.hmdp.listener;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单异步消费者
 * <p>
 * 原 RabbitMQ 版通过 QA(10s TTL 死信 → QD)实现延时消费；
 * RocketMQ 版本利用 broker 端的延迟消息机制（发送时指定延迟级别 3 ≈ 10s），
 * 消费者收到消息即说明延迟已过，直接落库 + CAS 扣库存。
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
        // 订单 ID 为主键唯一，重复消费时 save 会因主键冲突失败，天然幂等
        voucherOrderService.save(voucherOrder);
        // 数据库秒杀库存减一（CAS，stock > 0）
        seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
    }
}
