package com.hmdp.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 缓存删除补偿消费者
 * <p>
 * 更新数据库后删除缓存失败时，{@code ShopServiceImpl#update} 会发送一条补偿消息到
 * {@code cache-delete-topic}。本消费者收到后重试删除，配合缓存自身的物理 TTL 兜底，
 * 确保"更新 DB → 删缓存"这条 Cache Aside 链路最终一致，避免旧数据长期驻留。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "cache-delete-topic",
        consumerGroup = "cache-delete-consumer"
)
public class CacheDeleteListener implements RocketMQListener<String> {

    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(String key) {
        log.info("收到缓存删除补偿消息，key: {}", key);
        try {
            stringRedisTemplate.delete(key);
            log.info("缓存删除补偿成功，key: {}", key);
        } catch (Exception e) {
            log.error("缓存删除补偿失败，key: {}，等待 RocketMQ 重投", key, e);
            // 抛异常触发 RocketMQ 重投；多次仍失败时由缓存物理 TTL 兜底
            throw e;
        }
    }
}
