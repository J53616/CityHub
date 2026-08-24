package com.hmdp.annotation;

import com.hmdp.enums.LimitType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 滑动窗口限流注解
 * <p>
 * 通过 AOP 切面 + Redis 的 ZSet（成员 = 请求毫秒时间戳，score = 时间戳）实现滑动窗口计数：
 * 每次请求先 ZREMRANGEBYSCORE 移除窗口外的历史记录，再 ZADD 当前时间戳，
 * 最后 ZCARD 统计窗口内请求数，超过 limit 则拒绝。整个逻辑由 Lua 原子执行，避免并发下超限。
 * </p>
 *
 * <pre>
 * &#064;RateLimit(time = 1, count = 10)          // 1 秒内最多 10 次，默认全局维度
 * &#064;RateLimit(time = 1, count = 10, limitType = LimitType.IP)   // 按 IP 限流
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    /**
     * 限流时间窗口，单位：秒
     */
    int time() default 1;

    /**
     * 窗口内允许的最大请求数
     */
    int count() default 10;

    /**
     * 限流维度
     */
    LimitType limitType() default LimitType.DEFAULT;
}
