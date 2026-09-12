package com.hmdp.aspect;

import cn.hutool.core.util.StrUtil;
import com.hmdp.annotation.RateLimit;
import com.hmdp.enums.LimitType;
import com.hmdp.exception.RateLimitException;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.UUID;

/**
 * 滑动窗口限流切面
 * <p>
 * 拦截标注 {@link RateLimit} 的方法，在方法执行前通过 Lua 脚本做滑动窗口计数校验：
 * </p>
 * <ol>
 *   <li>按注解上的维度（全局 / IP / 用户）拼接 Redis key；</li>
 *   <li>执行 {@code rateLimit.lua}，返回 1 放行、0 拒绝；</li>
 *   <li>拒绝时抛出 {@link RateLimitException}，由全局异常处理器返回 429。</li>
 * </ol>
 */
@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** key 前缀，区分限流与业务缓存 */
    private static final String KEY_PREFIX = "rate:limit:";

    private static final DefaultRedisScript<Long> LIMIT_SCRIPT;

    static {
        LIMIT_SCRIPT = new DefaultRedisScript<>();
        LIMIT_SCRIPT.setLocation(new ClassPathResource("rateLimit.lua"));
        LIMIT_SCRIPT.setResultType(Long.class);
    }

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        // 1.拼接限流 key（按维度）
        String key = buildKey(signature.getName(), rateLimit);
        // 2.执行 Lua 脚本做滑动窗口计数
        Long result = stringRedisTemplate.execute(
                LIMIT_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(rateLimit.time()),
                String.valueOf(rateLimit.count()),
                String.valueOf(System.currentTimeMillis()),
                // member 必须唯一：同一毫秒内的并发请求若共用时间戳作 member 会被 ZADD 覆盖，
                // 使 ZCARD 偏小、放行数超过阈值（突发流量下限流失效）
                UUID.randomUUID().toString()
        );
        // 3.被限流
        if (result == null || result == 0L) {
            log.warn("接口[{}]触发限流，key: {}", signature.getName(), key);
            throw new RateLimitException("操作过于频繁，请稍后再试");
        }
        // 4.放行
        return joinPoint.proceed();
    }

    /**
     * 按限流维度拼接 Redis key：
     * <ul>
     *   <li>DEFAULT：rate:limit:{方法名}</li>
     *   <li>IP：rate:limit:ip:{客户端IP}，未拿到 IP 时退化为全局</li>
     *   <li>USER：rate:limit:user:{用户id}，未登录时退化为全局</li>
     * </ul>
     */
    private String buildKey(String methodName, RateLimit rateLimit) {
        LimitType limitType = rateLimit.limitType();
        String key = KEY_PREFIX + limitType.name().toLowerCase() + ":";
        if (limitType == LimitType.IP) {
            String ip = getIp();
            if (StrUtil.isBlank(ip)) {
                return KEY_PREFIX + methodName;
            }
            return key + ip;
        }
        if (limitType == LimitType.USER) {
            if (UserHolder.getUser() == null) {
                return KEY_PREFIX + methodName;
            }
            return key + UserHolder.getUser().getId();
        }
        return KEY_PREFIX + methodName;
    }

    /**
     * 获取客户端 IP。优先取 X-Forwarded-For（经过反向代理时携带真实 IP），
     * 取不到再用 Servlet 的 remoteAddr。
     */
    private String getIp() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return null;
        }
        HttpServletRequest request = attributes.getRequest();
        String ip = request.getHeader("X-Forwarded-For");
        if (StrUtil.isBlank(ip) || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (StrUtil.isBlank(ip) || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        // 多级代理时 X-Forwarded-For 为逗号分隔的多个 IP，取第一个
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }
}
