package com.hmdp.exception;

/**
 * 触发限流时抛出的业务异常
 * <p>
 * 由 {@code WebExceptionAdvice#handleRateLimitException} 统一捕获，
 * 返回 429 + "操作过于频繁，请稍后再试"，避免和普通 500 混淆。
 * </p>
 */
public class RateLimitException extends RuntimeException {

    public RateLimitException(String message) {
        super(message);
    }
}
