package com.hmdp.enums;

/**
 * 限流维度
 * <ul>
 *   <li>{@link #DEFAULT}：全局维度，所有请求共享同一个计数窗口（如秒杀入口的全局兜底）</li>
 *   <li>{@link #IP}：按客户端 IP 隔离计数，防爬虫 / 刷接口</li>
 *   <li>{@link #USER}：按登录用户隔离计数，防刷券（依赖 UserHolder，需要登录态）</li>
 * </ul>
 */
public enum LimitType {
    /** 全局维度，不拼接额外标识 */
    DEFAULT,
    /** 按 IP 限流，key 后缀取客户端 IP */
    IP,
    /** 按用户限流，key 后缀取当前登录用户 id */
    USER
}
