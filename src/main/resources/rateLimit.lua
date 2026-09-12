-- 滑动窗口限流脚本
-- KEYS[1]  限流 key（已拼接好维度前缀，如 rate:limit:ip:127.0.0.1）
-- ARGV[1]  窗口大小（秒）
-- ARGV[2]  窗口内最大请求数
-- ARGV[3]  当前毫秒时间戳（由 Java 端传入，保证所有调用使用同一时钟源），作为 ZSet 的 score
-- ARGV[4]  请求唯一标识（由 Java 端生成），作为 ZSet 的 member

-- 1.窗口左边界（当前时间 - 窗口时长）
local windowStart = tonumber(ARGV[3]) - tonumber(ARGV[1]) * 1000

-- 2.移除窗口外（score < 窗口左边界）的历史请求记录
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, windowStart)

-- 3.统计窗口内剩余请求数
local current = tonumber(redis.call('ZCARD', KEYS[1]))

-- 4.超限直接返回，不写入当前请求（保证限流后窗口不会因失败请求被撑大）
if current >= tonumber(ARGV[2]) then
    return 0
end

-- 5.未超限：记录当前请求（score=毫秒时间戳，用于窗口滑出；member=唯一标识）
--   若 member 也用毫秒时间戳，同一毫秒内到达的并发请求会互相覆盖，导致 ZCARD 偏小，
--   恰恰在流量最猛的突发时刻限流失效
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
-- 6.窗口最晚结束时间 = 当前时间 + 窗口时长，作为 key 的过期时间，避免 key 无限堆积
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[1]) * 1000)

return 1
