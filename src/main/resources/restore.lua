-- 1.参数列表
--1.1.用户id
local userId=ARGV[1]
--1.2.优惠券id
local voucherId=ARGV[2]

-- 2.数据key
--2.1.库存key
local stockKey='seckill:stock:' .. voucherId
--2.2.订单key
local orderKey='seckill:order:' .. voucherId

-- 3.脚本业务（关单库存回滚，原子执行）
--3.1.幂等保护：若用户不在下单集合中，说明该单库存此前已释放（延迟消息与定时兜底竞态下，
--    只有 DB CAS 成功的那一次会走到这里，这里再兜一层，避免极端情况下重复回补导致超卖）
if(redis.call('sismember',orderKey,userId)==0) then
    return -1
end
--3.2.移除用户下单记录（恢复“一人一单”资格）
redis.call('srem',orderKey,userId)
--3.3.回补库存 incrby stockKey 1
local stock=redis.call('incrby',stockKey,1)
--3.4.返回回补后的库存
return stock
