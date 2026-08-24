package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    /**
     * 逻辑过期缓存：不依赖 Redis 物理 TTL，而是把过期时间存进 value（{@link RedisData}）。
     * 为避免"更新 DB 后删缓存失败、缓存又无物理 TTL"导致旧数据永久驻留，
     * 这里同时设置一个比逻辑过期略长的物理 TTL 作为最终兜底（Cache Aside 兜底）。
     */
    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //物理 TTL = 逻辑过期时间 + 60s 余量，保证物理 TTL 永远最后兜底
        stringRedisTemplate.opsForValue().set(
                key,
                JSONUtil.toJsonStr(redisData),
                unit.toSeconds(time) + PHYSICAL_TTL_REMAIN,
                TimeUnit.SECONDS);
    }

    /** 逻辑过期之外追加的物理 TTL 余量（秒），作为删缓存失败的最终兜底 */
    private static final long PHYSICAL_TTL_REMAIN = 60L;

    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(json)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3.存在，返回商铺信息
            return JSONUtil.toBean(json, type);

        }
        //判断是否为空值
        if(json!=null){
            return null;
        }
        //4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //5.判断数据库中是否存在
        if(r==null){
            //6.不存在，返回错误状态码
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //7.存在，写入redis，返回商铺信息
       this.set(key,r,time,unit);

        return r;

    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isBlank(json)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3.不存在，返回商铺信息
            return null;

        }

        //4.存在，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R shop = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1.未过期，直接返回店铺信息
            return shop;
        }
        //5.2.已过期，需要返回缓存重建
        //6.缓存重建
        //6.1.获取互斥锁
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        //6.2.判断是否获取锁成功
        if(isLock){
            //  6.3.成功，开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                   //查询数据库
                    R r1= dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(key);
                }
            });

        }

        //6.4.返回过期的商铺信息
        return shop;

    }
    /**
     * 逻辑过期 + 缓存穿透的组合方案（主路径推荐）：
     * <ul>
     *   <li>缓存 miss 时回源 DB：存在则写入逻辑过期缓存；<b>不存在则缓存空值（短 TTL）</b>，防穿透；</li>
     *   <li>缓存命中且未过期：直接返回；</li>
     *   <li>缓存命中但逻辑过期：抢互斥锁 + 线程池异步重建，所有请求立即返回旧数据（防击穿，AP）。</li>
     * </ul>
     */
    public <R, ID> R queryWithLogicalExpireAndPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 1.缓存 miss（可能是穿透请求，也可能是缓存未预热）
        if (StrUtil.isBlank(json)) {
            R dbData = dbFallback.apply(id);
            if (dbData == null) {
                // 1.1 数据库无此数据：缓存空值，短 TTL，防穿透
                stringRedisTemplate.opsForValue()
                        .set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 1.2 数据库有数据：写入逻辑过期缓存（含物理 TTL 兜底）
            this.setWithLogicalExpire(key, dbData, time, unit);
            return dbData;
        }
        // 2.缓存命中：解析逻辑过期信息
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R data = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            // 2.1 未过期，直接返回
            return data;
        }
        // 2.2 已过期：抢互斥锁，抢到者异步重建，其余请求返回旧数据（防击穿）
        boolean isLock = tryLock(RedisConstants.LOCK_SHOP_KEY + id);
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r1 = dbFallback.apply(id);
                    if (r1 != null) {
                        this.setWithLogicalExpire(key, r1, time, unit);
                    }
                } catch (Exception e) {
                    log.error("异步重建缓存失败，key: {}", key, e);
                } finally {
                    unLock(RedisConstants.LOCK_SHOP_KEY + id);
                }
            });
        }
        return data;
    }

    /**
     * 创建锁
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 封闭锁
     * @param key
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
