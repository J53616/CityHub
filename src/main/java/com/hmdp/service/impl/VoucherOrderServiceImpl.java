package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.PayCallbackDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SeckillStockCache;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RocketMQTemplate rocketMQTemplate;
    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillStockCache seckillStockCache;

    /**
     * 脚本初始化
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    /** 关单库存回滚脚本（Lua 原子执行：回补库存 + 移除下单记录，见 restore.lua） */
    private static final DefaultRedisScript<Long> RESTORE_SCRIPT;
    static {
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        RESTORE_SCRIPT = new DefaultRedisScript<>();
        RESTORE_SCRIPT.setLocation(new ClassPathResource("restore.lua"));
        RESTORE_SCRIPT.setResultType(Long.class);
    }

    /** 订单状态（与 tb_voucher_order.status 对应） */
    private static final int STATUS_UNPAID = 1;
    private static final int STATUS_PAID = 2;
    private static final int STATUS_CANCELED = 4;

    /** 落库消息 topic（秒杀下单异步落库，立即投递不延迟） */
    private static final String ORDER_TOPIC = "seckill-order-topic:seckill";
    /** 超时关单 topic（延迟消息驱动，消费者 OrderTimeoutListener） */
    private static final String CLOSE_ORDER_TOPIC = "seckill-order-close-topic:close";
    /**
     * 超时关单延迟级别：5 = 1 分钟（RocketMQ 默认延迟级别表 1s/5s/10s/30s/1m/2m/...，
     * 1 分钟对应 level 5），与 OrderTimeoutTask 的超时阈值 1 分钟一致。
     * 说明：RocketMQ 无“2 分钟”以外的细粒度精确档位，秒杀场景选 1 分钟档，将“超时未支付”窗口压缩到最短可配置档。
     */
    private static final int CLOSE_DELAY_LEVEL = 5;

    /**
     * 秒杀链路演进（历史实现已删除，完整旧代码见 git 历史）：
     * <ol>
     *   <li>资格校验与扣减：DB 悲观锁 → DB 乐观锁 → Redis+Lua 原子脚本（当前，见 seckill.lua）；</li>
     *   <li>异步落库：曾尝试 Redis Stream 消费者、JVM 阻塞队列，最终采用 RocketMQ 立即投递解耦削峰；</li>
     *   <li>一人一单：曾用 Redisson 分布式锁串行化，后由 Lua 原子脚本替代，不再需要分布式锁。</li>
     * </ol>
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2.判断结果是否为0
        int r = 0;
        if (result != null) {
            r = result.intValue();
        }
        if(r!=0){
            //2.1.不为0，代表没有购买资格
            if (r == 1) {
                return Result.fail("库存不足");
            }
            if (r == -1) {
                // 库存 key 不存在：秒杀券不存在或 Redis 库存未初始化（见 VoucherServiceImpl#addSeckillVoucher）
                return Result.fail("秒杀已结束或不存在");
            }
            return Result.fail("不能重复下单");
        }
        // 2.0 预扣成功，主动失效本地二级缓存，让秒杀页剩余库存立即可见（无需等 1s TTL）
        seckillStockCache.invalidate(voucherId);
        // 2.1 脱离请求线程，发消息给 RocketMQ
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        // 你可以用 JSON，也可以用序列化
        // 增加消息发送的异常处理
        //放入mq
        String jsonStr = JSONUtil.toJsonStr(order);
        // 1.落库消息（delayLevel=0，立即投递）：异步只是把 DB 写从请求线程剥离以削峰，
        //   不能延迟落库——否则用户秒付时 DB 尚无订单，payCallback 会查不到订单（原 RabbitMQ 版 10s 延时是历史遗留）
        try {
            rocketMQTemplate.syncSend(ORDER_TOPIC,
                    org.springframework.messaging.support.MessageBuilder.withPayload(jsonStr).build(),
                    3000, 0);
        } catch (Exception e) {
            log.error("发送 RocketMQ 落库消息失败，订单ID: {}", orderId, e);
            throw new RuntimeException("发送消息失败");
        }
        // 2.超时关单延迟消息：到期（1 分钟，delayLevel=5）由 OrderTimeoutListener 检查关单；
        //   发送失败不阻塞下单，由 OrderTimeoutTask 定时扫描兜底关闭
        try {
            rocketMQTemplate.syncSend(CLOSE_ORDER_TOPIC,
                    org.springframework.messaging.support.MessageBuilder.withPayload(String.valueOf(orderId)).build(),
                    3000, CLOSE_DELAY_LEVEL);
        } catch (Exception e) {
            log.warn("发送超时关单延迟消息失败，订单ID: {}，将由定时扫描兜底关闭", orderId, e);
        }
        // 3. 返回订单号给前端（实际下单异步处理）
        return Result.ok(orderId);
    }


    /**
     * 支付回调：乐观锁更新订单状态
     * <p>
     * 利用 CAS 条件 {@code status = 1}（未支付）实现"支付成功"与"超时关单"的并发互斥：
     * 两个 SQL 同时执行时，数据库行锁保证只有一个更新成功，成功者驱动订单进入对应终态。
     * </p>
     * <ul>
     *   <li>更新成功：订单 未支付(1) → 已支付(2)，记录支付时间与流水号。</li>
     *   <li>影响 0 行：说明订单已不是未支付态——已被定时任务关单（已取消 4），
     *       此时触发"原路退回"（退款），避免用户已付款但库存被释放导致的超卖风险。</li>
     * </ul>
     */
    @Transactional
    public Result payCallback(PayCallbackDTO dto) {
        Long orderId = dto.getOrderId();
        // 1.乐观锁：只有状态为"未支付"才流转为"已支付"
        boolean updated = update()
                .set("status", STATUS_PAID)
                .set("pay_type", 1)
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("status", STATUS_UNPAID)
                .update();
        if (updated) {
            log.info("支付回调成功，orderId: {}", orderId);
            return Result.ok("支付成功");
        }
        // 2.影响 0 行：订单可能已被关单，或本就不存在
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return Result.fail("订单不存在");
        }
        if (order.getStatus() == STATUS_CANCELED) {
            // 2.1 已被定时任务关单（已取消），触发原路退回（退款）
            // 此处仅记录日志，生产环境应调用支付渠道退款接口，并保证退款幂等
            log.warn("订单已超时取消，支付回调触发原路退回，orderId: {}, tradeNo: {}", orderId, dto.getTradeNo());
            return Result.fail("订单已取消，将自动退款");
        }
        // 2.2 已是已支付等终态，重复回调
        return Result.fail("订单状态已变更，忽略重复回调");
    }

    /**
     * 超时关单（延迟消息消费者入口）：先按订单号查询，存在则委托 {@link #doCloseTimeoutOrder} 完成关单 + 库存释放。
     *
     * @return 是否真正关单成功
     */
    @Override
    @Transactional
    public boolean closeTimeoutOrder(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            log.warn("关单时订单不存在，orderId: {}", orderId);
            return false;
        }
        return doCloseTimeoutOrder(order);
    }

    /**
     * 超时关单（定时扫描兜底入口）：委托 {@link #doCloseTimeoutOrder} 完成关单 + 库存释放。
     *
     * @return 是否真正关单成功
     */
    @Override
    @Transactional
    public boolean closeTimeoutOrder(VoucherOrder order) {
        return doCloseTimeoutOrder(order);
    }

    /**
     * 关单核心逻辑（幂等，乐观锁）：
     * <ol>
     *   <li>CAS 关单：{@code update tb_voucher_order set status=4 where id=? and status=1}，
     *       与支付回调（status=1→2）互斥，已支付 / 已关闭订单影响 0 行；</li>
     *   <li>DB 秒杀库存回补；</li>
     *   <li>Redis 回补：Lua 原子执行【库存 +1、移除用户下单记录（恢复一人一单资格）】，见 restore.lua。</li>
     * </ol>
     */
    private boolean doCloseTimeoutOrder(VoucherOrder order) {
        boolean closed = update()
                .set("status", STATUS_CANCELED)
                .set("pay_time", null)
                .eq("id", order.getId())
                .eq("status", STATUS_UNPAID)
                .update();
        if (!closed) {
            // 期间已被支付或已关闭，跳过
            return false;
        }
        // 数据库秒杀库存回补（CAS：stock = stock + 1）
        seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", order.getVoucherId())
                .update();
        // 释放 Redis 预扣：Lua 原子执行【回补库存 + 移除下单用户记录（恢复一人一单资格）】。
        // 相比原先 increment + sremove 两条非原子命令，Lua 保证两步要么都成功要么都不成功，
        // 避免“回补一半崩溃”导致 Redis 库存与下单记录漂移。restore.lua 返回 -1 表示用户记录已不在
        // （幂等命中，此前已释放），此时同样视为回补生效，不再重复加库存。
        Long restored = stringRedisTemplate.execute(
                RESTORE_SCRIPT,
                Collections.emptyList(),
                order.getUserId().toString(), order.getVoucherId().toString());
        // 回补后主动失效本地二级缓存，让释放的库存立即可抢（无需等 1s TTL）
        seckillStockCache.invalidate(order.getVoucherId());
        log.info("订单超时关单成功并释放库存，orderId: {}, voucherId: {}, Redis 回补后库存: {}",
                order.getId(), order.getVoucherId(), restored);
        return true;
    }
}
