package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.PayCallbackDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
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

    /**
     * 脚本初始化
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /** 订单状态（与 tb_voucher_order.status 对应） */
    private static final int STATUS_UNPAID = 1;
    private static final int STATUS_PAID = 2;
    private static final int STATUS_CANCELED = 4;

    /** 落库消息 topic（秒杀下单异步落库，延迟级别 3 ≈ 10s） */
    private static final String ORDER_TOPIC = "seckill-order-topic:seckill";
    /** 超时关单 topic（延迟消息驱动，消费者 OrderTimeoutListener） */
    private static final String CLOSE_ORDER_TOPIC = "seckill-order-close-topic:close";
    /** 超时关单延迟级别：14 = 10 分钟（RocketMQ 固定延迟级别表），与超时阈值一致 */
    private static final int CLOSE_DELAY_LEVEL = 14;

//    //阻塞队列，线程从中获取时，如果为空，则线程阻塞
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//
//    @PostConstruct
//    private void init(){
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//    private class VoucherOrderHandler implements Runnable {
//        String queueName="stream.orders";
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //1.获取消息队列中的队列信息
//                    //XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.orders >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                    );
//                    //2.判断消息获取是否成功
//                    if (list == null || list.isEmpty()) {
//                        //2.1.如果获取失败，说明没有消息，继续下一次循环
//                        continue;
//                    }
//                    //3.解析消息中的订单信息
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//
//                    //4.如果获取成功，可以下单
//                    handleVoucherOrder(voucherOrder);
//
//                    //5.ACK确认 SACK stream.orders g1 id
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//
//                }catch (Exception e){
//                    log.error("处理订单异常",e);
//                    try {
//                        handPendingList();
//                    } catch (InterruptedException ex) {
//                        throw new RuntimeException(ex);
//                    }
//                }
//            }
//
//        }
//
//        private void handPendingList() throws InterruptedException {
//            while (true) {
//                try {
//                    //1.获取pending-list中的队列信息
//                    //XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.orders 0
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1),
//                            StreamOffset.create(queueName, ReadOffset.from("0"))
//                    );
//                    //2.判断消息获取是否成功
//                    if (list == null || list.isEmpty()) {
//                        //2.1.如果获取失败，说明pending-list没有消息，结束循环
//                        break;
//                    }
//                    //3.解析消息中的订单信息
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//
//                    //4.如果获取成功，可以下单
//                    handleVoucherOrder(voucherOrder);
//
//                    //5.ACK确认 SACK stream.orders g1 id
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//                }catch (Exception e){
//                    log.error("处理pending-list订单异常",e);
//                    Thread.sleep(20);
//                }
//            }
//        }
//    }
   /* private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的队列信息
                    VoucherOrder order = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(order);

                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }

            }

        }
    }*/

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
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }
        // 2. 脱离请求线程，发消息给 RocketMQ
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        // 你可以用 JSON，也可以用序列化
        // 增加消息发送的异常处理
        //放入mq
        String jsonStr = JSONUtil.toJsonStr(order);
        // 1.落库消息（延迟级别 3 ≈ 10s，模拟原 RabbitMQ QA 队列的 10s TTL 延时语义）
        try {
            rocketMQTemplate.syncSend(ORDER_TOPIC,
                    org.springframework.messaging.support.MessageBuilder.withPayload(jsonStr).build(),
                    3000, 3);
        } catch (Exception e) {
            log.error("发送 RocketMQ 落库消息失败，订单ID: {}", orderId, e);
            throw new RuntimeException("发送消息失败");
        }
        // 2.超时关单延迟消息：到期（≈10 分钟）由 OrderTimeoutListener 检查关单；
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


//    public Result seckillVoucher(Long voucherId) {
//        //查询用户券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀时间
//        //是否开始
//        LocalDateTime beginTime = voucher.getBeginTime();
//        if(beginTime.isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始！");
//        }
//        //是否结束
//        LocalDateTime endTime = voucher.getEndTime();
//        if(endTime.isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        //判断库存呢是否充足
//        if(voucher.getStock()<=0){
//            return Result.fail("库存不足！");
//        }
//        Long userId = UserHolder.getUser().getId();
//       //创建锁对象
//        //SimpleRedisLock  lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否获取锁成功
//        if(!isLock) {
//            //失败，返回错误或重试
//            return Result.fail("不允许重复下单");
//
//        }
//        try {
//            //直接调用，不会触发spring aop的事务管理
//            //要通过代理调用，获取代理对象，才会被spring aop拦截
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//
//
//    }

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
     *   <li>Redis 回补：库存 +1、移除用户下单记录（恢复一人一单资格）。</li>
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
        // 释放 Redis 预扣：补库存 + 移除下单用户记录（恢复一人一单资格）
        stringRedisTemplate.opsForValue()
                .increment(RedisConstants.SECKILL_STOCK_KEY + order.getVoucherId());
        stringRedisTemplate.opsForSet()
                .remove(RedisConstants.SECKILL_ORDER_KEY + order.getVoucherId(), order.getUserId().toString());
        log.info("订单超时关单成功并释放库存，orderId: {}, voucherId: {}", order.getId(), order.getVoucherId());
        return true;
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        //查询订单
        Long userId =voucherOrder.getUserId();
            Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            //判断是否存在
            if (count > 0) {
                //用户已经购买过了
                log.error("用户已经购买过一次了");
                return;
            }
            //扣减库存
            boolean success = seckillVoucherService
                    .update()
                    .setSql("stock=stock-1")
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)
                    .update();
            if (!success) {
                log.error("库存不足");
                return ;
            }

            save(voucherOrder);

    }
}
