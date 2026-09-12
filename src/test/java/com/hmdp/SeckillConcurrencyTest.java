package com.hmdp;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.thread.ThreadUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IUserService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 秒杀并发正确性压测：不超卖 + 一人一单
 *
 * <p>用真实 HTTP（而非 MockMvc）发起并发，因为只有经过 Tomcat 线程池才是真正的端到端链路。
 * MockMvc 直接调用 DispatcherServlet、不经过容器线程池，说服力弱一档。</p>
 *
 * <p><b>为什么这个测试不需要"完美同时"</b>：正确性由 {@code seckill.lua} 的原子性保证，
 * 不依赖请求到达的时序——Redis 单线程执行脚本，并发请求天然串行。本测试真正要证的是
 * <b>端到端的计数守恒</b>：100 个名额、上千竞争者打进来，最后账必须对得上。
 * 所以即便 Tomcat 线程池只有 200、部分请求实际是排队进来的，断言依然成立。</p>
 *
 * <p><b>前置条件</b>：MySQL / Redis / RocketMQ 三个依赖均已启动（本测试会连真实容器）。</p>
 *
 * <p><b>入口限流怎么绕过</b>：秒杀接口挂 {@code @RateLimit(time=1, count=5, limitType=IP)}，
 * 本机压测所有请求的 {@code remoteAddr} 都是 127.0.0.1，每秒只放行 5 个请求、测试必然失败。
 * 这里利用 {@code RateLimitAspect#getIp()} 优先读 {@code X-Forwarded-For} 的行为，
 * 给每个请求伪造不同的 IP 头，让它们各落各的限流桶。
 * 注意：这恰恰暴露了 IP 限流的真实风险——<b>XFF 是客户端可控的</b>，详见面试文档 Q8。</p>
 *
 * <p><b>副作用与清理</b>：测试会插入临时券与订单，结束后按 voucherId 清理 DB 与 Redis。
 * 订单号 {@code RedisIdWorker} 无机器位、是"时间戳+序列"拼接，无法按 ID 区间清扫，
 * 故一律按 voucher_id 删除。</p>
 */
@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
class SeckillConcurrencyTest {

    // ---------------- 压测参数 ----------------
    /** 并发竞争者数量（受限于 tb_user 里的用户数，实际取两者较小值） */
    private static final int CONCURRENCY = 1000;
    /** 秒杀券库存：最终应当恰好卖出这么多单 */
    private static final int STOCK = 100;
    /** 等待异步落库完成的上限 */
    private static final long DRAIN_TIMEOUT_MS = 30_000L;

    @Resource
    private MockMvc mockMvc;

    @Resource
    private IUserService userService;

    @Resource
    private IVoucherService voucherService;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ObjectMapper mapper;

    @LocalServerPort
    private int port;

    @Test
    @SneakyThrows
    @DisplayName("1000 并发抢 100 库存：不超卖 + 一人一单")
    void seckillShouldNotOversell() {
        // 0.建券预热（复用生产的 addSeckillVoucher，顺带校验预热链路本身是对的）
        Long voucherId = seedSeckillVoucher();
        log.info("压测券已就绪，voucherId: {}，库存: {}", voucherId, STOCK);

        try {
            // 1.准备令牌：按 VOICE 阈值放大 TTL，避免 1000 次登录把最早那批 token 挤过期
            List<String> tokens = obtainTokens();
            Assert.isTrue(tokens.size() > STOCK,
                    String.format("竞争者数量(%d)必须大于库存(%d)，否则构不成超卖竞争", tokens.size(), STOCK));
            log.info("令牌准备完成，竞争并发数: {}", tokens.size());

            // 2.并发抢购
            List<String> bodies = fireSeckill(voucherId, tokens);

            // 3.统计响应侧结果
            int successCount = 0;
            int failCount = 0;
            for (String body : bodies) {
                if (body != null && body.contains("\"success\":true")) {
                    successCount++;
                } else {
                    failCount++;
                }
            }
            log.info("压测完成：成功 {}，失败 {}", successCount, failCount);

            // 4.等待异步落库排空（MQ 消费者是异步的，不能立即断言 DB）
            long dbOrders = awaitDbOrdersSettled(voucherId, successCount);

            // 5.断言：不超卖
            Assert.isTrue(successCount == STOCK,
                    String.format("❌ 超卖或少卖！成功响应 %d 笔，预期恰好 %d 笔", successCount, STOCK));
            Assert.isTrue(dbOrders == STOCK,
                    String.format("❌ DB 落库订单数 %d，预期 %d（可能 MQ 丢消息或消费者异常）", dbOrders, STOCK));

            // 6.断言：一人一单
            long duplicated = countDuplicateUsers(voucherId);
            Assert.isTrue(duplicated == 0,
                    String.format("❌ 一人一单失效！有 %d 个用户重复下单", duplicated));

            // 7.断言：Redis 侧也收敛到 0，且下单集合大小与卖出数一致
            String remain = stringRedisTemplate.opsForValue()
                    .get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
            Assert.isTrue("0".equals(remain),
                    String.format("❌ Redis 剩余库存为 %s，预期 0", remain));

            Long orderSetSize = stringRedisTemplate.opsForSet()
                    .size(RedisConstants.SECKILL_ORDER_KEY + voucherId);
            Assert.isTrue(orderSetSize != null && orderSetSize == STOCK,
                    String.format("❌ Redis 下单集合大小 %s，预期 %d", orderSetSize, STOCK));

            log.info("✅ 压测通过：并发 {} 抢 {} 库存，恰好成交 {} 笔，无超卖、无重复下单",
                    tokens.size(), STOCK, successCount);
        } finally {
            // 8.清理：无论成败都清干净，避免污染后续测试与对账任务
            cleanup(voucherId);
        }
    }

    // ==================== 建券与预热 ====================

    /**
     * 建券并预热 Redis。复用生产的 {@code addSeckillVoucher}，
     * 这样预热逻辑本身也顺带被覆盖到（见 VoucherServiceImpl#addSeckillVoucher）。
     */
    private Long seedSeckillVoucher() {
        Voucher voucher = new Voucher();
        voucher.setShopId(1L);
        voucher.setTitle("【压测】秒杀券-" + System.currentTimeMillis());
        voucher.setSubTitle("并发正确性压测专用，测试结束自动清理");
        voucher.setRules("压测数据");
        voucher.setPayValue(1L);
        voucher.setActualValue(100L);
        voucher.setType(1);
        voucher.setStatus(1);
        voucher.setStock(STOCK);
        voucher.setBeginTime(LocalDateTime.now().minusHours(1));
        voucher.setEndTime(LocalDateTime.now().plusHours(1));

        voucherService.addSeckillVoucher(voucher);
        return voucher.getId();
    }

    // ==================== 令牌准备 ====================

    /**
     * 用真实登录接口批量换取 token。
     * <p>这一步走 MockMvc（而非真实 HTTP）：它只是<b>数据准备</b>，不是被测对象，
     * 用 MockMvc 可以省掉上千次真实网络往返，让测试跑得快些。</p>
     */
    @SneakyThrows
    private List<String> obtainTokens() {
        List<String> phoneList = userService.lambdaQuery()
                .select(User::getPhone)
                .last("limit " + CONCURRENCY)
                .list().stream().map(User::getPhone).collect(Collectors.toList());

        Assert.isTrue(phoneList.size() > STOCK,
                String.format("tb_user 用户数(%d)必须大于库存(%d)，请先灌入足够的测试用户", phoneList.size(), STOCK));

        ExecutorService executor = ThreadUtil.newExecutor(phoneList.size());
        List<String> tokenList = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(phoneList.size());

        phoneList.forEach(phone -> executor.execute(() -> {
            try {
                // 1.取验证码
                String codeJson = mockMvc.perform(MockMvcRequestBuilders
                                .post("/user/code")
                                .queryParam("phone", phone))
                        .andExpect(MockMvcResultMatchers.status().isOk())
                        .andReturn().getResponse().getContentAsString();
                Result codeResult = mapper.readerFor(Result.class).readValue(codeJson);
                if (codeResult.getSuccess() == null || !codeResult.getSuccess()) {
                    return;
                }
                String code = codeResult.getData().toString();

                // 2.登录取 token
                LoginFormDTO form = LoginFormDTO.builder().code(code).phone(phone).build();
                String tokenJson = mockMvc.perform(MockMvcRequestBuilders
                                .post("/user/login")
                                .content(mapper.writeValueAsString(form))
                                .contentType(MediaType.APPLICATION_JSON))
                        .andExpect(MockMvcResultMatchers.status().isOk())
                        .andReturn().getResponse().getContentAsString();
                Result tokenResult = mapper.readerFor(Result.class).readValue(tokenJson);
                if (tokenResult.getSuccess() != null && tokenResult.getSuccess()) {
                    tokenList.add(tokenResult.getData().toString());
                }
            } catch (Exception e) {
                log.warn("用户 {} 登录取 token 失败: {}", phone, e.getMessage());
            } finally {
                latch.countDown();
            }
        }));

        latch.await(5, TimeUnit.MINUTES);
        executor.shutdown();
        return tokenList;
    }

    // ==================== 并发抢购 ====================

    /**
     * 用真实 HTTP 发起并发抢购。
     * <p>所有线程用同一个 {@link CountDownLatch} 起跑线等待，再同时放行，
     * 尽量让请求在同一瞬间压在 Redis 的 Lua 脚本上。</p>
     */
    @SneakyThrows
    private List<String> fireSeckill(Long voucherId, List<String> tokens) {
        RestTemplate restTemplate = buildRestTemplate();
        String url = "http://127.0.0.1:" + port + "/voucher-order/seckill/" + voucherId;

        ExecutorService executor = ThreadUtil.newExecutor(tokens.size());
        List<String> bodies = new CopyOnWriteArrayList<>();
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(tokens.size());
        AtomicInteger httpErrors = new AtomicInteger();

        for (int i = 0; i < tokens.size(); i++) {
            final String token = tokens.get(i);
            // 每个请求伪造一个不同的来源 IP，绕开 IP 维度限流
            final String fakeIp = "10.0." + (i / 250) + "." + (i % 250 + 1);

            executor.execute(() -> {
                try {
                    startGate.await();
                    HttpHeaders headers = new HttpHeaders();
                    headers.set("authorization", token);
                    headers.set("X-Forwarded-For", fakeIp);
                    String body = restTemplate.exchange(
                            url, HttpMethod.POST, new HttpEntity<>(headers), String.class).getBody();
                    bodies.add(body == null ? "" : body);
                } catch (Exception e) {
                    httpErrors.incrementAndGet();
                    bodies.add("{\"success\":false,\"errorMsg\":\"http error\"}");
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // 同时放行
        startGate.countDown();
        boolean done = finishLatch.await(3, TimeUnit.MINUTES);
        executor.shutdown();

        Assert.isTrue(done, "并发请求未在超时时间内全部返回");
        if (httpErrors.get() > 0) {
            log.warn("有 {} 个请求发生传输层异常（计入失败响应）", httpErrors.get());
        }
        Assert.isTrue(bodies.size() == tokens.size(),
                String.format("响应数 %d 与请求数 %d 不一致", bodies.size(), tokens.size()));
        return bodies;
    }

    /**
     * 构造带超时的 RestTemplate。
     * <p>必须设超时：默认的 {@code SimpleClientHttpRequestFactory} 超时是无限，
     * 一旦有请求卡住会直接把测试挂死。</p>
     */
    private RestTemplate buildRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10_000);
        factory.setReadTimeout(60_000);
        return new RestTemplate(factory);
    }

    // ==================== 断言辅助 ====================

    /**
     * 等待异步落库排空：轮询 DB 订单数，直到达到预期或超时。
     * <p>秒杀走的是"Redis 预扣 + MQ 异步落库"，响应返回时 DB 里可能还没有订单，
     * 因此不能拿到响应就立刻断言 DB。</p>
     */
    @SneakyThrows
    private long awaitDbOrdersSettled(Long voucherId, int expected) {
        long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        long count = 0;
        while (System.currentTimeMillis() < deadline) {
            count = countOrders(voucherId);
            if (count >= expected && expected > 0) {
                break;
            }
            Thread.sleep(500);
        }
        return count;
    }

    private long countOrders(Long voucherId) {
        return voucherOrderService.query()
                .eq("voucher_id", voucherId)
                .count();
    }

    /** 统计重复下单的用户数，期望为 0（一人一单的核心断言） */
    private long countDuplicateUsers(Long voucherId) {
        List<VoucherOrder> orders = voucherOrderService.query()
                .eq("voucher_id", voucherId)
                .list();
        return orders.stream()
                .collect(Collectors.groupingBy(VoucherOrder::getUserId, Collectors.counting()))
                .values().stream()
                .filter(c -> c > 1)
                .count();
    }

    // ==================== 清理 ====================

    /**
     * 按 voucherId 清掉本次压测产生的所有痕迹：订单、券、秒杀券、Redis key。
     * <p>订单号由 {@code RedisIdWorker} 生成、无机器位，无法按 ID 区间清扫，只能按 voucher_id 删。</p>
     */
    private void cleanup(Long voucherId) {
        try {
            int removed = voucherOrderService.getBaseMapper().delete(
                    new QueryWrapper<VoucherOrder>().eq("voucher_id", voucherId));
            seckillVoucherService.removeById(voucherId);
            voucherService.removeById(voucherId);
            stringRedisTemplate.delete(RedisConstants.SECKILL_STOCK_KEY + voucherId);
            stringRedisTemplate.delete(RedisConstants.SECKILL_ORDER_KEY + voucherId);
            log.info("压测数据已清理：voucherId: {}，删除订单 {} 条", voucherId, removed);
        } catch (Exception e) {
            log.error("清理压测数据失败，请手动检查 voucherId: {}", voucherId, e);
        }
    }
}
