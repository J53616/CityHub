package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherService extends IService<Voucher> {

    Result queryVoucherOfShop(Long shopId);

    void addSeckillVoucher(Voucher voucher);

    /**
     * 查询秒杀券实时剩余库存（走 Caffeine 本地一级 + Redis 二级缓存，不触 DB）
     * <p>秒杀页轮询"还剩多少"的高频读用此接口，避免每次穿透单节点 Redis。</p>
     *
     * @param voucherId 秒杀券 id
     * @return 剩余库存（≥0）；若券不存在/未预热返回 -1，便于前端区分"售罄"与"秒杀未开始/不存在"
     */
    Integer querySeckillStock(Long voucherId);
}
