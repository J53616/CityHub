package com.hmdp.service;

import com.hmdp.dto.PayCallbackDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    void createVoucherOrder(VoucherOrder voucherOrder);

    /**
     * 支付成功回调：乐观锁更新订单状态为已支付
     *
     * @param payCallbackDTO 支付回调参数
     * @return 处理结果
     */
    Result payCallback(PayCallbackDTO payCallbackDTO);

    /**
     * 超时关单（延迟消息消费者入口）：按订单号查询后关闭超时未支付订单并释放库存
     *
     * @param orderId 订单号
     * @return 是否真正关单成功
     */
    boolean closeTimeoutOrder(Long orderId);

    /**
     * 超时关单（定时扫描兜底入口）：关闭超时未支付订单并释放库存
     *
     * @param order 已查询出的超时未支付订单
     * @return 是否真正关单成功
     */
    boolean closeTimeoutOrder(VoucherOrder order);
}
