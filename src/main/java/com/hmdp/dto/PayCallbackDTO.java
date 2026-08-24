package com.hmdp.dto;

import lombok.Data;

/**
 * 支付回调请求参数
 * <p>
 * 模拟第三方支付平台的支付成功回调。生产环境应按支付渠道（支付宝 / 微信）的规范
 * 做验签与幂等校验，此处仅保留核心字段用于演示订单状态流转。
 * </p>
 */
@Data
public class PayCallbackDTO {

    /** 订单号 */
    private Long orderId;

    /** 支付流水号（第三方支付平台生成） */
    private String tradeNo;

    /** 实际支付金额（分），用于与订单应付金额对账 */
    private Long payAmount;
}
