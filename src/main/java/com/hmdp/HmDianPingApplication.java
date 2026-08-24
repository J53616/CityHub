package com.hmdp;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.apache.rocketmq.spring.autoconfigure.RocketMQAutoConfiguration;

@EnableAspectJAutoProxy(exposeProxy = true)
@MapperScan("com.hmdp.mapper")
@SpringBootApplication
@EnableScheduling
@Import(RocketMQAutoConfiguration.class)
public class HmDianPingApplication {
    //
    public static void main(String[] args) {
        SpringApplication.run(HmDianPingApplication.class, args);
    }

}
