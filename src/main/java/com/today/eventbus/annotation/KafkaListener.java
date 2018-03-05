package com.today.eventbus.annotation;

import java.lang.annotation.*;

/**
 * 描述: 方法上注解，标明此方法是一个kafka message 消息监听器
 *
 * @author hz.lei
 * @date 2018年03月02日 上午12:48
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface KafkaListener {

    Class<?> serializer();

}
