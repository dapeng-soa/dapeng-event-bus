package com.today.eventbus.annotation;

import java.lang.annotation.*;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 上午12:48
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface KafkaListener {

    Class<?> serializer();

}
