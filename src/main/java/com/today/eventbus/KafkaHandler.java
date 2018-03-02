package com.today.eventbus;

import java.lang.annotation.*;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:02
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface KafkaHandler {
}
