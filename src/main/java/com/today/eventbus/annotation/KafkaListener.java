package com.today.eventbus.annotation;


import com.today.eventbus.utils.Constant;

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

    String topic() default "";


    String groupId() default "";

    String serializer() default "";

    /**
     * hosts for kafka cluster,  users should set the real host by
     * System.setProperty(kafkaHostKey,"xxx") or export the env with the key.
     *
     * @return
     */
    String kafkaHostKey() default Constant.DEFAULT_CONSUMER_HOST_KEY;


}
