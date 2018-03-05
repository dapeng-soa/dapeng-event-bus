package com.today.eventbus.annotation;

import com.today.eventbus.utils.Constant;

import java.lang.annotation.*;

/**
 * 描述: 加在类注解上,说明它是一个kafka 消息 消费者
 *
 * @author hz.lei
 * @date 2018年03月05日 下午4:03
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface KafkaConsumer {

    String topic();

    String groupId();

    /**
     * hosts for kafka cluster,  users should set the real host by
     * System.setProperty(kafkaHostKey,"xxx") or export the env with the key.
     *
     * @return
     */
    String kafkaHostKey() default Constant.DEFAULT_CONSUMER_HOST_KEY;


}
