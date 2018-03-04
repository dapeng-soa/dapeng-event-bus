package com.today.eventbus.annotation;

import com.today.eventbus.utils.Contans;

import java.lang.annotation.*;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 上午12:48
 */
@Target({ElementType.METHOD,ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface KafkaListener {

    String topic();


    String groupId();

    String serializer();

    /**
     * hosts for kafka cluster,  users should set the real host by
     * System.setProperty(kafkaHostKey,"xxx") or export the env with the key.
     *
     * @return
     */
    String kafkaHostKey() default Contans.DEFAULT_CONSUMER_HOST_KEY;


}
