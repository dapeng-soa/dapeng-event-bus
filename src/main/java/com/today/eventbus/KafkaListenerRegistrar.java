package com.today.eventbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:29
 */
public class KafkaListenerRegistrar implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerRegistrar.class);
    private final List<ConsumerEndpoint> endpointDescriptors = new ArrayList<>();

    public static final Map<String, MsgKafkaConsumer> TOPIC_CONSUMERS = new HashMap<>();

    public void registerEndpoint(ConsumerEndpoint endpoint) {
        Assert.notNull(endpoint, "Endpoint must be set");
        synchronized (this.endpointDescriptors) {
            addConsumer(endpoint);
        }
    }


    public void addConsumer(ConsumerEndpoint endpoint) {
        String groupId = endpoint.getGroupId();
        String topic = endpoint.getTopic();
        try {
            // 默认 group id
            String className = endpoint.getBean().getClass().getName();
            groupId = "".equals(groupId) ? className : groupId;
            String consumerKey = groupId + ":" + topic;

            if (TOPIC_CONSUMERS.containsKey(consumerKey)) {
                TOPIC_CONSUMERS.get(consumerKey).addCustomer(endpoint);
            } else {
                // KafkaConsumer consumer = new KafkaConsumer(groupId, topic);
                MsgKafkaConsumer consumer = new MsgKafkaConsumer(groupId, topic);
                consumer.addCustomer(endpoint);
                TOPIC_CONSUMERS.put(consumerKey, consumer);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void afterPropertiesSet() {
        logger.info("ready to start consumer ,consumer size {}", TOPIC_CONSUMERS.size());
        for (Map.Entry<String, MsgKafkaConsumer> entry : TOPIC_CONSUMERS.entrySet()) {
            entry.getValue().start();
        }
    }
}
