package com.today.eventbus.rest.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.HashMap;
import java.util.Map;

/**
 * 描述: RestListenerFactory
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:29
 */
public class RestListenerFactory implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(RestListenerFactory.class);

    private static final Map<String, RestKafkaConsumer> REST_CONSUMERS = new HashMap<>();


    @Override
    public void afterPropertiesSet() {
        registerEndpoint();
        logger.info("[RestConsumer]: ready to start rest consumer ,rest event consumer size {}", REST_CONSUMERS.size());
        REST_CONSUMERS.values().forEach(Thread::start);
    }


    private void registerEndpoint() {
        RestConsumerConfig config = ParserUtil.getConsumerConfig();
        config.getRestConsumerEndpoints().forEach(endpoint -> addConsumer(endpoint));
    }


    private void addConsumer(RestConsumerEndpoint endpoint) {
        String groupId = endpoint.getGroupId();
        String topic = endpoint.getTopic();
        String kafkaHost = endpoint.getKafkaHost();
        try {
            // 默认 group id
            String consumerKey = groupId + ":" + topic;

            if (REST_CONSUMERS.containsKey(consumerKey)) {
                REST_CONSUMERS.get(consumerKey).addConsumer(endpoint);
            } else {
                RestKafkaConsumer consumer = new RestKafkaConsumer(kafkaHost, groupId, topic);
                consumer.addConsumer(endpoint);
                REST_CONSUMERS.put(consumerKey, consumer);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
