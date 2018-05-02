package com.today.eventbus.rest.support;

import org.simpleframework.xml.core.Persister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:29
 */
public class RestListenerFactoryBean implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(RestListenerFactoryBean.class);

    public static final Map<String, RestKafkaConsumer> REST_CONSUMERS = new HashMap<>();


    public void registerEndpoint() {
        Persister persister = new Persister();
        RestConsumerConfig config = null;
        try {
            config = persister.read(RestConsumerConfig.class,
                    ResourceUtils.getFile("classpath:rest-consumer.xml"));
        } catch (FileNotFoundException e) {
            logger.error("配置文件rest-consumer.xml 在classpath路径下不存在，请进行配置", e);
            throw new RuntimeException("配置文件rest-consumer.xml 在classpath路径下不存在，请进行配置");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        Assert.notNull(config, "Endpoint must be set");
        config.getRestConsumerEndpoints().forEach(endpoint -> {
            addConsumer(endpoint);
        });
    }

    public void addConsumer(RestConsumerEndpoint endpoint) {
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

    @Override
    public void afterPropertiesSet() {
        registerEndpoint();
        logger.info("[RestConsumer]: ready to start rest consumer ,rest event consumer size {}", REST_CONSUMERS.size());
        REST_CONSUMERS.values().forEach(Thread::start);
    }

}
