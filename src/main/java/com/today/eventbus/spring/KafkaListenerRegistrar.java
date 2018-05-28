package com.today.eventbus.spring;

import com.today.binlog.BinlogKafkaConsumer;
import com.today.eventbus.ConsumerEndpoint;
import com.today.eventbus.MsgKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 描述:
 *
 * @author hz.lei
 * @since 2018年03月02日 上午1:29
 */
public class KafkaListenerRegistrar implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerRegistrar.class);
    private final List<ConsumerEndpoint> endpointDescriptors = new ArrayList<>();

    public static final Map<String, MsgKafkaConsumer> EVENT_CONSUMERS = new HashMap<>();

    public static final Map<String, BinlogKafkaConsumer> BINLOG_CONSUMERS = new HashMap<>();

    public void registerEndpoint(ConsumerEndpoint endpoint) {
        Assert.notNull(endpoint, "Endpoint must be set");
        synchronized (this.endpointDescriptors) {
            addConsumer(endpoint);
        }
    }


    public void addConsumer(ConsumerEndpoint endpoint) {
        String groupId = endpoint.getGroupId();
        String topic = endpoint.getTopic();
        String kafkaHost = endpoint.getKafkaHost();
        try {
            // 默认 group id
            String className = endpoint.getBean().getClass().getName();
            groupId = "".equals(groupId) ? className : groupId;
            String consumerKey = groupId + ":" + topic;
            //判断类型 binlog   or   event-bus ？
            if (endpoint.getBinlog()) {
                // binlog
                if (BINLOG_CONSUMERS.containsKey(consumerKey)) {
                    BINLOG_CONSUMERS.get(consumerKey).addConsumer(endpoint);
                } else {
                    BinlogKafkaConsumer consumer = new BinlogKafkaConsumer(kafkaHost, groupId, topic);
                    consumer.addConsumer(endpoint);
                    BINLOG_CONSUMERS.put(consumerKey, consumer);
                }

            } else {
                // event
                if (EVENT_CONSUMERS.containsKey(consumerKey)) {
                    EVENT_CONSUMERS.get(consumerKey).addConsumer(endpoint);
                } else {
                    MsgKafkaConsumer consumer = new MsgKafkaConsumer(kafkaHost, groupId, topic);
                    consumer.addConsumer(endpoint);
                    EVENT_CONSUMERS.put(consumerKey, consumer);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void afterPropertiesSet() {
        logger.info("ready to start consumer ,event consumer size {}, binlog consumer size {}", EVENT_CONSUMERS.size(), BINLOG_CONSUMERS.size());

        //启动实例
        ExecutorService executorService = Executors.newFixedThreadPool(EVENT_CONSUMERS.size() + BINLOG_CONSUMERS.size());

        EVENT_CONSUMERS.values().forEach(consumer -> executorService.execute(consumer));
        BINLOG_CONSUMERS.values().forEach(consumer -> executorService.execute(consumer));
    }
}
