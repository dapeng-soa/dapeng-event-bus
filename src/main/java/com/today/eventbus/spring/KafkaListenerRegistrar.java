package com.today.eventbus.spring;

import com.today.binlog.BinlogKafkaConsumer;
import com.today.eventbus.ConsumerEndpoint;
import com.today.eventbus.MsgKafkaConsumer;
import com.today.eventbus.common.MsgConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.Lifecycle;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 描述: kafka 消费者 registry 注册 bean
 *
 * @author hz.lei
 * @since 2018年03月02日 上午1:29
 */
public class KafkaListenerRegistrar implements Lifecycle {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerRegistrar.class);
    private final List<ConsumerEndpoint> endpointDescriptors = new ArrayList<>();

    private volatile boolean isRunning = false;

    private ExecutorService executorService;

    private static final Map<String, MsgKafkaConsumer> EVENT_CONSUMERS = new HashMap<>();

    private static final Map<String, BinlogKafkaConsumer> BINLOG_CONSUMERS = new HashMap<>();

    public void registerEndpoint(ConsumerEndpoint endpoint) {
        Assert.notNull(endpoint, "Endpoint must be set");
        synchronized (this.endpointDescriptors) {
            addConsumer(endpoint);
        }
    }


    private void addConsumer(ConsumerEndpoint endpoint) {
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

    /**
     * for {@link MsgAnnotationBeanPostProcessor} to call when all singleton bean init.
     */
    public void afterPropertiesSet() {
        logger.info("ready to start consumer ,event consumer size {}, binlog consumer size {}", EVENT_CONSUMERS.size(), BINLOG_CONSUMERS.size());
        if ((EVENT_CONSUMERS.size() + BINLOG_CONSUMERS.size()) > 0) {
            //启动实例
            executorService = Executors.newFixedThreadPool(EVENT_CONSUMERS.size() + BINLOG_CONSUMERS.size());

            EVENT_CONSUMERS.values().forEach(executorService::execute);
            BINLOG_CONSUMERS.values().forEach(executorService::execute);
        }
    }

    @Override
    public void start() {
        logger.info("==============> begin to start KafkaListenerRegistrar");
        isRunning = true;
    }

    @Override
    public void stop() {
        logger.info("==============> begin to stop KafkaListenerRegistrar");
        EVENT_CONSUMERS.values().forEach(MsgConsumer::stopRunning);
        BINLOG_CONSUMERS.values().forEach(MsgConsumer::stopRunning);
        if (executorService != null) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
        logger.info("KafkaListenerRegistrar is already stopped!");
        isRunning = false;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
