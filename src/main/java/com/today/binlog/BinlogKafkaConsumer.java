package com.today.binlog;

import com.today.eventbus.ConsumerEndpoint;
import com.today.eventbus.MsgKafkaConsumer;
import com.today.eventbus.config.KafkaConfigBuilder;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 描述: 处理 binlog 缓存 监听 事件
 *
 * @author hz.lei
 * @date 2018年03月07日 上午1:42
 */
public class BinlogKafkaConsumer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(MsgKafkaConsumer.class);
    private String groupId;
    private String topic;
    private String kafkaConnect;
    private List<ConsumerEndpoint> binlogConsumer = new ArrayList<>();
    protected KafkaConsumer<Integer, byte[]> consumer;

    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     */
    public BinlogKafkaConsumer(String kafkaHost, String groupId, String topic) {
        this.kafkaConnect = kafkaHost;
        this.groupId = groupId;
        this.topic = topic;
        this.init();
    }

    private void init() {
        logger.info(new StringBuffer("[KafkaConsumer] [init] ")
                .append("kafkaConnect(").append(kafkaConnect)
                .append(") groupId(").append(groupId)
                .append(") topic(").append(topic).append(")").toString());

        KafkaConfigBuilder.ConsumerConfiguration builder = KafkaConfigBuilder.defaultConsumer();

        final Properties props = builder.bootstrapServers(kafkaConnect)
                .group(groupId)
                .withKeyDeserializer(IntegerDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withOffsetCommitted("false")
                .build();

        consumer = new KafkaConsumer<>(props);
    }


    @Override
    public void run() {
        logger.info("[KafkaConsumer][{}][run] ", this.groupId + ":" + this.topic);
        this.consumer.subscribe(Arrays.asList(this.topic));
        while (true) {
            try {
                ConsumerRecords<Integer, byte[]> records = consumer.poll(100);
                if (records != null && records.count() > 0) {

                    if (records != null && logger.isDebugEnabled()) {
                        logger.debug("Binlog received: " + records.count() + " records");
                    }

                    for (ConsumerRecord<Integer, byte[]> record : records) {
                        logger.info("receive message,ready to process, topic: {} ,partition: {} ,offset: {}",
                                record.topic(), record.partition(), record.offset());

                        for (ConsumerEndpoint consumer : binlogConsumer) {
                            dealMessage(consumer, record.value());
                        }
                    }

                    try {
                        consumer.commitSync();
                    } catch (CommitFailedException e) {
                        logger.error("commit failed", e);
                    }
                }

            } catch (Exception e) {
                logger.error("[KafkaConsumer][{}][run] " + e.getMessage(), groupId + ":" + topic, e);
            }
        }
    }

    private void dealMessage(ConsumerEndpoint consumer, byte[] value) {
        List<BinlogEvent> binlogEvents = BinlogMsgProcessor.process(value);
        // > 0 才处理
        if (binlogEvents.size() > 0) {

            try {
                consumer.getMethod().invoke(consumer.getBean(), binlogEvents);
            } catch (IllegalAccessException e) {
                logger.error("实例化@BinlogListener 注解的方法 出错", e);
            } catch (InvocationTargetException e) {
                Throwable ex = e.getTargetException();
                logger.error("消息处理失败，消费者抛出异常 " + ex.getMessage(), ex);
            }

            logger.info("invoke message end ,bean: {}, method: {}", consumer.getBean(), consumer.getMethod());
        }

    }

    /**
     * 添加相同的 group + topic
     *
     * @param endpoint
     */
    public void addConsumer(ConsumerEndpoint endpoint) {
        binlogConsumer.add(endpoint);
    }
}
