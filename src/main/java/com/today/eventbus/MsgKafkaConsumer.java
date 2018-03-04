package com.today.eventbus;

import com.github.dapeng.org.apache.thrift.TException;
import com.github.dapeng.util.SoaSystemEnvProperties;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 描述: msg 消息 kafkaConsumer
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:38
 */
public class MsgKafkaConsumer extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MsgKafkaConsumer.class);
    private List<ConsumerEndpoint> bizConsumers = new ArrayList();
    private String groupId;
    private String topic;
    private String kafkaConnect;
    protected KafkaConsumer<Long, byte[]> consumer;

    /**
     * host1:port1,host2:port2,...
     *
     * @param groupId
     * @param topic
     */
    public MsgKafkaConsumer(String kafkaHost, String groupId, String topic) {
        this.kafkaConnect = kafkaHost;
        this.groupId = groupId;
        this.topic = topic;
        this.init();
    }

    public void init() {
        logger.info(new StringBuffer("[KafkaConsumer] [init] ")
                .append("kafkaConnect(").append(kafkaConnect)
                .append(") groupId(").append(groupId)
                .append(") topic(").append(topic).append(")").toString());

        KafkaConfigBuilder.ConsumerConfiguration builder = KafkaConfigBuilder.defaultConsumer();

        final Properties props = builder.bootstrapServers(kafkaConnect)
                .group(groupId)
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withOffsetCommitted("false")
                .withIsolation("read_committed")
                .build();

        consumer = new KafkaConsumer<>(props);
    }

    public void addCustomer(ConsumerEndpoint client) {
        this.bizConsumers.add(client);
    }


    @Override
    public void run() {
        logger.info("[KafkaConsumer][{}][run] ", this.groupId + ":" + this.topic);
        this.consumer.subscribe(Arrays.asList(this.topic));
        while (true) {
            try {
                ConsumerRecords<Long, byte[]> records = consumer.poll(100);
                for (ConsumerRecord<Long, byte[]> record : records) {
                    logger.info("receive message,ready to process, topic: {} ,partition: {} ,offset: {}",
                            record.topic(), record.partition(), record.offset());

                    for (ConsumerEndpoint consumer : bizConsumers) {
                        dealMessage(consumer, record.value());
                    }
                }

                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    logger.error("commit failed", e);
                }
            } catch (Exception e) {
                logger.error("[KafkaConsumer][{}][run] " + e.getMessage(), groupId + ":" + topic, e);
            }
        }
    }

    /**
     * process message
     *
     * @param consumer
     * @param message
     */
    protected void dealMessage(ConsumerEndpoint consumer, byte[] message) {
        logger.info("Iterator and process biz message groupId: {}, topic: {}", groupId, topic);

        KafkaMessageProcessor processor = new KafkaMessageProcessor();
        String eventType = processor.getEventType(message);

        List<Class<?>> parameterTypes = consumer.getParameterTypes();

        long count = parameterTypes.stream()
                .filter(param -> param.getName().equals(eventType))
                .count();

        if (count > 0) {
            byte[] eventBinary = processor.getEventBinary();

            Object eventIface = null;
            try {
                eventIface = processor.decodeMessage(eventBinary, consumer.getSerializerType());
            } catch (TException e) {
                e.printStackTrace();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                logger.error("实例化beanSerializer出错，可能定义BeanSerializer 全限定名配置写错");
            }

            try {
                consumer.getMethod().invoke(consumer.getBean(), eventIface);
                logger.info("invoke message end ,bean: {}, method: {}", consumer.getBean(), consumer.getMethod());
            } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                logger.error("参数不合法，当前方法虽然订阅此topic，但是不接收当前事件", e);
            }

        } else {
            logger.info("方法 [ {} ] 不接收当前收到的消息类型 {} ", consumer.getMethod(), eventType);
        }
    }
}
