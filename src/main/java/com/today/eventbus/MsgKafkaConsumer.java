package com.today.eventbus;

import com.github.dapeng.core.SoaException;
import com.github.dapeng.org.apache.thrift.TException;
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
    private List<ConsumerEndpoint> bizConsumers = new ArrayList<>();
    private String groupId;
    private String topic;
    private String kafkaConnect;
    protected KafkaConsumer<Long, byte[]> consumer;

    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     */
    public MsgKafkaConsumer(String kafkaHost, String groupId, String topic) {
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
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withOffsetCommitted("false")
                .withIsolation("read_committed")
                .withSessionTimeOut("30000")
                .build();

        consumer = new KafkaConsumer<>(props);
    }

    /**
     * 添加相同的 group + topic  消费者
     *
     * @param endpoint
     */
    public void addConsumer(ConsumerEndpoint endpoint) {
        this.bizConsumers.add(endpoint);

    }

    @Override
    public void run() {
        logger.info("[KafkaConsumer][{}][run] ", this.groupId + ":" + this.topic);
        this.consumer.subscribe(Arrays.asList(this.topic));
        while (true) {
            try {
                ConsumerRecords<Long, byte[]> records = consumer.poll(100);

                if (records != null && logger.isDebugEnabled()) {
                    logger.debug("Received: " + records.count() + " records");
                }


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
    protected void dealMessage(ConsumerEndpoint consumer, byte[] message) throws SoaException {
        logger.info("Iterator and process biz message groupId: {}, topic: {}", groupId, topic);

        KafkaMessageProcessor processor = new KafkaMessageProcessor();
        String eventType = processor.getEventType(message);

        List<Class<?>> parameterTypes = consumer.getParameterTypes();

        long count = parameterTypes.stream()
                .filter(param -> param.getName().equals(eventType))
                .count();

        if (count > 0) {
            byte[] eventBinary = processor.getEventBinary();

            try {
                Object event = processor.decodeMessage(eventBinary, consumer.getEventSerializer());
                consumer.getMethod().invoke(consumer.getBean(), event);
                logger.info("invoke message end ,bean: {}, method: {}", consumer.getBean(), consumer.getMethod());
            } catch (IllegalAccessException | IllegalArgumentException e) {
                logger.error("参数不合法，当前方法虽然订阅此topic，但是不接收当前事件:" + eventType, e);

            } catch (InvocationTargetException e) {
                Throwable ex = e.getTargetException();
                logger.error("消息处理失败，消费者抛出异常 " + ex.getMessage(), ex);
                throw new SoaException("订阅消息处理失败 ", consumer.getMethod().getName());
            } catch (TException e) {
                logger.error(e.getMessage(), e);
                logger.error("反序列化事件" + eventType + "出错");
            } catch (InstantiationException e) {
                logger.error(e.getMessage(), e);
                logger.error("实例化事件" + eventType + "对应的编解码器失败");
            }
        } else {
            logger.debug("方法 [ {} ] 不接收当前收到的消息类型 {} ", consumer.getMethod(), eventType);
        }
    }


}
