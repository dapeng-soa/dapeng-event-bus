package com.today.eventbus;

import com.github.dapeng.core.SoaException;
import com.github.dapeng.org.apache.thrift.TException;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
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
                .withSessionTimeOut("100000")
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
                if (records != null && records.count() > 0) {

                    if (records != null && logger.isDebugEnabled()) {
                        logger.debug("Per poll received: " + records.count() + " records");
                    }

                    for (ConsumerRecord<Long, byte[]> record : records) {
                        logger.info("receive message,ready to process, topic: {} ,partition: {} ,offset: {}",
                                record.topic(), record.partition(), record.offset());
                        try {
                            for (ConsumerEndpoint bizConsumer : bizConsumers) {
                                dealMessage(bizConsumer, record.value());
                            }
                        } catch (Exception e) {
                            long offset = record.offset();
                            logger.error("[dealMessage error]: " + e.getMessage());
                            logger.error("[Retry]: 订阅者偏移量:[{}] 处理消息失败，进行重试 ", offset);

                            int partition = record.partition();
                            String topic = record.topic();
                            TopicPartition topicPartition = new TopicPartition(topic, partition);

                            //将offset seek到当前失败的消息位置，前面已经消费的消息的偏移量相当于已经提交了，因为这里seek到偏移量是最新的报错的offset。手动管理偏移量
                            consumer.seek(topicPartition, offset);
                            break;
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

    /**
     * process message
     *
     * @param consumer
     * @param message
     */
    protected void dealMessage(ConsumerEndpoint consumer, byte[] message) throws SoaException {
        logger.info("Iterator and process biz message groupId: {}, topic: {}", groupId, topic);

        KafkaMessageProcessor processor = new KafkaMessageProcessor();
        String eventType;
        try {
            eventType = processor.getEventType(message);
        } catch (Exception e) {
            logger.error("[Parse Error]: 解析消息eventType出错，忽略该消息");
            return;
        }

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
                // 包装异常处理
                throwEx(e, consumer.getMethod().getName());
            } catch (TException e) {
                logger.error("[反序列化事件 {" + eventType + "} 出错]: " + e.getMessage(), e);
            } catch (InstantiationException e) {
                logger.error("[实例化事件 {" + eventType + "} 对应的编解码器失败]:" + e.getMessage(), e);
            }
        } else {
            logger.debug("方法 [ {} ] 不接收当前收到的消息类型 {} ", consumer.getMethod(), eventType);
        }
    }


    /**
     * 1. 反射调用的 目标类如果抛出异常 ，将被包装为 InvocationTargetException e
     * 2. 通过  InvocationTargetException.getTargetException 可以得到目标具体抛出的异常
     * 3. 如果目标类是通过aop代理的类,此时获得的异常会是 UndeclaredThrowableException
     * 4.如果目标类不是代理类，获得异常将直接为原始目标方法抛出的异常
     * <p>
     * 因此,需要判断目标异常如果为UndeclaredThrowableException，需要再次 getCause 拿到原始异常
     */
    private void throwEx(InvocationTargetException e, String methodName) throws SoaException {
        Throwable target = e.getTargetException();

        if (target instanceof UndeclaredThrowableException) {
            target = target.getCause();
        }
        logger.error("[TargetException]:" + target.getClass(), target.getMessage());

        if (target instanceof SoaException) {
            logger.error("[订阅者处理消息失败,不会重试] throws SoaException: " + target.getMessage(), target);
            return;
        }
        throw new SoaException("[订阅者处理消息失败,会重试] throws: " + target.getMessage(), methodName);
    }
}
