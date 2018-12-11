package com.today.eventbus;

import com.github.dapeng.core.SoaException;
import com.github.dapeng.org.apache.thrift.TException;
import com.today.eventbus.common.ConsumerContext;
import com.today.eventbus.common.MsgConsumer;
import com.today.eventbus.utils.Constant;
import com.today.eventbus.common.retry.DefaultRetryStrategy;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaLongDeserializer;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;


/**
 * 描述: msg 消息 kafkaConsumer
 *
 * @author hz.lei
 * @since 2018年03月02日 上午1:38
 */
public class MsgKafkaConsumer extends MsgConsumer<Long, byte[], ConsumerEndpoint> {
    /**
     * @param kafkaHost     host1:port1,host2:port2,...
     * @param groupId       消费者组唯一标志
     * @param topic         消费者订阅的主题
     * @param timeout       kafka 消费者会话超时时间
     * @param maxAttempts   重试最大次数
     * @param retryInterval 重试策略的重试间隔
     */
    public MsgKafkaConsumer(String kafkaHost, String groupId, String topic, int timeout, int maxAttempts, int retryInterval) {
        super(kafkaHost, groupId, topic, timeout, maxAttempts, retryInterval);
    }

    @Override
    protected void init() {
        logger.info("[MsgKafkaConsumer] init. kafkaConnect({}), groupId({}), topic({}), timeout:({})",
                kafkaConnect, groupId, topic, timeout);

        KafkaConfigBuilder.ConsumerConfiguration builder = KafkaConfigBuilder.defaultConsumer();

        Properties props = builder.bootstrapServers(kafkaConnect)
                .group(groupId)
                .withKeyDeserializer(KafkaLongDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withOffsetCommitted(false)
                .excludeInternalTopic(false)
                .withIsolation(Constant.ISOLATION_LEVEL)
                .maxPollSize(Constant.MAX_POLL_SIZE)
                .build();

        //增加 session.timeout 的配置
        if (timeout > Constant.DEFAULT_SESSION_TIMEOUT) {
            int heartbeat = timeout / 3;
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, timeout);
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeat);
        }
        consumer = new KafkaConsumer<>(props);
    }

    @Override
    protected void buildRetryStrategy() {
        retryStrategy = new DefaultRetryStrategy(maxAttempts, retryInterval);
    }


    /**
     * process message
     */
    @Override
    protected void dealMessage(ConsumerEndpoint consumer, ConsumerRecord<Long, byte[]> record) throws SoaException {
        Long keyId = record.key();
        byte[] message = record.value();

        logger.debug("[{}]:[BEGIN] 开始处理订阅方法 dealMessage, method {}", getClass().getSimpleName(), consumer.getMethod().getName());

        KafkaMessageProcessor processor = new KafkaMessageProcessor();
        String eventType;
        try {
            eventType = processor.getEventType(message);
        } catch (Exception e) {
            logger.error("[" + getClass().getSimpleName() + "]<->[Parse Error]: 解析消息eventType出错，忽略该消息");
            return;
        }
        //logger
        logger.info("receive message,收到消息 topic:{}, 分区:{}, offset:{}, 收到类型:{}, 当前消费者处理类型:{}",
                record.topic(), record.partition(), record.offset(), eventType, consumer.getEventType());

        List<Class<?>> parameterTypes = consumer.getParameterTypes();

        long count = parameterTypes.stream()
                .filter(param -> param.getName().equals(eventType))
                .count();

        if (count > 0) {
            logger.info("[{}]<->[开始处理消息,消息KEY:{}]: method {}, groupId: {}, topic: {}, bean: {}",
                    keyId, getClass().getSimpleName(), consumer.getMethod().getName(), groupId, topic, consumer.getBean());

            byte[] eventBinary = processor.getEventBinary();

            try {
                Object event = processor.decodeMessage(eventBinary, consumer.getEventSerializer());

                //判断method参数数量...
                if (consumer.getHasConsumerMetaData()) {
                    ConsumerContext context = buildConsumerContext(record, eventType);
                    consumer.getMethod().invoke(consumer.getBean(), context, event);
                    logger.info("KafkaConsumer[结束处理消息]:method {}, groupId: {},context元信息:{}",
                            consumer.getMethod().getName(), groupId, context.toString());
                } else {
                    consumer.getMethod().invoke(consumer.getBean(), event);
                    logger.info("KafkaConsumer[结束处理消息]: method {}, groupId: {}, topic: {}, bean: {}",
                            consumer.getMethod().getName(), groupId, topic, consumer.getBean());
                }
            } catch (IllegalAccessException | IllegalArgumentException e) {
                logger.error("[" + getClass().getSimpleName() + "]<->参数不合法，当前方法虽然订阅此topic，但是不接收当前事件:" + eventType, e);
            } catch (InvocationTargetException e) {
                // 包装异常处理
                throwRealException(e, consumer.getMethod().getName());
            } catch (TException e) {
                logger.error("[" + getClass().getSimpleName() + "]<->[反序列化事件 {" + eventType + "} 出错]: " + e.getMessage(), e);
            } catch (InstantiationException e) {
                logger.error("[" + getClass().getSimpleName() + "]<->[实例化事件 {" + eventType + "} 对应的编解码器失败]:" + e.getMessage(), e);
            }
        } else {
            logger.debug("[{}]<-> 方法 [ {} ] 不接收当前收到的消息类型 {} ", getClass().getSimpleName(), consumer.getMethod().getName(), eventType);
        }

        logger.debug("[{}]:[END] 结束处理订阅方法 dealMessage, method {}", getClass().getSimpleName(), consumer.getMethod().getName());
    }

    /**
     * 构造消费者上下文元信息...
     */
    private ConsumerContext buildConsumerContext(ConsumerRecord<Long, byte[]> record, String eventType) {
        // if null
        String timeType = record.timestampType() == null ? null : record.timestampType().name;

        return new ConsumerContext(record.key(), record.topic(), record.offset(), record.partition(),
                record.timestamp(), timeType, eventType);
    }
}
