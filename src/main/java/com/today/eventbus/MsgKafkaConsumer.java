package com.today.eventbus;

import com.github.dapeng.core.InvocationContext;
import com.github.dapeng.core.InvocationContextImpl;
import com.github.dapeng.core.SoaException;
import com.github.dapeng.core.helper.DapengUtil;
import com.github.dapeng.core.helper.SoaSystemEnvProperties;
import com.github.dapeng.org.apache.thrift.TException;
import com.today.eventbus.common.MsgConsumer;
import com.today.eventbus.utils.Constant;
import com.today.eventbus.common.retry.DefaultRetryStrategy;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaLongDeserializer;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.MDC;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


/**
 * 描述: msg 消息 kafkaConsumer
 *
 * @author hz.lei
 * @since 2018年03月02日 上午1:38
 */
public class MsgKafkaConsumer extends MsgConsumer<Long, byte[], ConsumerEndpoint> {

    private static String transactionId = "retryQueue" + UUID.randomUUID().toString();
    private static String retryTopic = System.getenv("serviceName") + "-retry-topic";

    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     * @param timeout
     */
    public MsgKafkaConsumer(String kafkaHost, String groupId, String topic, int timeout) {
        super(kafkaHost, groupId, topic, timeout);
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
        retryProducer = new MsgKafkaProducer(kafkaConnect, transactionId);
    }

    @Override
    protected void buildRetryStrategy() {
        retryStrategy = new DefaultRetryStrategy();
    }


    /**
     * process message
     */
    @Override
    protected void dealMessage(ConsumerEndpoint consumer, byte[] message, Long keyId) throws SoaException {
        logger.debug("[{}]:[BEGIN] 开始处理订阅方法 dealMessage, method {}", getClass().getSimpleName(), consumer.getMethod().getName());

        KafkaMessageProcessor processor = new KafkaMessageProcessor();
        String eventType;
        try {
            eventType = processor.getEventType(message);
        } catch (Exception e) {
            logger.error("[" + getClass().getSimpleName() + "]<->[Parse Error]: 解析消息eventType出错，忽略该消息");
            return;
        }

        List<Class<?>> parameterTypes = consumer.getParameterTypes();

        long count = parameterTypes.stream()
                .filter(param -> param.getName().equals(eventType))
                .count();

        if (count > 0) {
            logger.info("[{}]<->[开始处理消息,消息KEY(唯一ID):{}]: method {}, groupId: {}, topic: {}, bean: {}",
                    keyId, getClass().getSimpleName(), consumer.getMethod().getName(), groupId, topic, consumer.getBean());

            byte[] eventBinary = processor.getEventBinary();

            try {
                Object event = processor.decodeMessage(eventBinary, consumer.getEventSerializer());
                consumer.getMethod().invoke(consumer.getBean(), event);
                logger.info("[{}]<->[处理消息结束]: method {}, groupId: {}, topic: {}, bean: {}",
                        getClass().getSimpleName(), consumer.getMethod().getName(), groupId, topic, consumer.getBean());
            } catch (Exception e) {
                if (e instanceof InvocationTargetException) {
                    // 包装异常处理
                    throwRealException((InvocationTargetException) e, consumer.getMethod().getName());
                } else {
                    if (e instanceof IllegalAccessException || e instanceof IllegalArgumentException) {
                        logger.error("[" + getClass().getSimpleName() + "]<->参数不合法，当前方法虽然订阅此topic，但是不接收当前事件:" + eventType, e);
                    } else if (e instanceof TException) {
                        logger.error("[" + getClass().getSimpleName() + "]<->[反序列化事件 {" + eventType + "} 出错]: " + e.getMessage(), e);
                    } else if (e instanceof InstantiationException) {
                        logger.error("[" + getClass().getSimpleName() + "]<->[实例化事件 {" + eventType + "} 对应的编解码器失败]:" + e.getMessage(), e);
                    }
                    sendToRetryTopic(keyId, message);
                }


            }
        } else {
            logger.debug("[{}]<-> 方法 [ {} ] 不接收当前收到的消息类型 {} ", getClass().getSimpleName(), consumer.getMethod().getName(), eventType);
        }

        logger.debug("[{}]:[END] 结束处理订阅方法 dealMessage, method {}", getClass().getSimpleName(), consumer.getMethod().getName());
    }

    @Override
    protected void sendToRetryTopic(Long key, byte[] value) {
        logger.info("[" + getClass().getSimpleName() + "] 消息处理失败，消息被发送到重试topic，等待被重新消费");
        retryProducer.send(retryTopic, key, value);
    }
}