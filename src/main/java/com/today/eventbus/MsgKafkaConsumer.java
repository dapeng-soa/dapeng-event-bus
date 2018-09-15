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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.MDC;

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
            InvocationContext invocationCtx = InvocationContextImpl.Factory.currentInstance();
            long sessionTid = DapengUtil.generateTid();
            invocationCtx.sessionTid(sessionTid);
            MDC.put(SoaSystemEnvProperties.KEY_LOGGER_SESSION_TID, DapengUtil.longToHexStr(sessionTid));
            logger.info("[{}]<->[开始处理消息,消息KEY(唯一ID):{}]: method {}, groupId: {}, topic: {}, bean: {}",
                    keyId, getClass().getSimpleName(), consumer.getMethod().getName(), groupId, topic, consumer.getBean());

            byte[] eventBinary = processor.getEventBinary();

            try {
                Object event = processor.decodeMessage(eventBinary, consumer.getEventSerializer());
                consumer.getMethod().invoke(consumer.getBean(), event);
                logger.info("[{}]<->[处理消息结束]: method {}, groupId: {}, topic: {}, bean: {}",
                        getClass().getSimpleName(), consumer.getMethod().getName(), groupId, topic, consumer.getBean());
            } catch (IllegalAccessException | IllegalArgumentException e) {
                logger.error("[" + getClass().getSimpleName() + "]<->参数不合法，当前方法虽然订阅此topic，但是不接收当前事件:" + eventType, e);
            } catch (InvocationTargetException e) {
                // 包装异常处理
                throwRealException(e, consumer.getMethod().getName());
            } catch (TException e) {
                logger.error("[" + getClass().getSimpleName() + "]<->[反序列化事件 {" + eventType + "} 出错]: " + e.getMessage(), e);
            } catch (InstantiationException e) {
                logger.error("[" + getClass().getSimpleName() + "]<->[实例化事件 {" + eventType + "} 对应的编解码器失败]:" + e.getMessage(), e);
            } finally {
                MDC.remove(SoaSystemEnvProperties.KEY_LOGGER_SESSION_TID);
                InvocationContextImpl.Factory.removeCurrentInstance();
            }
        } else {
            logger.debug("[{}]<-> 方法 [ {} ] 不接收当前收到的消息类型 {} ", getClass().getSimpleName(), consumer.getMethod().getName(), eventType);
        }

        logger.debug("[{}]:[END] 结束处理订阅方法 dealMessage, method {}", getClass().getSimpleName(), consumer.getMethod().getName());
    }
}
