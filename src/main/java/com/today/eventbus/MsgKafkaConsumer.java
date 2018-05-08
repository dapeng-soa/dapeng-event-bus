package com.today.eventbus;

import com.github.dapeng.core.SoaException;
import com.github.dapeng.org.apache.thrift.TException;
import com.today.common.MsgConsumer;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

/**
 * 描述: msg 消息 kafkaConsumer
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:38
 */
public class MsgKafkaConsumer extends MsgConsumer<Long, byte[]> {

    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     */
    public MsgKafkaConsumer(String kafkaHost, String groupId, String topic) {
        super(kafkaHost, groupId, topic);
    }

    @Override
    protected void init() {
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
     * process message
     */
    @Override
    protected void dealMessage(ConsumerEndpoint consumer, byte[] message) throws SoaException {
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
            logger.info("[{}]<->[dealMessage] begin <-> method {}, groupId: {}, topic: {}, bean: {}",
                    getClass().getSimpleName(), consumer.getMethod().getName(), groupId, topic, consumer.getBean());

            byte[] eventBinary = processor.getEventBinary();

            try {
                Object event = processor.decodeMessage(eventBinary, consumer.getEventSerializer());
                consumer.getMethod().invoke(consumer.getBean(), event);
                logger.info("[{}]<->[dealMessage] end <-> method {}, groupId: {}, topic: {}, bean: {}",
                        getClass().getSimpleName(), consumer.getMethod().getName(), groupId, topic, consumer.getBean());

            } catch (IllegalAccessException | IllegalArgumentException e) {
                logger.error("[" + getClass().getSimpleName() + "]<->参数不合法，当前方法虽然订阅此topic，但是不接收当前事件:" + eventType, e);
            } catch (InvocationTargetException e) {
                // 包装异常处理
                throwEx(e, consumer.getMethod().getName());
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
}
