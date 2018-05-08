package com.today.binlog;

import com.github.dapeng.core.SoaException;
import com.today.common.MsgConsumer;
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
public class BinlogKafkaConsumer extends MsgConsumer<Integer, byte[], ConsumerEndpoint> {


    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     */
    public BinlogKafkaConsumer(String kafkaHost, String groupId, String topic) {
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
                .withKeyDeserializer(IntegerDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withOffsetCommitted("false")
                .build();

        consumer = new KafkaConsumer<>(props);
    }


    @Override
    protected void dealMessage(ConsumerEndpoint consumer, byte[] value) throws SoaException {
        List<BinlogEvent> binlogEvents = BinlogMsgProcessor.process(value);
        // > 0 才处理
        if (binlogEvents.size() > 0) {
            try {
                consumer.getMethod().invoke(consumer.getBean(), binlogEvents);
            } catch (IllegalAccessException e) {
                logger.error("[" + getClass().getSimpleName() + "] <-> 实例化@BinlogListener 注解的方法 出错", e);
            } catch (InvocationTargetException e) {
                throwEx(e, consumer.getMethod().getName());
            }
            logger.info("[{}]<->[dealMessage] end, method: {}, groupId: {}, topic: {}, bean: {}",
                    consumer.getMethod().getName(), groupId, topic, consumer.getBean());

        }

    }
}
