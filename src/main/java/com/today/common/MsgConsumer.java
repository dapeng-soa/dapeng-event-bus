package com.today.common;

import com.today.binlog.BinlogKafkaConsumer;
import com.today.eventbus.ConsumerEndpoint;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 描述: com.today.common
 *
 * @author hz.lei
 * @date 2018年05月07日 下午3:28
 */
public abstract class MsgConsumer<KEY, VALUE> {
    private static final Logger logger = LoggerFactory.getLogger(MsgConsumer.class);
    private List<ConsumerEndpoint> bizConsumers = new ArrayList<>();
    private String groupId;
    private String topic;
    private String kafkaConnect;
    protected KafkaConsumer<KEY, VALUE> consumer;

    public MsgConsumer(String kafkaHost, String groupId, String topic) {
        this.kafkaConnect = kafkaHost;
        this.groupId = groupId;
        this.topic = topic;
        this.init();
    }

    /**
     * 添加相同的 group + topic  消费者
     *
     * @param endpoint
     */
    public void addConsumer(ConsumerEndpoint endpoint) {
        this.bizConsumers.add(endpoint);

    }







    protected abstract void init();

}
