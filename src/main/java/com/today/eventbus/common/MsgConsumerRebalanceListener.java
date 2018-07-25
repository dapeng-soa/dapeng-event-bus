package com.today.eventbus.common;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * desc: MsgConsumerRebalanceListener
 *
 * @author hz.lei
 * @since 2018年07月25日 上午11:48
 */
public class MsgConsumerRebalanceListener implements ConsumerRebalanceListener {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Consumer consumer;

    public MsgConsumerRebalanceListener(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("[RebalanceListener-Revoked]:reblance触发, partition被收回");
        partitions.forEach(p -> {
            long position = consumer.position(p);
            logger.info("partition:{}, next offset:{} ", p, position);
            OffsetAndMetadata committed = consumer.committed(p);
            logger.info("OffsetAndMetadata: {}", committed);
        });
        consumer.commitSync();
    }


    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("[RebalanceListener-Assigned]:reblance 触发, partition重新分配");
        partitions.forEach(partition -> {
            //获取消费偏移量，实现原理是向协调者发送获取请求
            OffsetAndMetadata offset = consumer.committed(partition);
            logger.info("onPartitionsAssigned: partition:{}, offset:{}", partition, offset);
            if (offset == null) {
                logger.info("assigned offset is null ,do nothing for it !");
            } else {
                //设置本地拉取分量，下次拉取消息以这个偏移量为准
                consumer.seek(partition, offset.offset());
            }
        });
    }
}

