package com.today.eventbus.common;

import com.today.eventbus.config.ResumeConfig;
import com.today.eventbus.utils.CommonUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * MsgConsumer Re-balance Listener
 *
 * @author hz.lei
 * @since 2018年07月25日 上午11:48
 */
public class MsgConsumerRebalanceListener implements ConsumerRebalanceListener {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final Consumer consumer;
    private final String groupId;
    private final String topic;

    private boolean firstAssigned = true;

    public MsgConsumerRebalanceListener(Consumer consumer, String groupId, String topic) {
        this.consumer = consumer;
        this.groupId = groupId;
        this.topic = topic;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("[RebalanceListener-Revoked]:reblance revoked 触发, partition被收回");
        partitions.forEach(p -> {
            long position = consumer.position(p);
            logger.info("拉取偏移量信息: partition:{}, next offset position:{}", p, position);
            OffsetAndMetadata committed = consumer.committed(p);
            logger.info("提交偏移量信息: {}", committed);
        });
        consumer.commitSync();
    }


    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("[RebalanceListener-Assigned]:reblance assigned 触发, partition重新分配");
        if (firstAssigned) {
            logger.info("第一次分区重平衡分配分区,首先进行回溯配置检查，如果存在即回溯..");
            Map<String, ResumeConfig> resumeConfigMap = CommonUtil.loadMsgBackConfig();
            logger.info("回溯配置信息:ResumeConfig {}", resumeConfigMap);

            //如果回溯配置不为空
            if (!resumeConfigMap.isEmpty()) {
                logger.info("当前消费者 groupId:{},topic:{},过滤后的配置信息: {}", this.groupId, this.topic, resumeConfigMap.toString());
                partitions.forEach(partition -> {
                    String resumeKey = CommonUtil.combineResumeKey(groupId, topic, partition.partition());
                    ResumeConfig resumeConfig = resumeConfigMap.get(resumeKey);

                    if (resumeConfig != null) {
                        logger.info("消费者组:{},topic:{},分区:{} 存在回溯配置，回溯 offset:{}",
                                this.groupId, this.topic, partition.partition(), resumeConfig.offset());
                        consumer.seek(partition, resumeConfig.offset());
                    } else {
                        assignedPartitionNormal(partition);
                    }
                });
            }
            firstAssigned = false;
        }
        partitions.forEach(this::assignedPartitionNormal);
    }

    /**
     * 没有回溯配置文件的情况下,采用 默认的配置
     */
    private void assignedPartitionNormal(TopicPartition partition) {
        //获取消费偏移量，实现原理是向协调者发送获取请求
        OffsetAndMetadata offset = consumer.committed(partition);
        logger.info("onPartitionsAssigned: partition:{}, offset:{}", partition, offset);
        if (offset == null) {
            logger.info("assigned offset is null ,do nothing for it !");
        } else {
            //设置本地拉取分量，下次拉取消息以这个偏移量为准
            consumer.seek(partition, offset.offset());
        }
    }
}

