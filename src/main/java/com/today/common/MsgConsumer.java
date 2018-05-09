package com.today.common;

import com.github.dapeng.core.SoaException;
import com.github.dapeng.org.apache.thrift.TException;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 描述: 重构，所有consumer继承的父类
 *
 * @author hz.lei
 * @date 2018年05月07日 下午3:28
 */
public abstract class MsgConsumer<KEY, VALUE, ENDPOINT> extends Thread {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected List<ENDPOINT> bizConsumers = new ArrayList<>();

    protected String groupId;

    protected String topic;

    protected String kafkaConnect;

    protected KafkaConsumer<KEY, VALUE> consumer;

    public MsgConsumer(String kafkaHost, String groupId, String topic) {
        this.kafkaConnect = kafkaHost;
        this.groupId = groupId;
        this.topic = topic;
        init();
    }

    /**
     * 添加相同的 group + topic  消费者
     *
     * @param endpoint
     */
    public void addConsumer(ENDPOINT endpoint) {
        this.bizConsumers.add(endpoint);

    }

    @Override
    public void run() {
        logger.info("[" + getClass().getSimpleName() + "][ {} ][run] ", this.groupId + ":" + this.topic);
        this.consumer.subscribe(Arrays.asList(this.topic));
        while (true) {
            try {
                ConsumerRecords<KEY, VALUE> records = consumer.poll(100);
                if (records != null && records.count() > 0) {
                    if (records != null && logger.isDebugEnabled()) {
                        logger.info("[" + getClass().getSimpleName() + "] while poll received: " + records.count() + " records");
                    }
                    for (ConsumerRecord<KEY, VALUE> record : records) {
                        logger.info("[" + getClass().getSimpleName() + "] record receive message to process, topic: {} ,partition: {} ,offset: {}",
                                record.topic(), record.partition(), record.offset());
                        try {
                            for (ENDPOINT bizConsumer : bizConsumers) {
                                dealMessage(bizConsumer, record.value());
                            }
                        } catch (Exception e) {
                            long offset = record.offset();
                            logger.error("[" + getClass().getSimpleName() + "]<->[dealMessage error]: " + e.getMessage());
                            logger.error("[" + getClass().getSimpleName() + "]<->[Retry]: 订阅者偏移量:[{}] 处理消息失败，进行重试 ", offset);

                            int partition = record.partition();
                            String topic = record.topic();
                            TopicPartition topicPartition = new TopicPartition(topic, partition);

                            /**
                             * 将offset seek到当前失败的消息位置，前面已经消费的消息的偏移量相当于已经提交了
                             * 因为这里seek到偏移量是最新的报错的offset。手动管理偏移量
                             */
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
     * 1. 反射调用的 目标类如果抛出异常 ，将被包装为 InvocationTargetException e
     * 2. 通过  InvocationTargetException.getTargetException 可以得到目标具体抛出的异常
     * 3. 如果目标类是通过aop代理的类,此时获得的异常会是 UndeclaredThrowableException
     * 4.如果目标类不是代理类，获得异常将直接为原始目标方法抛出的异常
     * <p>
     * 因此,需要判断目标异常如果为UndeclaredThrowableException，需要再次 getCause 拿到原始异常
     */
    protected void throwEx(InvocationTargetException e, String methodName) throws SoaException {
        Throwable target = e.getTargetException();

        if (target instanceof UndeclaredThrowableException) {
            target = target.getCause();
        }
        logger.error("[" + getClass().getSimpleName() + "]<->[TargetException]:" + target.getClass(), target.getMessage());

        if (target instanceof SoaException) {
            logger.error("[" + getClass().getSimpleName() + "]<->[订阅者处理消息失败,不会重试] throws SoaException: " + target.getMessage(), target);
            return;
        }
        throw new SoaException("[订阅者处理消息失败,会重试] throws: " + target.getMessage(), methodName);
    }

    private void dealRetryEx(ConsumerRecord<KEY, VALUE> record, Exception e) {
        long offset = record.offset();
        logger.error("[" + getClass().getSimpleName() + "]<->[dealMessage error]: " + e.getMessage());
        logger.error("[" + getClass().getSimpleName() + "]<->[Retry]: 订阅者偏移量:[{}] 处理消息失败，进行重试 ", offset);

        int partition = record.partition();
        String topic = record.topic();
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        /**
         * 将offset seek到当前失败的消息位置，前面已经消费的消息的偏移量相当于已经提交了
         * 因为这里seek到偏移量是最新的报错的offset。手动管理偏移量
         */
        consumer.seek(topicPartition, offset);
    }


    // template method

    /**
     * 消息具体处理逻辑
     *
     * @param bizConsumer 多个业务消费者遍历执行
     * @param value
     * @throws SoaException
     */
    protected abstract void dealMessage(ENDPOINT bizConsumer, VALUE value) throws TException;

    /**
     * 初始化 consumer
     */
    protected abstract void init();


}
