package com.today.eventbus.agent.support;

import com.today.eventbus.agent.support.parse.AgentConsumerXml;
import com.today.eventbus.agent.support.parse.BizConsumer;
import com.today.eventbus.agent.support.parse.ConsumerGroup;
import com.today.eventbus.agent.support.parse.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 描述: RestListenerFactory
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:29
 */
public class RestListenerFactory implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(RestListenerFactory.class);

    private static final Map<String, RestKafkaConsumer> REST_CONSUMERS = new HashMap<>();


    @Override
    public void afterPropertiesSet() {
        registerConsumerInstance();
        logger.info("\n<<<<<<<<<<<<< [RestConsumer]:开始启动consumer，实例数为: {} >>>>>>>>>>>>>>>> \n", REST_CONSUMERS.size());
        StringBuilder logBuffer = new StringBuilder();
        logBuffer.append("<<<<<<<<<<<<<<<<<<<<<<-消费者组信息展示开始>>>>>>>>>>>>>>>>>>>>>>>>\n");
        REST_CONSUMERS.keySet().stream().sorted().forEach(k -> {

            logBuffer.append("消费者组: 组名:[" + k.substring(0, k.indexOf("-")) + "], " +
                    "实例名:[" + k.substring(k.indexOf("-") + 1) + "]\n");

            REST_CONSUMERS.get(k).getBizConsumers().forEach(biz -> {
                logBuffer.append("bizConsumer: 事件类型:[" + biz.getEvent() + "], 事件转发url:[" + biz.getDestinationUrl() + "] \n");
            });
            logBuffer.append("\n");
        });
        logBuffer.append("<<<<<<<<<<<<<<<<<<<<<<-消费者组信息展示完毕->>>>>>>>>>>>>>>>>>>>>>>>\n");
        logger.info(logBuffer.toString());
        //启动实例
        ExecutorService executorService = Executors.newFixedThreadPool(REST_CONSUMERS.size());

        REST_CONSUMERS.values().forEach(consumer -> executorService.execute(consumer));
    }

    /**
     * 注册消费者实例，一个实例即为一个线程
     * 一个实例下面可以有多个 bizConsumer,进行过滤消费
     */
    private void registerConsumerInstance() {
        AgentConsumerXml agentConfig = ParserUtil.getConsumerConfig();
        List<ConsumerGroup> consumerGroups = agentConfig.getConsumerGroups();

        consumerGroups.forEach(group -> {
            Integer threadCount = group.getThreadCount();
            for (int i = 0; i < threadCount; i++) {
                RestKafkaConsumer consumerInstance = new RestKafkaConsumer(group.getId(), group.getKafkaHost(), group.getGroupId(), group.getTopic());
                group.getConsumers().getConsumers().forEach(consumer -> {
                    consumer.setGroupId(group.getGroupId());
                    consumer.setService(group.getService());
                    consumer.setVersion(group.getVersion());
                    addConsumer(consumerInstance, consumer);
                });
                REST_CONSUMERS.put(consumerInstance.getInstName() + "-instance-" + i, consumerInstance);
            }
        });
    }

    private void addConsumer(RestKafkaConsumer instance, BizConsumer consumer) {
        instance.addConsumer(consumer);
    }
}
