package com.today.eventbus.agent.support.parse;

import org.simpleframework.xml.core.Persister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Desc: ParserUtil 解析 xml data
 *
 * @author hz.lei
 * @since 2018年05月17日 下午12:08
 */
public class ParserUtil {
    private static Logger logger = LoggerFactory.getLogger(ParserUtil.class);

    private static AgentConsumerXml consumerConfig = parserXmlData();

    public static AgentConsumerXml getConsumerConfig() {
        return consumerConfig;
    }

    /**
     * 获取配置的consumer信息 key consumer 对应的service
     *
     * @return
     */
    public static Map<String, ConsumerGroup> getConsumersMap() {
        Map<String, ConsumerGroup> consumersMap = new HashMap(16);
        consumerConfig.getConsumerGroups().forEach(consumer -> consumersMap.put(consumer.getService(), consumer));
        return consumersMap;
    }

    /**
     * 获取 event service 列表，并且去重
     *
     * @return
     */
    public static Set<String> getConsumerServiceSet() {
        Set<String> serviceList = new HashSet<>(16);
        consumerConfig.getConsumerGroups().forEach(consumer -> serviceList.add(consumer.getService()));
        return serviceList;
    }

    private static AgentConsumerXml parserXmlData() {
        Persister persister = new Persister();
        AgentConsumerXml config = null;
        File file;
        FileInputStream inputStream = null;
        try {
            //==images==//
            inputStream = new FileInputStream("conf/rest-consumer.xml");
            config = persister.read(AgentConsumerXml.class, inputStream);
        } catch (FileNotFoundException e) {
            logger.warn("read file system NotFound [conf/rest-consumer.xml],found conf file [rest-consumer.xml] on classpath");
            try {
                //==develop==//
                file = ResourceUtils.getFile("classpath:rest-consumer.xml");
                config = persister.read(AgentConsumerXml.class, file);
            } catch (FileNotFoundException e1) {
                throw new RuntimeException("rest-consumer.xml in classpath and conf/ NotFound, please Settings");
            } catch (Exception e1) {
                logger.error(e1.getMessage(), e1);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        Assert.notNull(config, "Endpoint must be set");
        //转换消息
        transferEl(config);
        logger.info("解析xml信息: " + config.toString());
        return config;
    }

    private static void transferEl(AgentConsumerXml config) {
        config.getConsumerGroups().forEach(group -> {
            String kafkaHostKey = group.getKafkaHost();

            String kafkaHost = get(kafkaHostKey, null);
            logger.info("transfer env key, endpoint id: {}, kafkaHost: {}", group.getId(), kafkaHost);

            if (kafkaHost != null) {
                group.setKafkaHost(kafkaHost);
                logger.info("转换kafka环境变量key {} ,转换后值: {}", kafkaHostKey, kafkaHost);
            } else {
                logger.error("kafka 消息代理 消费者组 id [" + group.getId() + "] 需要环境变量 env [" + kafkaHostKey + "] but NotFound");
                throw new NullPointerException("kafka msgAgent endpoint id [" + group.getId() + "] need env [" + kafkaHostKey + "] but NotFound");
            }
        });

    }

    private static String get(String key, String defaultValue) {
        String envValue = System.getenv(key.replaceAll("\\.", "_"));

        if (envValue == null)
            return System.getProperty(key, defaultValue);

        return envValue;
    }


}
