package com.today.eventbus.rest.support;

import org.simpleframework.xml.core.Persister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:29
 */
public class RestListenerFactory implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(RestListenerFactory.class);

    public static final Map<String, RestKafkaConsumer> REST_CONSUMERS = new HashMap<>();


    public void registerEndpoint() {
        Persister persister = new Persister();
        RestConsumerConfig config = null;
        File file;
        FileInputStream inputStream = null;
        try {
            //==images==//
            inputStream = new FileInputStream("conf/rest-consumer.xml");
            config = persister.read(RestConsumerConfig.class, inputStream);
        }catch (FileNotFoundException e){
            logger.warn("read file system NotFound [conf/rest-consumer.xml],found conf file [rest-consumer.xml] on classpath");
            try {
                //==develop==//
                file = ResourceUtils.getFile("classpath:rest-consumer.xml");
                config = persister.read(RestConsumerConfig.class, file);
            } catch (FileNotFoundException e1) {
                throw new RuntimeException("rest-consumer.xml in classpath and conf/ NotFound, please Settings");
            } catch (Exception e1) {
                logger.error(e1.getMessage(), e1);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }finally {
            if (null!=inputStream){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        Assert.notNull(config, "Endpoint must be set");

        transferEl(config);

        config.getRestConsumerEndpoints().forEach(endpoint -> addConsumer(endpoint));
    }


    public void addConsumer(RestConsumerEndpoint endpoint) {
        String groupId = endpoint.getGroupId();
        String topic = endpoint.getTopic();
        String kafkaHost = endpoint.getKafkaHost();
        try {
            // 默认 group id
            String consumerKey = groupId + ":" + topic;

            if (REST_CONSUMERS.containsKey(consumerKey)) {
                REST_CONSUMERS.get(consumerKey).addConsumer(endpoint);
            } else {
                RestKafkaConsumer consumer = new RestKafkaConsumer(kafkaHost, groupId, topic);
                consumer.addConsumer(endpoint);
                REST_CONSUMERS.put(consumerKey, consumer);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void afterPropertiesSet() {
        registerEndpoint();
        logger.info("[RestConsumer]: ready to start rest consumer ,rest event consumer size {}", REST_CONSUMERS.size());
        REST_CONSUMERS.values().forEach(Thread::start);
    }

    private void transferEl(RestConsumerConfig config) {
        config.getRestConsumerEndpoints().forEach(endpoint -> {
            String kafkaHostKey = endpoint.getKafkaHost();

            String kafkaHost = get(kafkaHostKey, null);
            logger.info("transfer env key, endpoint id: {}, kafkaHost: {}", endpoint.getId(), kafkaHost);

            if (kafkaHost != null) {
                endpoint.setKafkaHost(kafkaHost);
            } else {
                logger.error("kafka msgAgent endpoint id ["+endpoint.getId()+"] need env ["+kafkaHostKey+"] but NotFound");
                throw new NullPointerException("kafka msgAgent endpoint id ["+endpoint.getId()+"] need env ["+kafkaHostKey+"] but NotFound");
            }
        });


    }

    public static String get(String key, String defaultValue) {
        String envValue = System.getenv(key.replaceAll("\\.", "_"));

        if (envValue == null)
            return System.getProperty(key, defaultValue);

        return envValue;
    }

    public static void main(String[] args) {
        RestListenerFactory factory = new RestListenerFactory();
        factory.registerEndpoint();


    }

}
