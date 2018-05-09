package com.today.eventbus.rest.support;

import com.github.dapeng.core.metadata.Service;
import com.github.dapeng.json.JsonSerializer;
import com.github.dapeng.openapi.cache.ServiceCache;
import com.github.dapeng.org.apache.thrift.TException;
import com.github.dapeng.org.apache.thrift.protocol.TCompactProtocol;
import com.github.dapeng.util.MetaDataUtil;
import com.github.dapeng.util.TCommonTransport;
import com.github.dapeng.util.TKafkaTransport;
import com.today.eventbus.MsgKafkaConsumer;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 描述: com.today.eventbus.rest.support
 *
 * @author hz.lei
 * @date 2018年05月02日 下午4:39
 */
public class RestKafkaConsumer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(MsgKafkaConsumer.class);
    private List<RestConsumerEndpoint> bizConsumers = new ArrayList<>();
    private String groupId;
    private String topic;
    private String kafkaConnect;
    protected KafkaConsumer<Long, byte[]> consumer;

    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     */
    public RestKafkaConsumer(String kafkaHost, String groupId, String topic) {
        this.kafkaConnect = kafkaHost;
        this.groupId = groupId;
        this.topic = topic;
        this.init();
    }

    private void init() {
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
     * 添加相同的 group + topic  消费者
     *
     * @param endpoint
     */
    public void addConsumer(RestConsumerEndpoint endpoint) {
        this.bizConsumers.add(endpoint);

    }

    @Override
    public void run() {
        logger.info("[KafkaConsumer][{}][run] ", this.groupId + ":" + this.topic);
        this.consumer.subscribe(Arrays.asList(this.topic));
        while (true) {
            try {
                ConsumerRecords<Long, byte[]> records = consumer.poll(100);
                if (records != null && records.count() > 0) {

                    if (records != null && logger.isDebugEnabled()) {
                        logger.debug("Per poll received: " + records.count() + " records");
                    }

                    for (ConsumerRecord<Long, byte[]> record : records) {
                        logger.info("receive message,ready to process, topic: {} ,partition: {} ,offset: {}",
                                record.topic(), record.partition(), record.offset());
                        try {
                            for (RestConsumerEndpoint bizConsumer : bizConsumers) {
                                dealMessage(bizConsumer, record.value());
                            }
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            long offset = record.offset();
                            logger.error("当前偏移量 {} 处理消息失败，进行重试 ", offset);

                            int partition = record.partition();
                            String topic = record.topic();
                            TopicPartition topicPartition = new TopicPartition(topic, partition);

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

    private void dealMessage(RestConsumerEndpoint bizConsumer, byte[] value) throws TException {

        Service service = ServiceCache.getService(bizConsumer.getService(), bizConsumer.getVersion());
        if (service == null) {
            int i = 3;
            while (service == null && i > 0) {
                service = ServiceCache.getService(bizConsumer.getService(), bizConsumer.getVersion());
                i--;
                try {
                    Thread.sleep(i * 1000);
                } catch (InterruptedException e) {
                }
            }
        }
        KafkaMessageProcessor processor = new KafkaMessageProcessor();
        String eventType;
        try {
            eventType = processor.getEventType(value);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("解析消息eventType出错，忽略该消息");
            return;
        }
        /**
         * 事件类型和 传入的even最后的事件名一致才处理
         */
        if (checkEquals(eventType, bizConsumer.getEvent())) {
            byte[] eventBinary = processor.getEventBinary();
            JsonSerializer jsonDecoder = new JsonSerializer(service, null, bizConsumer.getVersion(), MetaDataUtil.findStruct(bizConsumer.getEvent(), service));
            String body = jsonDecoder.read(new TCompactProtocol(new TKafkaTransport(eventBinary, TCommonTransport.Type.Read)));

            List<NameValuePair> pairs = combinesParams(eventType, body);
            try {
                CloseableHttpResponse post = post(bizConsumer.getUri(), pairs);
                if (post.getStatusLine().getStatusCode() != 200) {
                    //重试
                    logger.error("httpClient error");
                    InnerExecutor.service.execute(() -> {
                        try {
                            CloseableHttpResponse threadResult;
                            do {
                                threadResult = post(bizConsumer.getUri(), pairs);

                            } while (threadResult.getStatusLine().getStatusCode() != 200);
                        } catch (IOException e) {
                            logger.error("[HttpClient] io 异常 " + e.getMessage(), e);
                        }
                    });
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public CloseableHttpResponse post(String uri, List<NameValuePair> arguments) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(new UrlEncodedFormEntity(arguments, "UTF-8"));
        CloseableHttpResponse response = httpClient.execute(httpPost);
        return response;
    }


    public List<NameValuePair> combinesParams(String eventType, String params) {
        List<NameValuePair> pairs = new ArrayList<>(4);
        pairs.add(new BasicNameValuePair("event", eventType));
        pairs.add(new BasicNameValuePair("body", params));

        return pairs;
    }

    private boolean checkEquals(String eventType, String event) {
        try {
            String endEventType = eventType.substring(eventType.lastIndexOf("."));
            String endEvent = event.substring(event.lastIndexOf("."));
            return endEvent.equals(endEventType);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    private static class InnerExecutor {
        private static ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }
}
