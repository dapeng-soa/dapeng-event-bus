package com.today.eventbus.rest.support;

import com.github.dapeng.core.metadata.Service;
import com.github.dapeng.json.JsonSerializer;
import com.github.dapeng.openapi.cache.ServiceCache;
import com.github.dapeng.org.apache.thrift.TException;
import com.github.dapeng.org.apache.thrift.protocol.TCompactProtocol;
import com.github.dapeng.util.MetaDataUtil;
import com.github.dapeng.util.TCommonTransport;
import com.github.dapeng.util.TKafkaTransport;
import com.today.common.MsgConsumer;
import com.today.common.retry.DefaultRetryStrategy;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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
public class RestKafkaConsumer extends MsgConsumer<Long, byte[], RestConsumerEndpoint> {


    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     */
    public RestKafkaConsumer(String kafkaHost, String groupId, String topic) {
        super(kafkaHost, groupId, topic);
    }

    @Override
    protected void init() {
        logger.info(new StringBuffer("[RestKafkaConsumer] [init] ")
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


    @Override
    protected void dealMessage(RestConsumerEndpoint bizConsumer, byte[] value) throws TException {

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
            logger.error("[RestKafkaConsumer]:解析消息eventType出错，忽略该消息");
            return;
        }

        /**
         * 事件类型和 传入的even最后的事件名一致才处理
         */
        if (checkEquals(eventType, bizConsumer.getEvent())) {
            byte[] eventBinary = processor.getEventBinary();
            /**
             * 针对 2.0.1
             */
            JsonSerializer jsonDecoder = new JsonSerializer(service, null/*, bizConsumer.getVersion()*/, MetaDataUtil.findStruct(bizConsumer.getEvent(), service));

            String body = jsonDecoder.read(new TCompactProtocol(new TKafkaTransport(eventBinary, TCommonTransport.Type.Read)));


            List<NameValuePair> pairs = combinesParams(eventType, body);
            CloseableHttpResponse postResult = post(bizConsumer.getUri(), pairs);
            try {
                if (postResult.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    String content = EntityUtils.toString(postResult.getEntity(), "UTF-8");
                    String resp = new String(content.getBytes(), "UTF-8");
                    logger.info("response code: " + resp);
                } else {
                    //重试
                    logger.error("[RestKafkaConsumer]<->[HttpClient] 调用远程url error");
                    InnerExecutor.service.execute(() -> {
                        int i = 1;
                        CloseableHttpResponse threadResult;
                        do {
                            logger.info("[RestKafkaConsumer]<->[HttpClient] 线程: " + Thread.currentThread().getName() + " 进行重试,重试次数：" + i);
                            threadResult = post(bizConsumer.getUri(), pairs);
                        } while (i++ <= 3 && threadResult.getStatusLine().getStatusCode() != 200);
                    });
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    postResult.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public CloseableHttpResponse post(String uri, List<NameValuePair> arguments) {
        logger.info("[RestKafkaConsumer]:收到消息并代理成功,准备通过httpClient请求！");
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(uri);
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(arguments, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
        }

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
        } catch (IOException e) {
            logger.error("[RestKafkaConsumer]<->[execute httpClient error] " + e.getMessage(), e);
        }
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

    @Override
    protected void buildRetryStrategy() {
        retryStrategy = new DefaultRetryStrategy();
    }
}
