package com.today.eventbus.agent.support;

import com.github.dapeng.core.metadata.Service;
import com.github.dapeng.json.JsonSerializer;
import com.github.dapeng.openapi.cache.ServiceCache;
import com.github.dapeng.org.apache.thrift.TException;
import com.github.dapeng.org.apache.thrift.protocol.TCompactProtocol;
import com.github.dapeng.util.MetaDataUtil;
import com.github.dapeng.util.TCommonTransport;
import com.github.dapeng.util.TKafkaTransport;
import com.today.eventbus.agent.support.parse.BizConsumer;
import com.today.eventbus.common.MsgConsumer;
import com.today.eventbus.common.retry.DefaultRetryStrategy;
import com.today.eventbus.config.KafkaConfigBuilder;
import com.today.eventbus.serializer.KafkaMessageProcessor;
import com.today.eventbus.utils.CharDecodeUtil;
import com.today.eventbus.utils.ResponseResult;
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
 * 描述: kafka消息代理,将消息解码为json，并转发给目标url
 *
 * @author hz.lei
 * @since 2018年05月02日 下午4:39
 */
public class RestKafkaConsumer extends MsgConsumer<Long, byte[], BizConsumer> {

    private String instName;

    /**
     * @param kafkaHost host1:port1,host2:port2,...
     * @param groupId
     * @param topic
     */
    public RestKafkaConsumer(String instName, String kafkaHost, String groupId, String topic) {
        super(kafkaHost, groupId, topic);
        this.instName = instName;
    }

    public String getInstName() {
        return instName;
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
    protected void dealMessage(BizConsumer bizConsumer, byte[] value) throws TException {
        Service service = ServiceCache.getService(bizConsumer.getService(), bizConsumer.getVersion());
        if (service == null) {
            int i = 0;
            while (service == null && i < 3) {
                service = ServiceCache.getService(bizConsumer.getService(), bizConsumer.getVersion());
                if (service != null) {
                    break;
                }
                i++;
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
        if (filterBizConsumerByEventType(eventType, bizConsumer.getEvent())) {
            byte[] eventBinary = processor.getEventBinary();
            /**
             * 针对 dapeng 2.0.2
             */
            JsonSerializer jsonDecoder = new JsonSerializer(service, null, bizConsumer.getVersion(), MetaDataUtil.findStruct(bizConsumer.getEvent(), service));

            String body = jsonDecoder.read(new TCompactProtocol(new TKafkaTransport(eventBinary, TCommonTransport.Type.Read)));

            List<NameValuePair> pairs = combinesParams(eventType, body);

            logger.info("[RestKafkaConsumer]:解析消息成功,准备 httpClient post request !");
            ResponseResult postResult = post(bizConsumer.getDestinationUrl(), pairs);

            if (postResult.getCode() == HttpStatus.SC_OK) {
                String response = CharDecodeUtil.decodeUnicode(postResult.getContent());
                logger.info("[HttpClient]:response code: {}, event:{}, url:{}",
                        response, bizConsumer.getEvent(), bizConsumer.getDestinationUrl());
            } else {
                //重试
                logger.error("[HttpClient]:调用远程url: {} 失败,http code: {},topic:{},event:{}",
                        bizConsumer.getDestinationUrl(), postResult.getCode(), bizConsumer.getTopic(), bizConsumer.getEvent());
                // another thread to execute retry
                InnerExecutor.service.execute(() -> {
                    int i = 1;
                    ResponseResult threadResult;
                    do {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            logger.error("睡眠10s,等待重试，被打断! " + e.getMessage());
                        }
                        logger.info("[HttpRetry]-{},httpclient重试，url:{},topic:{},event:{},重试次数:{}",
                                Thread.currentThread().getName(), bizConsumer.getDestinationUrl(), bizConsumer.getTopic(), bizConsumer.getEvent(), i);
                        threadResult = post(bizConsumer.getDestinationUrl(), pairs);
                        logger.info("重试返回结果:response code: {}, event:{}, url:{}",
                                CharDecodeUtil.decodeUnicode(threadResult.getContent()), bizConsumer.getEvent(), bizConsumer.getDestinationUrl());
                    } while (i++ <= 3 && threadResult.getCode() != HttpStatus.SC_OK);
                });
            }
        }
    }

    /**
     * httpClient to post request
     *
     * @param uri
     * @param arguments
     * @return
     */
    public ResponseResult post(String uri, List<NameValuePair> arguments) {

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(uri);
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(arguments, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
        }

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);

            int code = response.getStatusLine().getStatusCode();
            String content = EntityUtils.toString(response.getEntity(), "UTF-8");
            return new ResponseResult(code, content);
        } catch (IOException e) {
            logger.error("[RestKafkaConsumer]<->[execute httpClient error] " + e.getMessage(), e);
        } finally {
            // close resource
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }

            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return new ResponseResult(-1, "");
    }


    /**
     * combine request parameters
     *
     * @param eventType
     * @param params
     * @return
     */
    private List<NameValuePair> combinesParams(String eventType, String params) {
        List<NameValuePair> pairs = new ArrayList<>(4);
        pairs.add(new BasicNameValuePair("event", eventType));
        pairs.add(new BasicNameValuePair("body", params));

        return pairs;
    }


    /**
     * filterBizConsumerByEventType current eventType is the consumer subscribe eventType
     *
     * @param eventType
     * @param event
     * @return
     */
    private boolean filterBizConsumerByEventType(String eventType, String event) {
        try {
            String endEventType = eventType.substring(eventType.lastIndexOf("."));
            String endEvent = event.substring(event.lastIndexOf("."));
            return endEvent.equals(endEventType);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * retry thread pool ,lazy
     */
    private static class InnerExecutor {
        private static ExecutorService service = Executors.newSingleThreadExecutor();
    }


    /**
     * 重试策略
     */
    @Override
    protected void buildRetryStrategy() {
        retryStrategy = new DefaultRetryStrategy();
    }
}
