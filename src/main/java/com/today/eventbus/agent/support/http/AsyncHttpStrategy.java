package com.today.eventbus.agent.support.http;

import com.today.eventbus.agent.support.parse.BizConsumer;
import com.today.eventbus.utils.ResponseResult;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.asynchttpclient.*;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-12-12 2:16 PM
 */
public class AsyncHttpStrategy implements HttpStrategy {
    private static final Logger logger = LoggerFactory.getLogger(AsyncHttpStrategy.class);

    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private static final AsyncHttpClient asyncHttpClient = Dsl.asyncHttpClient();

    @Override
    public void asyncPost(BizConsumer bizConsumer, Long keyId, String eventType, String body, String eventLog) {
        logger.info("[AsyncHttp]准备通过异步请求模式");
        long begin = System.currentTimeMillis();
        String url = bizConsumer.getDestinationUrl();
        CompletableFuture<Response> responseFuture = asyncHttpClient.preparePost(url)
                .addFormParam("event", eventType)
                .addFormParam("body", body)
                .execute()
                .toCompletableFuture();

        responseFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.warn("[Consumer]::[OKHttp调用失败] " + ex.getMessage(), ex);
                retry(bizConsumer, keyId, eventType, body, eventLog);
                return;
            }
            logger.info("[AsyncHttp]请求耗时:{} ms", (System.currentTimeMillis() - begin));
            int code = result.getStatusCode();
            if (code == HttpStatus.SC_OK) {
                String content = result.getResponseBody(StandardCharsets.UTF_8);
                //返回结果也做一下限制
                String summary = content.length() <= 200 ? content : content.substring(0, 200);
                logger.info("[Http]:消息ID: {}, remote response: code:{}, body:{}, event:{}, url:{},event内容:{}",
                        keyId, code, summary, bizConsumer.getEvent(), bizConsumer.getDestinationUrl(), eventLog);
            } else {
                //重试
                logger.warn("[AsyncHttp]:调用远程url: {} 失败,进行重试。http code: {},topic:{},event:{},event内容:{}",
                        bizConsumer.getDestinationUrl(), result.getStatusCode(), bizConsumer.getTopic(), bizConsumer.getEvent(), eventLog);

                executor.execute(() -> {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        logger.error("睡眠10s,等待重试，被打断! " + e.getMessage());
                    }
                    logger.info("[HttpRetry]-{},http重试，url:{},topic:{},event:{}",
                            Thread.currentThread().getName(), bizConsumer.getDestinationUrl(), bizConsumer.getTopic(), bizConsumer.getEvent());

                    retry(bizConsumer, keyId, eventType, body, eventLog);
                });
            }
        });
    }

    private void retry(BizConsumer bizConsumer, Long keyId, String eventType, String body, String eventLog) {
        long begin = System.currentTimeMillis();

        String url = bizConsumer.getDestinationUrl();

        CompletableFuture<Response> responseFuture = asyncHttpClient.preparePost(url)
                .addFormParam("event", eventType)
                .addFormParam("body", body)
                .execute()
                .toCompletableFuture();

        responseFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.info("[AsyncHttp]重试1次失败出现异常,不再重试,具体异常:{}", ex);
                return;
            }
            logger.info("[AsyncHttp]重试请求耗时:{} ms", (System.currentTimeMillis() - begin));
            int statusCode = result.getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                String content = result.getResponseBody(StandardCharsets.UTF_8);
                //返回结果也做一下限制
                String summary = content.length() <= 200 ? content : content.substring(0, 200);
                logger.info("[AsyncHttp]:重试成功. 消息ID: {}, remote response: code:{}, body:{}, event:{}, url:{},event内容:{}",
                        keyId, statusCode, summary, bizConsumer.getEvent(), bizConsumer.getDestinationUrl(), eventLog);
            } else {
                logger.info("[AsyncHttp]重试1次失败，不再重试... remote code: {}", statusCode);
            }
        });
    }


    @Override
    public ResponseResult post(String url, String eventType, String params) {
        throw new UnsupportedOperationException("AsyncHttpStrategy不支持同步post");
    }

    @Override
    public void close() {
        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            try {
                asyncHttpClient.close();
            } catch (IOException e1) {
                logger.error(e.getMessage(), e1);

            }
        }
    }
}
