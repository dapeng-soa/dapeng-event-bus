package com.today.eventbus.agent.support.http;

import com.today.eventbus.utils.ResponseResult;
import okhttp3.*;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-12-12 2:16 PM
 */
public class HttpClientStrategy implements HttpStrategy {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientStrategy.class);

    private final CloseableHttpClient httpClient = HttpClients.createDefault();


    @Override
    public ResponseResult post(String url, String eventType, String params) {
        List<NameValuePair> nameValuePairs = combinesParams(eventType, params);
        return doPost(url, nameValuePairs);
    }

    @Override
    public void close() {
        try {
            if (httpClient != null)
                httpClient.close();
        } catch (IOException e) {
            try {
                httpClient.close();
            } catch (IOException e1) {
                logger.error(e1.getMessage(), e1);
            }
        }
    }

    /**
     * httpClient to post request
     */
    private ResponseResult doPost(String uri, List<NameValuePair> arguments) {
        long begin = System.currentTimeMillis();
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(new UrlEncodedFormEntity(arguments, StandardCharsets.UTF_8));

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            String content = EntityUtils.toString(response.getEntity(), "UTF-8");
            logger.info("[HttpPost]请求耗时: {}ms", System.currentTimeMillis() - begin);
            return new ResponseResult(code, content, null);
        } catch (IOException e) {
            logger.warn("[RestKafkaConsumer]::[httpClient调用失败] " + e.getMessage(), e);
            return new ResponseResult(-1, "", e);
        } finally {
            // close resource
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * combine request parameters
     */
    private List<NameValuePair> combinesParams(String eventType, String params) {
        List<NameValuePair> pairs = new ArrayList<>(4);
        pairs.add(new BasicNameValuePair("event", eventType));
        pairs.add(new BasicNameValuePair("body", params));
        return pairs;
    }
}
