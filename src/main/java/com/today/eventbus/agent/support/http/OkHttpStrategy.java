package com.today.eventbus.agent.support.http;

import com.today.eventbus.utils.ResponseResult;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-12-12 2:16 PM
 */
public class OkHttpStrategy implements HttpStrategy {
    private static final Logger logger = LoggerFactory.getLogger(OkHttpStrategy.class);

//    private static final MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded; charset=utf-8");

//    private final OkHttpClient okClient = new OkHttpClient.Builder()
//            .connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS))
//            .build();

    private final OkHttpClient okClient = new OkHttpClient();


    public ResponseResult post(String url, String eventType, String params) {
        long begin = System.currentTimeMillis();

        RequestBody requestBody = new FormBody.Builder()
                .add("event", eventType)
                .add("body", params)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();


        try (Response response = okClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                if (response.body() != null) {
                    String content = response.body().toString();
                    logger.info("[OKHttp]请求耗时: {}ms", System.currentTimeMillis() - begin);
                    return new ResponseResult(response.code(), content, null);
                }
                return new ResponseResult(response.code(), null, null);
            } else {
                throw new IOException("Unexpected code " + response);
            }
        } catch (IOException e) {
            logger.warn("[Consumer]::[OKHttp调用失败] " + e.getMessage(), e);
            return new ResponseResult(-1, "", e);
        }
    }

    @Override
    public void close() {
        logger.info("okHttp no need to close");
    }
}
