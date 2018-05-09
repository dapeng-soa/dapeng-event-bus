package com.today.common.retry;

import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.util.Collections;

/**
 * 描述: com.today.common.retry
 *
 * @author hz.lei
 * @date 2018年05月09日 下午2:41
 */
public class DefaultRetryStrategy extends RetryStrategy {

    /**
     * 默认 SimpleRetryPolicy 策略
     * <p>
     * maxAttempts         最多重试次数
     * retryableExceptions 定义触发哪些异常进行重试
     *
     * @return
     */
    @Override
    protected RetryPolicy createRetryPolicy() {
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(4, Collections.singletonMap(Exception.class, true));
        return simpleRetryPolicy;
    }

    /**
     * 指数退避策略，需设置参数sleeper、initialInterval、maxInterval和multiplier，
     * <p>
     * initialInterval 指定初始休眠时间，默认100毫秒，
     * multiplier      指定乘数，即下一次休眠时间为当前休眠时间*multiplier
     *
     * @return
     */

    @Override
    protected BackOffPolicy createBackOffPolicy() {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);
        backOffPolicy.setMultiplier(4);
        return backOffPolicy;
    }


}
