package com.today.eventbus.common.retry;

import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.util.Collections;

/**
 * 描述: com.today.eventbus.common.retry
 *
 * @author hz.lei
 * @since 2018年05月09日 下午2:41
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
     * maxInterval      最大重试间隔为 10s
     * <p>
     * 目标方法处理失败，马上重试，第二次会等待 initialInterval， 第三次等待  initialInterval * multiplier
     * 目前重试间隔 0s 2s 4s 8s
     *
     * @return
     */

    @Override
    protected BackOffPolicy createBackOffPolicy() {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(2000);
        backOffPolicy.setMultiplier(2);
        backOffPolicy.setMaxInterval(10000);
        return backOffPolicy;
    }


}
