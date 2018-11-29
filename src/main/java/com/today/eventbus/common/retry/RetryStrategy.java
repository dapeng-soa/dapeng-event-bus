package com.today.eventbus.common.retry;

import com.github.dapeng.org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * 描述: 重试策略
 *
 * @author hz.lei
 * @since 2018年05月09日 上午11:22
 */
public abstract class RetryStrategy {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected RetryTemplate retryTemplate;

    public RetryStrategy() {

    }

    protected void init() {
        retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(createRetryPolicy());
        retryTemplate.setBackOffPolicy(createBackOffPolicy());
    }

    public void execute(RetryMsgCallback callback) {
        try {
            retryTemplate.execute((RetryCallback<Object, Exception>) context -> {
                try {
                    callback.dealMessage();
                } catch (TException e) {
                    logger.error("[Retry]:重试消息失败,重试次数: {}, Cause: {}", context.getRetryCount() + 1, e.getMessage());
                    throw new RuntimeException(e.getMessage(), e);
                }
                logger.info("[Retry]:消息重试消费成功,重试次数: " + (context.getRetryCount()));
                return true;
            }, context -> {
                logger.error("[Retry]:达到重试上限,不再进行重试,重试次数: {}", context.getRetryCount() + 1);
                return true;
            });
        } catch (Exception e) {
            logger.error("[Retry]:RetryStrategy have a error !");
        }

    }

    /**
     * Create a {@link SimpleRetryPolicy} with the specified number of retry
     * attempts.
     * 子类需要配置策略
     * <p>
     * maxAttempts         最多重试次数
     * retryableExceptions 定义触发哪些异常进行重试
     *
     * @return
     */
    protected abstract RetryPolicy createRetryPolicy();


    /**
     * 指数退避策略，需设置参数sleeper、initialInterval、maxInterval和multiplier，
     * <p>
     * 子类配置指数退避策略
     *
     * @return
     */
    protected abstract BackOffPolicy createBackOffPolicy();

}
