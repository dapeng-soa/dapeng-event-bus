package com.today.common.retry;

import com.github.rholder.retry.*;
import com.google.common.base.Predicates;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * 描述: com.today.common.retry
 *
 * @author hz.lei
 * @date 2018年05月09日 上午9:36
 */
public class HelloWorld {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        Callable<Boolean> callable = () -> {
            Thread.sleep(1000);
            System.out.println("do job ...");
            return true;
        };

        Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                .retryIfResult(Predicates.equalTo(true))
                .retryIfExceptionOfType(IOException.class)
                .retryIfRuntimeException()
                .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(2, TimeUnit.SECONDS, executorService))
                .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                .build();
        for (int i = 0; i < 10; i++) {
            try {
                retryer.call(callable);
            } catch (RetryException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            System.out.println("------");
        }
    }
}
