package com.today.common.retry;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.base.Predicates;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * 描述: com.today.common.retry
 *
 * @author hz.lei
 * @date 2018年05月09日 上午9:36
 */
public class HelloWorld {

    public static void main(String[] args) {
        Callable<Boolean> callable = () -> {
            System.out.println("do job ...");
            return true;
        };

        Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                .retryIfResult(Predicates.equalTo(true))
                .retryIfExceptionOfType(IOException.class)
                .retryIfRuntimeException()
                .withStopStrategy(StopStrategies.stopAfterAttempt(10))
                .build();
        try {

            for (int i = 0; i < 10; i++) {
                retryer.call(callable);
            }


            retryer.call(callable);
        } catch (RetryException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
