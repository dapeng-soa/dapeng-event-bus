package com.today.eventbus.common.retry;

import com.github.dapeng.org.apache.thrift.TException;

/**
 * 描述: com.today.eventbus.common.retry
 *
 * @author hz.lei
 * @since 2018年05月09日 上午11:36
 */
public interface RetryMsgCallback {

    void dealMessage() throws TException;
}
