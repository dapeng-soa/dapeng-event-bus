package com.today.common.retry;

import com.github.dapeng.org.apache.thrift.TException;

/**
 * 描述: com.today.common.retry
 *
 * @author hz.lei
 * @date 2018年05月09日 上午11:36
 */
public interface RetryMsgCallback {

    void dealMessage() throws TException;
}
