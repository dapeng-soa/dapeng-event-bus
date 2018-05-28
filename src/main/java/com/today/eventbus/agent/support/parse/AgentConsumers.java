package com.today.eventbus.agent.support.parse;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import java.util.List;

/**
 * desc: 组下面biz消费者组列表
 *
 * @author hz.lei
 * @since 2018年05月26日 下午7:32
 */
@Root
public class AgentConsumers {

    @ElementList(name = "consumers", type = BizConsumer.class, inline = true)
    private List<BizConsumer> consumers;

    public List<BizConsumer> getConsumers() {
        return consumers;
    }

    @Override
    public String toString() {
        return "AgentConsumers{" + "consumers=" + consumers + '}';
    }
}
