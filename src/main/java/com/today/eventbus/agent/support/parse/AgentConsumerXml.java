package com.today.eventbus.agent.support.parse;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import java.util.List;

/**
 * 描述: kafka消息代理 配置 根目录
 *
 * @author hz.lei
 * @since 2018年05月03日 上午12:45
 */
@Root(name = "consumer-groups")
public class AgentConsumerXml {

    @ElementList(name = "consumer-group", type = ConsumerGroup.class, inline = true)
    private List<ConsumerGroup> consumerGroups;

    public List<ConsumerGroup> getConsumerGroups() {
        return consumerGroups;
    }

    @Override
    public String toString() {
        return "AgentConsumerXml{" + "consumerGroups=" + consumerGroups + '}';
    }
}
