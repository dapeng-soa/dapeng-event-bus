package com.today.eventbus.agent.support.parse;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/**
 * desc: 消费者组
 *
 * @author hz.lei
 * @since 2018年05月26日 下午7:31
 */
@Root(name = "consumer-group")

public class ConsumerGroup {
    @Attribute(required = false)
    private String id;

    @Element(name = "group-id")
    private String groupId;

    @Element
    private String topic;

    @Element(name = "kafka-host")
    private String kafkaHost;

    @Element
    private String service;

    @Element
    private String version;

    @Element(name = "thread-count")
    private Integer threadCount;

    @Element(name = "consumers")
    private AgentConsumers consumers;

    public String getId() {
        return id;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopic() {
        return topic;
    }

    public String getKafkaHost() {
        return kafkaHost;
    }

    public String getService() {
        return service;
    }

    public String getVersion() {
        return version;
    }

    public Integer getThreadCount() {
        return threadCount;
    }

    public AgentConsumers getConsumers() {
        return consumers;
    }

    @Override
    public String toString() {
        return "ConsumerGroup{" +
                "id='" + id + '\'' +
                ", groupId='" + groupId + '\'' +
                ", topic='" + topic + '\'' +
                ", kafkaHost='" + kafkaHost + '\'' +
                ", service='" + service + '\'' +
                ", version='" + version + '\'' +
                ", threadCount=" + threadCount +
                ", consumers=" + consumers +
                '}';
    }
}
