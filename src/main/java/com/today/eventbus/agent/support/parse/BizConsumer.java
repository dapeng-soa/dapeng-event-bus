package com.today.eventbus.agent.support.parse;


import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/**
 * desc: 业务消费者
 *
 * @author hz.lei
 * @since 2018年05月26日 下午5:04
 */
@Root(name = "consumer")
public class BizConsumer {

    private String groupId;

    private String topic;

    private String kafkaHost;

    private String service;

    private String version;

    @Element(name = "event-type")
    private String eventType;

    @Element
    private String event;

    @Element(name = "destination-url")
    private String destinationUrl;


    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKafkaHost() {
        return kafkaHost;
    }

    public void setKafkaHost(String kafkaHost) {
        this.kafkaHost = kafkaHost;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getDestinationUrl() {
        return destinationUrl;
    }

    public void setDestinationUrl(String destinationUrl) {
        this.destinationUrl = destinationUrl;
    }

    @Override
    public String toString() {
        return "BizConsumer{" + "groupId='" + groupId + '\'' + ", topic='" + topic + '\'' +
                ", kafkaHost='" + kafkaHost + '\'' + ", service='" + service + '\'' +
                ", version='" + version + '\'' + ", eventType='" + eventType + '\'' +
                ", event='" + event + '\'' + ", destinationUrl='" + destinationUrl + '\'' + '}';
    }
}
