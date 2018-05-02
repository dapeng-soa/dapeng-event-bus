package com.today.eventbus.rest.support;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

import java.lang.reflect.Method;
import java.util.List;

/**
 * 描述: xml 解析
 *
 * @author hz.lei
 * @date 2018年05月03日 上午1:11
 */
@Root(name = "endpoint")
public class RestConsumerEndpoint {

    @Element(required = false)
    private String id;
    @Element(name = "groupId")
    private String groupId;
    @Element
    private String topic;
    @Element(required = false)
    private String eventType;
    @Element(required = false)
    private Method method;
    @Element(required = false)
    private Object bean;
    @Element(required = false)
    private List<Class<?>> parameterTypes;
    @Element
    private String kafkaHostKey = "kafka.consumer.host";


    @Element
    private String service;
    @Element
    private String version;
    @Element
    private String event;

    @Element
    private String uri;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

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

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public Object getBean() {
        return bean;
    }

    public void setBean(Object bean) {
        this.bean = bean;
    }

    public List<Class<?>> getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(List<Class<?>> parameterTypes) {
        this.parameterTypes = parameterTypes;
    }

    public String getKafkaHostKey() {
        return kafkaHostKey;
    }

    public void setKafkaHostKey(String kafkaHostKey) {
        this.kafkaHostKey = kafkaHostKey;
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

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getKafkaHost() {
        String kafkaHost = System.getenv(kafkaHostKey.replaceAll("\\.", "_"));
        if (kafkaHost == null) {
            kafkaHost = System.getProperty(kafkaHostKey);
        }
        if (kafkaHost == null) {
            kafkaHost = "127.0.0.1:9092";
        }
        return kafkaHost;
    }
}
