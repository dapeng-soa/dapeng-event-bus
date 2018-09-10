package com.today.eventbus;


import com.github.dapeng.core.BeanSerializer;

import java.lang.reflect.Method;
import java.util.List;

/**
 * 描述: 业务关心 consumer 上下文
 *
 * @author hz.lei
 * @since 2018年03月02日 上午1:18
 */
public class ConsumerEndpoint {
    private String id;

    private String groupId;

    private String topic;

    private String eventType;

    private Method method;

    private Object bean;

    private List<Class<?>> parameterTypes;

    private Class<?> serializer;

    private String kafkaHostKey;

    private Boolean isBinlog = false;

    /**
     * session timeout
     */
    private long timeout;

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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public void setSerializer(Class<?> serializer) {
        this.serializer = serializer;
    }

    public Boolean getBinlog() {
        return isBinlog;
    }

    public void setBinlog(Boolean binlog) {
        isBinlog = binlog;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * 根据 serializer Class<?> 类型 创建实例 返回
     *
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public BeanSerializer getEventSerializer() throws IllegalAccessException, InstantiationException {
        return (BeanSerializer) serializer.newInstance();
    }


    public String getKafkaHost() {
        String kafkaHost = System.getenv(kafkaHostKey.replaceAll("\\.", "_"));
        if (kafkaHost == null) {
            kafkaHost = System.getProperty(kafkaHostKey);
        }
        return kafkaHost;
    }

    public void setKafkaHostKey(String kafkaHostKey) {
        this.kafkaHostKey = kafkaHostKey;
    }
}
