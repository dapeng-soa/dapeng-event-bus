package com.today.eventbus;


import com.github.dapeng.core.BeanSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月02日 上午1:18
 */
public class ConsumerEndpoint {

    private Logger logger = LoggerFactory.getLogger(getClass());


    private String id;


    private String groupId;

    private String topic;

    private String eventType;

    private Method method;

    private Object bean;

    private List<Class<?>> parameterTypes;

    private String serializerType;

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

    /**
     * 根据 serializer 全限定名 反射获取 其对象
     *
     * @return
     * @throws ClassNotFoundException
     */
    public BeanSerializer getSerializerType() throws Exception {
        BeanSerializer serializer;
        try {
            serializer = (BeanSerializer) this.getClass().getClassLoader().loadClass(serializerType).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new Exception(e.getMessage(), e);
        }
        return serializer;
    }

    public void setSerializerType(String serializerType) {
        this.serializerType = serializerType;
    }
}
