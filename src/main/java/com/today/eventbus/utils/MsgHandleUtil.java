package com.today.eventbus.utils;

import com.github.dapeng.core.BeanSerializer;
import com.github.dapeng.org.apache.thrift.TException;
import com.github.dapeng.org.apache.thrift.protocol.TCompactProtocol;
import com.github.dapeng.util.TKafkaTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 描述: 提供给第三方应用，进行序列化器的注册，以及消息的解码
 *
 * @author hz.lei
 * @date 2018年03月03日 下午3:56
 */


public class MsgHandleUtil {

    private static Logger logger = LoggerFactory.getLogger(MsgHandleUtil.class);


    private static Map<String, BeanSerializer> beanSerializers = new ConcurrentHashMap<>(16);

    /**
     * 第三方应用注册序列化器, .i.e 哪些事件你需要关心？ 在这里根据事件的全限定名，进行注册
     *
     * @param eventType
     * @param serializer
     */
    public static void register(String eventType, BeanSerializer serializer) {
        if (beanSerializers.containsKey(eventType)) {
            if (beanSerializers.get(eventType) == null) {
                beanSerializers.put(eventType, serializer);
            }
            throw new IllegalArgumentException("该event类型已注册对应的serializer 编解码器");
        }

        if (serializer == null) {
            throw new IllegalArgumentException("serializer 序列化器不能为空");
        }
        beanSerializers.put(eventType, serializer);
        logger.info("register beanSerializer successful, eventType[ {} ]", eventType);
    }


    /**
     * 传入 eventType的全限定名，获取对应的序列化器，进行消息序列化，并返回消息体。
     *
     * @param
     * @param msgBinary
     * @return
     * @throws TException
     */
    public static KafkaMsgInfo decodeMsg(byte[] msgBinary) throws TException {
        TKafkaTransport kafkaTransport = new TKafkaTransport(msgBinary, TKafkaTransport.Type.Read);
        TCompactProtocol protocol = new TCompactProtocol(kafkaTransport);
        // fetch eventType
        String eventType = kafkaTransport.getEventType();

        BeanSerializer serializer = getSerializerByType(eventType);

        // fetch event real message
        Object event = serializer.read(protocol);

        KafkaMsgInfo msgInfo = new KafkaMsgInfo(eventType, event);

        return msgInfo;
    }

    /**
     * 根据eventType 获取 对应的序列化器 实例
     *
     * @throws TException 可能初始化时 没有注册序列化器
     */
    public static BeanSerializer getSerializerByType(String eventType) throws TException {
        BeanSerializer beanSerializer = beanSerializers.get(eventType);
        if (beanSerializer == null) {
            throw new TException("eventType: [ " + eventType + " ]对应的序列化器未注册，请检查");
        }
        return beanSerializer;
    }

    public static class KafkaMsgInfo {
        private String eventType;
        private Object event;

        public KafkaMsgInfo(String eventType, Object event) {
            this.eventType = eventType;
            this.event = event;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public Object getEvent() {
            return event;
        }

        public void setEvent(Object event) {
            this.event = event;
        }
    }


}
