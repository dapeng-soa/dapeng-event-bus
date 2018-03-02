package com.today.eventbus.serializer;

import com.github.dapeng.core.BeanSerializer;
import com.github.dapeng.org.apache.thrift.TException;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 描述: 序列化器 register
 *
 * @author hz.lei
 * @date 2018年03月02日 上午11:28
 */
public class BeanSerializerRegister {

    private static Map<String, BeanSerializer> beanSerializers = new ConcurrentHashMap<>(16);

    public static void register(String eventType, BeanSerializer serializer) {
        if (beanSerializers.containsKey(eventType)) {
            if (beanSerializers.get(eventType) == null) {
                beanSerializers.put(eventType, serializer);
            }
            throw new IllegalArgumentException("该event类型已注册对应的serializer 编解码器");
        }

        Assert.notNull(serializer, "serializer 序列化器不能为空");
        beanSerializers.put(eventType, serializer);
    }

    /**
     * decode message
     *
     * @param eventType
     * @param bytes
     * @param <T>
     * @return
     * @throws TException
     */
    public static <T> T decodeMessage(String eventType, byte[] bytes) throws TException {
        KafkaMessageProcessor<T> processor = new KafkaMessageProcessor<>();
        BeanSerializer beanSerializer = beanSerializers.get(eventType);
        Assert.notNull(beanSerializer, "获取beanSerializer 失败");
        T event = processor.decodeMessage(bytes, beanSerializer);
        return event;
    }


}
