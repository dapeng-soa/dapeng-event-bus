package com.today.eventbus.utils;

import com.github.dapeng.core.BeanSerializer;
import com.github.dapeng.org.apache.thrift.TException;
import com.github.dapeng.org.apache.thrift.protocol.TCompactProtocol;
import com.github.dapeng.util.TKafkaTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述:
 *
 * @author hz.lei
 * @date 2018年03月03日 下午3:56
 */
public class MsgDecoderUtil {

    private static Logger logger = LoggerFactory.getLogger(MsgDecoderUtil.class);

    /**
     * @param <T>
     * @param msgBinary
     * @param serializer
     * @return
     * @throws TException
     */
    public static <T> MsgInfo decodeMsg(byte[] msgBinary, BeanSerializer<T> serializer) throws TException {
        TKafkaTransport kafkaTransport = new TKafkaTransport(msgBinary, TKafkaTransport.Type.Read);
        TCompactProtocol protocol = new TCompactProtocol(kafkaTransport);
        // fetch eventType
        String eventType = kafkaTransport.getEventType();
        // fetch event real message
        T event = serializer.read(protocol);
        return new MsgInfo(eventType, event);
    }


}
