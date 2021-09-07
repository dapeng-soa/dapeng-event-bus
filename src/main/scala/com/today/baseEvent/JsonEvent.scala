package com.today.baseEvent

/**
  * @author hui
  *         2021/6/10 0010 17:37 
  **/
import com.github.dapeng.core.BeanSerializer
import com.github.dapeng.org.apache.thrift.protocol.TCompactProtocol
import com.github.dapeng.util.TCommonTransport
import com.today.baseEvent.serializer.JsonEventSerializer
object JsonEvent {
  implicit val x: BeanSerializer[com.today.baseEvent.JsonEvent] = new JsonEventSerializer

  def getBytesFromBean(bean: JsonEvent): Array[Byte] = {
    val bytes = new Array[Byte](8192)
    val transport = new TCommonTransport(bytes, TCommonTransport.Type.Write)
    val protocol = new TCompactProtocol(transport)

    new JsonEventSerializer().write(bean, protocol)
    transport.flush()
    transport.getByteBuf
  }

  def getBeanFromBytes(bytes: Array[Byte]): JsonEvent = {
    val transport = new TCommonTransport(bytes, TCommonTransport.Type.Read)
    val protocol = new TCompactProtocol(transport)
    new JsonEventSerializer().read(protocol)
  }
}

/**
  * Autogenerated by Dapeng-Code-Generator (2.1.2-SNAPSHOT)
  *
  * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

  *
  **/
case class JsonEvent(

                      /**
                        *

                      事件Id

                        **/

                      id : Long, /**
                        *

                      订单编号

                        **/

                      json : String
                    )
