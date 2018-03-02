package com.today.eventbus

import com.github.dapeng.org.apache.thrift.TException
import com.today.eventbus.dao.EventMsgDao
import com.today.eventbus.serializer.KafkaMessageProcessor
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty

/**
  *
  * 描述:
  *
  * @author hz.lei
  * @date 2018年02月28日 下午2:46
  */
trait AbstractEventBus {

  private val logger = LoggerFactory.getLogger(classOf[AbstractEventBus])

  /**
    * scala object 基于Spring的初始化方法
    *
    * @return
    */
  def getInstance: this.type


  @BeanProperty
  var eventDao: EventMsgDao = _


  @throws[TException]
  def fireEvent(event: Any): Unit = {
    try {
      dispatchEvent(event)
      persistenceEvent(event)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
    }

  }

  /**
    * 业务系统处理事件分发逻辑
    *
    * @param event
    */
  protected def dispatchEvent(event: Any): Unit

  @throws[TException]
  private def persistenceEvent(event: Any): Unit = {
    logger.info("prepare to save event message")
    val processor = new KafkaMessageProcessor[Any]
    val bytes: Array[Byte] = processor.buildMessageByte(event)

    eventDao.saveMessageToDB(event.getClass.getName, bytes)
    logger.info("save message successful ")
  }


}
