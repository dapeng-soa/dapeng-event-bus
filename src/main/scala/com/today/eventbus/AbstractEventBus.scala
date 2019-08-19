package com.today.eventbus

import java.lang.reflect.Field
import java.sql.Connection

import javax.sql.DataSource
import com.github.dapeng.org.apache.thrift.TException
import com.today.eventbus.serializer.KafkaMessageProcessor
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty
import wangzx.scala_commons.sql._

/**
  *
  * 描述: 消息总线 基类
  *
  * @author hz.lei
  * @since 2018年02月28日 下午2:46
  */
trait AbstractEventBus {

  private val logger = LoggerFactory.getLogger(classOf[AbstractEventBus])

  @BeanProperty
  var dataSource: DataSource = _

  /**
    * fire a event
    *
    * @param event biz custom event
    */
  def fireEvent(event: Any): Unit = {
    dispatchEvent(event)
    persistenceEvent(event)
  }

  /**
    * fire a event with manually
    *
    * @param event biz custom event
    * @param conn  biz custom conn
    */
  def fireEventManually(event: Any, conn: Connection): Unit = {
    dispatchEvent(event)
    persistenceEventManually(event, conn)
  }

  /**
    * 业务系统处理事件分发逻辑
    *
    * @param event biz custom event
    */
  protected def dispatchEvent(event: Any): Unit

  /**
    * 持久化 event 消息 to database
    *
    * @param event biz custom event
    */
  @throws[TException]
  private def persistenceEvent(event: Any): Unit = {
    val processor = new KafkaMessageProcessor[Any]
    val bytes: Array[Byte] = processor.encodeMessage(event)
    val eventType = event.getClass.getName
    // fetch id
    val id = getMsgId(event)
    val topic = getMsgTopic(event)
    val key = getMsgKey(event)
    val executeSql = sql"INSERT INTO  dp_event_info set id=${id}, event_type=${eventType}, event_binary=${bytes}, event_topic=${topic},event_key=${key}"
    dataSource.executeUpdate(executeSql)

    logger.info(s"save message unique id: $id, eventType: $eventType successful ")
  }

  /**
    * 使用传入的 conn 对消息进行存储
    *
    * @param event
    * @param conn
    * @throws
    */
  @throws[TException]
  private def persistenceEventManually(event: Any, conn: Connection): Unit = {
    logger.info("prepare to save event message with manually connection")
    val processor = new KafkaMessageProcessor[Any]
    val bytes: Array[Byte] = processor.encodeMessage(event)
    val eventType = event.getClass.getName
    // fetch id
    val id = getMsgId(event)
    val topic = getMsgTopic(event)
    val key = getMsgKey(event)
    val executeSql = sql"INSERT INTO  dp_event_info set id=${id}, event_type=${eventType}, event_binary=${bytes}, event_topic=${topic},event_key=${key}"
    conn.executeUpdate(executeSql)

    logger.info(s"save message unique id: $id, eventType: $eventType  successful with manually connection")
  }

  /**
    * Todo 临时解决方案
    * 反射拿到消息第一个id参数，作为存储数据库的 唯一 id
    *
    * @param event
    * @return
    */
  private def getMsgId(event: Any): Long = {
    try {
      val field: Field = event.getClass.getDeclaredField("id")
      field.setAccessible(true)
      field.get(event).asInstanceOf[Long]
    } catch {
      case e: Exception =>
        logger.error("获取消息id失败，请检查事件是否带有唯一id，以及字段名是否为id")
        throw e
    }
  }

  /**
    *
    * 反射拿到event里面的topic
    *
    * @param event
    * @return
    */
  private def getMsgTopic(event: Any): String = {
    try {
      val field: Field = event.getClass.getDeclaredField("topic")
      field.setAccessible(true)
      field.get(event) match {
        case Some(x) =>  x.asInstanceOf[String]
        case None =>  null
      }
    } catch {
      case e: Exception =>
        logger.error("获取event的topic失败...")
        throw e
    }
  }

  /**
    *
    * 反射拿到event里面的key值
    *
    * @param event
    * @return
    */
  private def getMsgKey(event: Any): String = {
    try {
      val field: Field = event.getClass.getDeclaredField("key")
      field.setAccessible(true)
      field.get(event) match {
        case Some(x) =>  x.asInstanceOf[String]
        case None =>  null
      }
    } catch {
      case e: Exception =>
        logger.error("获取event的key值失败...")
        throw e
    }
  }

  /**
    * scala object 基于Spring的初始化方法
    *
    * @return
    */
  def getInstance: this.type
}
