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
    persistenceEvent(None, event)
  }

  /**
    * fire a event with manually
    *
    * @param event biz custom event
    * @param conn  biz custom conn
    */
  def fireEventManually(event: Any, conn: Connection): Unit = {
    dispatchEvent(event)
    persistenceEventManually(None, conn, event)
  }

  /**
    * 顺序的触发事件，需要多传入一个业务内容。
    * 然后会根据这个内容将相同的tag消息发送到一个分区。
    *
    * @since 3.0  2018.11.23
    */
  def fireEventOrdered(biz: String, event: Any): Unit = {
    dispatchEvent(event)
    persistenceEvent(Option.apply(biz), event)
  }

  /**
    * 顺序的触发事件，需要多传入一个业务内容。
    * 然后会根据这个内容将相同的tag消息发送到一个分区。
    * <p>
    * 手动的事务.
    * </p>
    *
    * @since 3.0  2018.11.23
    */
  def fireEventOrderedManually(biz: String, conn: Connection, event: Any): Unit = {
    dispatchEvent(event)
    persistenceEventManually(Option.apply(biz), conn, event)
  }


  /**
    * 持久化 event 消息 to database
    */
  private def persistenceEvent(biz: Option[String], event: Any): Unit = {
    val (id, eventType, executeSql) = processSaveEvent(biz, event)
    dataSource.executeUpdate(executeSql)
    logger.info(s"save message unique id: $id, biz:$biz, eventType: $eventType successful ")
  }

  /**
    * 使用传入的 conn 对消息进行存储
    */
  private def persistenceEventManually(biz: Option[String], conn: Connection, event: Any): Unit = {
    val (id, eventType, executeSql) = processSaveEvent(biz, event)
    conn.executeUpdate(executeSql)
    logger.info(s"save message unique id: $id,biz: $biz, eventType: $eventType  successful with manually connection")
  }

  /**
    * 公用 存储逻辑
    */
  private def processSaveEvent(biz: Option[String], event: Any): (Long, String, SQLWithArgs) = {
    val processor = new KafkaMessageProcessor[Any]
    val bytes: Array[Byte] = processor.encodeMessage(event)
    val eventType = event.getClass.getName
    // fetch id
    val id = getMsgId(event)
    val executeSql = biz match {
      case Some(value) ⇒ sql"INSERT INTO dp_common_event set id=${id}, event_type=${eventType}, event_biz=${value}, event_binary=${bytes}"
      case None ⇒ sql"INSERT INTO  dp_common_event set id=${id}, event_type=${eventType}, event_binary=${bytes}"
    }
    (id, eventType, executeSql)
  }

  /**
    * Todo 临时解决方案
    * 反射拿到消息第一个id参数，作为存储数据库的 唯一 id
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
    * 业务系统处理事件分发逻辑
    *
    * @param event biz custom event
    */
  protected def dispatchEvent(event: Any): Unit

  /**
    * scala object 基于Spring的初始化方法
    *
    * @return
    */
  def getInstance: this.type
}
