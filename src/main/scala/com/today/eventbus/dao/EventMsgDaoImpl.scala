package com.today.eventbus.dao

import javax.sql.DataSource

import wangzx.scala_commons.sql._

import scala.beans.BeanProperty

/**
  *
  * 描述: 消息机制 数据dao
  *
  * @author hz.lei
  * @date 2018年02月28日 下午2:53
  */
class EventMsgDaoImpl extends EventMsgDao {

  @BeanProperty
  var ds: DataSource = _

  /**
    * 查找所有的失败的或者未知的事务过程记录
    *
    * @param
    * @return
    */
  override def listMessages: List[EventStore] = {
    val res: List[EventStore] = ds.rows[EventStore](sql"SELECT * FROM common_event")
    res
  }


  /**
    * fire事件后，将事件持久化到数据库中
    *
    * @param eventType
    * @param event
    * @return
    */
  override def saveMessageToDB(eventType: String, event: Array[Byte]): Int = {
    val executeSql = sql"INSERT INTO  common_event set event_type=${eventType}, event_binary=${event}"
    ds.executeUpdate(executeSql)
  }

  /**
    * kafka consumer消费消息后，删除持久化的消息
    *
    * @param eventId
    * @return
    */
  override def deleteMessage(eventId: Long): Int = {
    val executeSql = sql"DELETE FROM common_event WHERE id = ${eventId}"
    ds.executeUpdate(executeSql)
  }

}
