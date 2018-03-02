package com.today.eventbus.dao

/**
  *
  * 描述:
  *
  * @author hz.lei
  * @date 2018年02月28日 下午2:50
  */
trait EventMsgDao {
  /**
    * 查找所有的失败的或者未知的事务过程记录
    *
    * @param
    * @return
    */

  def listMessages: List[EventStore]


  /**
    * fire事件后，将事件持久化到数据库中
    *
    * @param eventType
    * @param event
    * @return
    */
  def saveMessageToDB(eventType: String, event: Array[Byte]): Int

  /**
    * kafka consumer消费消息后，删除持久化的消息
    *
    * @param eventId
    * @return
    */
  def deleteMessage(eventId: Long): Int
}
