package com.today.eventbus

import java.sql.Timestamp

/**
  *
  * 描述: 消息持久化 case class
  *
  * @author hz.lei
  * @date 2018年02月28日 下午2:50
  */
case class EventStore(id: Long, eventType: String, eventBinary: Array[Byte], updatedAt: Timestamp)
