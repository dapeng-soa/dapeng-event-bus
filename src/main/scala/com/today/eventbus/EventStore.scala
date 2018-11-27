package com.today.eventbus

/**
  *
  * 描述: 消息持久化 case class
  *
  * @author hz.lei
  * @since 2018年02月28日 下午2:50
  */
case class EventStore(id: Long, eventType: String, eventBiz: Option[String], eventBinary: Array[Byte])
