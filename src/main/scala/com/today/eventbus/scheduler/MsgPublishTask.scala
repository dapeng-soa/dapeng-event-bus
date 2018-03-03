package com.today.eventbus.scheduler

import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

import com.today.eventbus.{EventStore, MsgKafkaProducer}
import org.slf4j.LoggerFactory
import wangzx.scala_commons.sql._

/**
  *
  * 描述: 事件定时任务
  *
  * @author hz.lei
  * @date 2018年02月28日 下午3:00
  */
class MsgPublishTask(topic: String,
                     kafkaHost: String,
                     transId: String,
                     dataSource: DataSource) {
  val logger = LoggerFactory.getLogger(classOf[MsgPublishTask])
  var producer = MsgKafkaProducer(kafkaHost, transId)

  /**
    * fetch message from database , then send to kafka broker
    */
  def doPublishMessages(): Unit = {
    if (logger.isDebugEnabled()) {
      logger.debug("begin the publish_msg time scheduler ")
    }

    val counter = new AtomicInteger(100)
    while (counter.get() == 100) {
      counter.set(0)
      dataSource.withTransaction[Unit](conn => {
        conn.eachRow[EventStore](sql"SELECT * FROM common_event limit 100 FOR UPDATE")(event => {
          conn.executeUpdate(sql"DELETE FROM common_event WHERE id = ${event.id}")
          producer.send(topic, event.id, event.eventBinary)
          counter.incrementAndGet()
        })
      })
    }

    if (logger.isDebugEnabled()) {
      logger.debug("send listMessages successful ")
    }
  }

}


