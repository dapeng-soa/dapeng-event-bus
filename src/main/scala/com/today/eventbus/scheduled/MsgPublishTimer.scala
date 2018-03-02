package com.today.eventbus.scheduled

import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

import com.today.eventbus.MsgKafkaProducer
import com.today.eventbus.dao.{EventMsgDao, EventStore}
import org.slf4j.LoggerFactory
import wangzx.scala_commons.sql._

/**
  *
  * 描述: 事件定时任务
  *
  * @author hz.lei
  * @date 2018年02月28日 下午3:00
  */
class MsgPublishTimer(topic: String, host: String, transId: String, msgDao: EventMsgDao, dataSource: DataSource) {
  val eventMsgDao: EventMsgDao = msgDao
  val producerTopic: String = topic
  val kafkaServerHost: String = host
  val ds: DataSource = dataSource

  val logger = LoggerFactory.getLogger(classOf[MsgPublishTimer])
  var producer = MsgKafkaProducer(kafkaServerHost, transId)

  /**
    * fetch message from database , then send to kafka broker
    */
  def doPublishMessages(): Unit = {
    logger.info("begin the publish_msg time scheduler ")

    //    val eventStores: List[EventStore] = eventMsgDao.listMessages
    //    if (!eventStores.isEmpty) {
    //      eventStores.foreach(es => {
    val counter = new AtomicInteger(100)
    while (counter.get() == 100) {
      counter.set(0)
      ds.withTransaction[Unit](conn => {
        conn.eachRow[EventStore](sql"SELECT * FROM common_event limit 100 FOR UPDATE")(event => {
          conn.executeUpdate(sql"DELETE FROM common_event WHERE id = ${event.id}")
          producer.send(producerTopic, event.id, event.eventBinary)
          counter.incrementAndGet()
        })
      })
    }

    //        })
    //      })
    logger.info("send listMessages successful ")
    //    } else {
    //      logger.debug("database no message to process ")
    //    }
  }

}


