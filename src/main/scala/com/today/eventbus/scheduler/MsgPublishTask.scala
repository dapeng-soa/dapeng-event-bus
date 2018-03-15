package com.today.eventbus.scheduler

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

import com.today.eventbus.{EventStore, MsgKafkaProducer}
import org.slf4j.LoggerFactory
import wangzx.scala_commons.sql._

/**
  *
  * 描述: 事件定时任务，轮询数据库，发送消息 to kafka
  *
  * @param kafkaHost kafka cluster:127.0.0.1:9091,127.0.0.1:9092
  * @author hz.lei
  * @date 2018年02月28日 下午3:00
  */
class MsgPublishTask(topic: String,
                     kafkaHost: String,
                     tidPrefix: String,
                     dataSource: DataSource) {
  private val logger = LoggerFactory.getLogger(classOf[MsgPublishTask])
  //transId: kafka消息对事务的支持前提，需要每一个生产者实例有不同的事务ID，全局唯一
  private val tid = tidPrefix + UUID.randomUUID().toString
  private val producer = new MsgKafkaProducer(kafkaHost, tid)
  logger.warn("Kafka producer transactionId:" + tid)

  /**
    * fetch message from database , then send to kafka broker
    */
  def doPublishMessages(): Unit = {
    if (logger.isDebugEnabled()) {
      logger.debug("begin to publish messages to kafka")
    }

    // 消息总条数计数器
    val counter = new AtomicInteger(0)
    // 批量处理, 每次从数据库取出消息的最大数量(window)
    val window = 100
    // 单轮处理的消息计数器, 用于控制循环退出.
    val resultSetCounter = new AtomicInteger(window)

    while (resultSetCounter.get() == window) {
      resultSetCounter.set(0)
      dataSource.withTransaction[Unit](conn => {
        //val lockId = insert into xxx
        conn.eachRow[EventStore](sql"SELECT * FROM common_event WHERE lock_id < ${lockId} limit ${window} FOR UPDATE")(event => {
          conn.executeUpdate(sql"DELETE FROM common_event WHERE id = ${event.id}")
          if (event.eventType == "TMP_LOCK") {
            producer.send(topic, event.id, event.eventBinary)

            counter.incrementAndGet()
          }

          resultSetCounter.incrementAndGet()

        })
      })
    }

    if (logger.isDebugEnabled()) {
      logger.debug(s"end publish messages(${counter.get()}) to kafka")
    }

  }

}


