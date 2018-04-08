package com.today.eventbus.scheduler

import java.sql.Connection
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.github.dapeng.util.SoaSystemEnvProperties
import com.google.common.util.concurrent.ThreadFactoryBuilder
import javax.sql.DataSource
import com.today.eventbus.{EventStore, MsgKafkaProducer}
import org.slf4j.LoggerFactory
import com.today.eventbus.scheduler.ScalaSql._
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
  val period = SoaSystemEnvProperties.SOA_EVENTBUS_PERIOD.toLong
  val initialDelay = 1000

  /**
    * 基于jdk定时线程池,处理消息轮询发送....
    */
  def startScheduled(): Unit = {
    val schedulerPublisher = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("dapeng-eventbus--scheduler-%d")
        .build)
    schedulerPublisher.scheduleAtFixedRate(() => {
      doPublishMessages()
    }, initialDelay, period, TimeUnit.MILLISECONDS)

  }


  /**
    * fetch message from database , then send to kafka broker
    */
  def doPublishMessages(): Unit = {
    /*if (logger.isDebugEnabled()) {
      logger.debug("begin to publish messages to kafka")
    }*/

    // 消息总条数计数器
    val counter = new AtomicInteger(0)
    // 批量处理, 每次从数据库取出消息的最大数量(window)
    val window = 100
    // 单轮处理的消息计数器, 用于控制循环退出.
    val resultSetCounter = new AtomicInteger(window)


    /**
      * id: 作用是不锁住全表，获取消息时不会影响插入
      *
      * uniqueId:
      */
    do {
      resultSetCounter.set(0)
      withTransaction(dataSource)(conn => {
        eachRow[Row](conn, sql"SELECT * FROM dp_event_lock WHERE id = 1 FOR UPDATE") { lock_row =>
          if (logger.isTraceEnabled()) {
            logger.trace("fetch the dp_event_lock")
          }
        }
        // 没有 for update
        eachRow[EventStore](conn, sql"SELECT * FROM dp_common_event limit ${window}") { event =>
          val result: Int = executeUpdate(conn, sql"DELETE FROM dp_common_event WHERE id = ${event.id}")

          if (result == 1) {
            producer.send(topic, event.id, event.eventBinary)
            counter.incrementAndGet()
          }
          resultSetCounter.incrementAndGet()

        }

        if (counter.get() > 0) {
          logger.info(s" This round : process and publish messages(${counter.get()}) rows to kafka \n")
        }

      })

    } while (resultSetCounter.get() == window)


    if (counter.get() > 0) {
      logger.info(s"end publish messages(${counter.get()}) to kafka")
    }
  }


  //old
  /**
    * fetch message from database , then send to kafka broker
    */
  /*def doPublishMessagesOld(): Unit = {
    /*if (logger.isDebugEnabled()) {
      logger.debug("begin to publish messages to kafka")
    }*/

    // 消息总条数计数器
    val counter = new AtomicInteger(0)
    // 批量处理, 每次从数据库取出消息的最大数量(window)
    val window = 100
    // 单轮处理的消息计数器, 用于控制循环退出.
    val resultSetCounter = new AtomicInteger(window)


    /**
      * id: 作用是不锁住全表，获取消息时不会影响插入
      *
      * uniqueId:
      */
    do {
      resultSetCounter.set(0)

      withTransaction(dataSource)(conn => {

        eachRow(conn, sql"SELECT * FROM dp_event_lock WHERE id = 1 FOR UPDATE") { lock_row =>

          eachRow(conn, sql"") { event =>

          }

        }

        val lock = conn.row[Row](sql"""SELECT * FROM dp_event_lock WHERE id = 1 FOR UPDATE """)

        // 没有 for update
        conn.eachRow[EventStore](sql"SELECT * FROM dp_common_event limit ${window}")(event => {
          val result: Int = conn.executeUpdate(sql"DELETE FROM dp_common_event WHERE id = ${event.id}")

          if (result == 1) {
            producer.send(topic, event.id, event.eventBinary)
            counter.incrementAndGet()
          }

          resultSetCounter.incrementAndGet()

        })

        if (counter.get() > 0) {
          logger.info(s" This round : process and publish messages(${counter.get()}) rows to kafka \n")
        }

      })

    } while (resultSetCounter.get() == window)


    if (counter.get() > 0) {
      logger.info(s"end publish messages(${counter.get()}) to kafka")
    }



    /*while (resultSetCounter.get() == window) {
      resultSetCounter.set(0)
      dataSource.withTransaction[Unit](conn => {

        val lockId: Int = conn.generateKey[Int](sql"INSERT INTO common_event SET unique_id= 0 ,event_type=${lockEventType}")

        //val lockId = insert into xxx
        conn.eachRow[EventStore](sql"SELECT * FROM common_event WHERE id < ${lockId} limit ${window} FOR UPDATE")(event => {
          conn.executeUpdate(sql"DELETE FROM common_event WHERE id = ${event.id}")
          if (event.eventType != lockEventType) {
            producer.send(topic, event.uniqueId, event.eventBinary)

            counter.incrementAndGet()
          }
          resultSetCounter.incrementAndGet()

        })
      })
    }*/


  }*/

}


