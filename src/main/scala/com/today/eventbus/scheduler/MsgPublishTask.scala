package com.today.eventbus.scheduler

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.today.eventbus.common.SysEnvUtil
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
  *         date 2018年02月28日 下午3:00
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

  val period = SysEnvUtil.SOA_EVENTBUS_PERIOD.toLong
  val initialDelay = 1000
  logger.warn("Kafka producer fetch message period :" + period)

  val logCount = new AtomicInteger(0)


  /**
    * 基于jdk定时线程池,处理消息轮询发送.... 批量
    * 默认 使用 异步 发送消息模式
    */
  def startScheduled(): Unit = {
    val schedulerPublisher = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("dapeng-eventbus--scheduler-%d")
        .build)
    schedulerPublisher.scheduleAtFixedRate(() => {
      try {
        doPublishMessagesAsync()
      } catch {
        case e: Exception => logger.error(s"eventbus: 定时轮询线程内出现了异常，已捕获 msg:${e.getMessage}", e)
      }
    }, initialDelay, period, TimeUnit.MILLISECONDS)

  }

  /**
    * 基于jdk定时线程池,处理消息轮询发送....
    */
  def startScheduledSync(): Unit = {
    val schedulerPublisher = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("dapeng-eventbus--scheduler-%d")
        .build)
    schedulerPublisher.scheduleAtFixedRate(() => {
      try {
        doPublishMessagesSync()
      } catch {
        case e: Exception => logger.error(s"eventbus: 定时轮询线程内出现了异常，已捕获 msg:${e.getMessage}", e)
      }

    }, initialDelay, period, TimeUnit.MILLISECONDS)

  }


  /**
    * 批量删除并发送消息
    */
  def doPublishMessagesAsync(): Unit = {
    // log日志多久打印一次
    val logPeriod = logCount.incrementAndGet()

    if (logPeriod == 50) {
      logger.info(s"[scheduled logger 间隔: ${logPeriod}]::begin to publish messages to kafka")
    }

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
        val lockRow = row[Row](conn, sql"SELECT * FROM dp_event_lock WHERE id = 1 FOR UPDATE")

        if (logPeriod == 50) {
          logger.info(s"[scheduled logger 间隔: ${logPeriod}]:: 获得 dp_event_lock 锁,开始查询消息并发送 lock: ${lockRow}")
        }

        val eventMsgs: List[EventStore] = rows[EventStore](conn, sql"SELECT * FROM dp_common_event limit ${window}")
        if (eventMsgs.size > 0) {
          val idStr: String = eventMsgs.map(_.id).mkString(",")
          executeUpdate(conn, "DELETE FROM dp_common_event WHERE id in (" + idStr + ")")

          producer.batchSend(topic, eventMsgs)
        }
      })

      resultSetCounter.incrementAndGet()

      if (counter.get() > 0) {
        logger.info(s" This round : process and publish messages(${counter.get()}) rows to kafka \n")
      }

    } while (resultSetCounter.get() == window)


    if (counter.get() > 0) {
      logger.info(s"end publish messages(${counter.get()}) to kafka")
    }

    if (logPeriod == 50) {
      logger.info(s"[scheduled logger 间隔: ${logPeriod}]:: MsgPublishTask 结束一轮轮询，将计数器置为 0 ")
      logCount.set(0)
    }

  }


  /**
    * fetch message from database , then send to kafka broker
    */
  def doPublishMessagesSync(): Unit = {
    val logPeriod = logCount.incrementAndGet()
    if (logPeriod == 50) {
      logger.info(s"[scheduled logger 间隔: ${logPeriod}]::begin to publish messages to kafka")
    }

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
        val lockRow = row[Row](conn, sql"SELECT * FROM dp_event_lock WHERE id = 1 FOR UPDATE")
        if (logPeriod == 20) {
          logger.info(s"[scheduled logger 间隔: ${logPeriod}]::获得 dp_event_lock 锁,开始查询消息并发送 lock: ${lockRow}")
        }

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

    }
    while (resultSetCounter.get() == window)


    if (counter.get() > 0) {
      logger.info(s"end publish messages(${counter.get()}) to kafka")
    }

    if (logPeriod == 20) {
      logger.info(s"[scheduled logger 间隔: ${logPeriod}]::[MsgPublishTask] 结束一轮轮询，将计数器置为 0 ")
      logCount.set(0)
    }
  }

}


