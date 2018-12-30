package com.today.eventbus.scheduler

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.github.dapeng.core.helper.MasterHelper
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.today.eventbus.common.SysEnvUtil
import javax.sql.DataSource
import com.today.eventbus.{EventStore, MsgKafkaProducer}
import org.slf4j.LoggerFactory
import com.today.eventbus.scheduler.ScalaSql._
import org.apache.kafka.common.KafkaException
import org.springframework.beans.factory.DisposableBean
import wangzx.scala_commons.sql._

import scala.beans.BeanProperty

/**
  *
  * 描述: 事件定时任务，轮询数据库，发送消息 to kafka
  *
  * @param kafkaHost kafka cluster:127.0.0.1:9091,127.0.0.1:9092
  * @author hz.lei
  * @since 2018年02月28日 下午3:00
  */
class MsgPublishTask(topic: String,
                     kafkaHost: String,
                     tidPrefix: String,
                     dataSource: DataSource) extends DisposableBean {

  private val logger = LoggerFactory.getLogger(classOf[MsgPublishTask])

  /**
    * transId: kafka消息对事务的支持前提，需要每一个生产者实例有不同的事务ID，全局唯一
    */
  private val tid = tidPrefix + UUID.randomUUID().toString
  /**
    * kafka 生产者初始化
    */
  private var producer: MsgKafkaProducer = initTransProducer(kafkaHost, tid)
  /**
    * 轮询间隔时间，根据环境变量指定，默认 300ms
    */
  private val period = SysEnvUtil.SOA_EVENTBUS_PERIOD.toLong
  logger.warn("Kafka producer fetch message period :" + period)
  /**
    * 初始化延迟时间 1s
    */
  private val initialDelay = 1000

  private val logCount = new AtomicInteger(0)
  /**
    * 每轮询100次，log一次日志
    */
  private val logWhileLoop = 100

  private val scheduledCount = new AtomicInteger(0)
  private val scheduledLoop = 500

  /**
    * 初始化 producer 生产者
    *
    * @param host
    * @param tid
    * @return
    */
  private def initTransProducer(host: String, tid: String): MsgKafkaProducer = {
    new MsgKafkaProducer(kafkaHost, tid)
  }


  /**
    * master模式，只有master进行轮询
    * 根据 serviceName 进行节点 master 选举
    */
  @BeanProperty
  var serviceName: String = null

  @BeanProperty
  var versionName = "1.0.0"

  /**
    * 构建定时线程池
    */
  private val schedulerPublisher = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("dapeng-eventbus--scheduler-%d")
      .build)


  /**
    * 基于jdk定时线程池,处理消息轮询发送.... 批量
    * 默认 使用 异步 发送消息模式
    */
  def startScheduled(): Unit = {
    if (serviceName == null) {
      schedulerPublisher.scheduleWithFixedDelay(() => {
        publishMessagesAsyncWithException()
      }, initialDelay, period, TimeUnit.MILLISECONDS)

    } else {
      schedulerPublisher.scheduleWithFixedDelay(() => {
        // log日志多久打印一次
        val logPeriod = scheduledCount.incrementAndGet()

        if (logPeriod == scheduledLoop) {
          logger.info(s"定时线程 logger 间隔: [$logPeriod] 轮记录日志,publishTask节点是否为master:[${MasterHelper.isMaster(serviceName, versionName)}]")
          scheduledCount.set(0)
        }
        //判断为master才执行
        if (MasterHelper.isMaster(serviceName, versionName)) {
          publishMessagesAsyncWithException()
        }
      }, initialDelay, period, TimeUnit.MILLISECONDS)
    }
  }

  /**
    * 执行异步发送消息到 kafka broker, 出现异常会进行捕获
    */
  def publishMessagesAsyncWithException(): Unit = {
    try {
      doPublishMessagesAsync()
    } catch {
      case e: KafkaException => {
        logger.error(s"eventBus: 生产者发送出现KafkaException,准备重启生产者。具体异常: ${e.getMessage}", e)
        producer.closeTransProducer()
        producer = initTransProducer(kafkaHost, tid)
      }
      case e: Exception => logger.error(s"eventBus: 定时轮询线程内出现了异常，已捕获 msg:${e.getMessage}", e)
    }
  }


  /**
    * 采用kafka 异步发送消息的模式，批量轮询数据库消息并发送
    */
  def doPublishMessagesAsync(): Unit = {
    // log日志多久打印一次
    val logPeriod = logCount.incrementAndGet()

    if (logPeriod == logWhileLoop) {
      logger.info(s"[scheduled logger 间隔: $logPeriod]::开始轮询数据库消息....")
    }

    // 消息总条数计数器
    val counter = new AtomicInteger(0)
    // 批量处理, 每次从数据库取出消息的最大数量(window)
    val window = 100
    // 单轮处理的消息计数器, 用于控制循环退出.
    val resultSetCounter = new AtomicInteger(window)


    //id: 作用是不锁住全表，获取消息时不会影响插入 uniqueId
    do {
      resultSetCounter.set(0)
      //事务控制
      withTransaction(dataSource)(conn => {
        val lockRow = row[Row](conn, sql"SELECT * FROM dp_event_lock WHERE id = 1 FOR UPDATE")

        if (logPeriod == logWhileLoop) {
          logger.info(s"[scheduled logger 间隔: $logPeriod]::获得dp_event_lock锁:$lockRow,开始查询消息表dp_common_event")
        }

        val eventMsgs: List[EventStore] = rows[EventStore](conn, sql"SELECT * FROM dp_common_event limit ${window}")
        if (eventMsgs.nonEmpty) {
          /* val idStr: String = eventMsgs.map(_.id).mkString(",")
           executeUpdate(conn, "DELETE FROM dp_common_event WHERE id in (" + idStr + ")")*/

          eventMsgs.map(_.id).foreach(id => {
            executeUpdate(conn, sql"DELETE FROM dp_common_event WHERE id = ${id}")
          })

          producer.batchSend(topic, eventMsgs)
          resultSetCounter.addAndGet(eventMsgs.size)
          counter.addAndGet(eventMsgs.size)
        }
      })

      if (resultSetCounter.get() > 0) {
        logger.info(s" This round : process and publish messages(${resultSetCounter.get()}) rows to kafka \n")
      }

    } while (resultSetCounter.get() == window)


    if (counter.get() > 0) {
      logger.info(s"end publish messages(${counter.get()}) to kafka")
    }

    if (logPeriod == logWhileLoop) {
      //      logger.info(s"[scheduled logger 间隔: ${logPeriod}]:: MsgPublishTask 结束一轮轮询，将计数器置为 0 ")
      logCount.set(0)
    }

  }


  /**
    * 基于jdk定时线程池,处理消息轮询发送....
    */
  def startScheduledSync(): Unit = {
    schedulerPublisher.scheduleWithFixedDelay(() => {
      try {
        doPublishMessagesSync()
      } catch {
        case e: Exception => logger.error(s"eventbus: 定时轮询线程内出现了异常，已捕获 msg:${e.getMessage}", e)
      }

    }, initialDelay, period, TimeUnit.MILLISECONDS)

  }


  /**
    * fetch message from database , then send to kafka broker
    */
  def doPublishMessagesSync(): Unit = {
    val logPeriod = logCount.incrementAndGet()
    if (logPeriod == logWhileLoop) {
      logger.info(s"[scheduled logger 间隔: $logPeriod]::begin to publish messages to kafka")
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
        if (logPeriod == logWhileLoop) {
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

    if (logPeriod == logWhileLoop) {
      //      logger.info(s"[scheduled logger 间隔: ${logPeriod}]::[MsgPublishTask] 结束一轮轮询，将计数器置为 0 ")
      logCount.set(0)
    }
  }

  override def destroy(): Unit = {
    logger.info("<<<<<< Spring容器销毁定时器 schedulerPublisher shutdown >>>>> ")
    schedulerPublisher.shutdown()
  }
}


