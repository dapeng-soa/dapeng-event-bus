package com.today.eventbus


import java.sql.Connection

import com.today.eventbus.config.KafkaConfigBuilder
import com.today.eventbus.scheduler.ScalaSql.executeUpdate
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}
import org.slf4j.{Logger, LoggerFactory}
import wangzx.scala_commons.sql._

/**
  *
  * 描述: event kafka producer
  *
  * @param serverHost kafka cluster:127.0.0.1:9091,127.0.0.1:9092
  * @author hz.lei
  * @since 2018年02月28日 下午3:17
  */
class MsgKafkaProducer(serverHost: String, transactionId: String) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MsgKafkaProducer])

  initTransProducer(transactionId)

  var producer: KafkaProducer[Long, Array[Byte]] = _

  /**
    * 事务控制的 producer
    *
    * @return
    */
  private def initTransProducer(transId: String): Unit = {
    val builder = KafkaConfigBuilder.defaultProducer
    val properties = builder
      .withKeySerializer(classOf[LongSerializer])
      .withValueSerializer(classOf[ByteArraySerializer])
      .bootstrapServers(serverHost)
      .withTransactions(transId)
      .withIdempotence(true) //幂等性保证
      .build

    producer = new KafkaProducer[Long, Array[Byte]](properties)
    producer.initTransactions()

    logger.warn("kafka transaction producer is started successful with transID: " + transId)
  }

  def send(topic: String, id: Long, msg: Array[Byte]): Unit = {
    try {
      producer.beginTransaction()

      val metadata: RecordMetadata = producer.send(new ProducerRecord[Long, Array[Byte]](topic, id, msg)).get()

      producer.commitTransaction()

      logger.info(
        s"""in transaction per msg ,send message to broker successful,
        id: ${id}, topic: ${metadata.topic}, offset: ${metadata.offset}, partition: ${metadata.partition}""")

    } catch {
      case e: Exception =>
        producer.abortTransaction()
        logger.error(e.getMessage, e)
        logger.error("send message failed,topic: {}", topic)
        throw e
    }
  }

  /**
    * batch to send message , if one is failed ,all batch message will  rollback.
    *
    * @param topic
    * @param eventMessage
    */
  def batchSend(topic: String, eventMessage: List[EventStore], conn: Connection): Unit = {
    var loggerTopic = topic
    try {
      producer.beginTransaction()
      eventMessage.foreach((eventStore: EventStore) => {
        val topicNew = if (eventStore.eventTopic != null) eventStore.eventTopic else topic
        loggerTopic = topicNew
        val partition = generatePartition(topicNew, eventStore)
        producer.send(new ProducerRecord[Long, Array[Byte]](topicNew, partition, eventStore.id, eventStore.eventBinary), (metadata: RecordMetadata, exception: Exception) => {
          if (exception != null) {
            logger.error(
              s"""msgKafkaProducer: batch  send message to broker failed in transaction per msg ,id: ${eventStore.id}, topic: ${metadata.topic}, offset: ${metadata.offset}, partition: ${metadata.partition}""")
            throw exception
          } else {
            executeUpdate(conn, sql"DELETE FROM dp_event_info WHERE id = ${eventStore.id}")
            logger.debug(s"发送消息,id: ${eventStore.id}, topic: ${metadata.topic}, offset: ${metadata.offset}, partition: ${metadata.partition}")
          }
        })
      })
      producer.commitTransaction()

      logger.info(s"bizProducer:批量发送消息 id:(${eventMessage.map(_.id).toString}),size:[${eventMessage.size}]  to kafka broker successful")
    } catch {
      case e: Exception =>
        logger.error("send message failed,topic: {}", loggerTopic)
        logger.error(e.getMessage, e)
        try {
          producer.abortTransaction()
        } catch {
          case e: Exception =>
            logger.error(s"abortTransaction Error: ${e.getMessage}", e)
            throw e
        }
        throw e
    }
  }

  /**
    * 关闭生产者
    */
  def closeTransProducer(): Unit = {
    producer.close()
    logger.info("<======== shutdown KafkaProducer successful =========>")
  }

  def generatePartition(topic: String, eventStore: EventStore): Int = {
    val numPartitions = producer.partitionsFor(topic).size()
    if (eventStore.eventKey == null) {
      Math.abs(eventStore.id.hashCode() % numPartitions)
    } else {
      Math.abs(eventStore.eventKey.hashCode() % numPartitions)
    }
  }
}
