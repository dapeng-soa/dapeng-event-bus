package com.today.eventbus

import java.util

import com.today.eventbus.config.KafkaConfigBuilder
import com.today.eventbus.dao.EventStore
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * 描述: event kafka producer
  *
  * @author hz.lei
  * @date 2018年02月28日 下午3:17
  */
case class MsgKafkaProducer(serverHost: String, transactionId: String) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MsgKafkaProducer])
  /**
    * 127.0.0.1:9091,127.0.0.1:9092
    */
  val kafkaConnect = serverHost
  val transId = transactionId
  initTransProducer(transId)

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
      .bootstrapServers(kafkaConnect)
      .withTransactions(transId)
      .build

    producer = new KafkaProducer[Long, Array[Byte]](properties)
    producer.initTransactions()
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
  def batchSend(topic: String, eventMessage: util.List[EventStore]): Unit = {
    try {
      producer.beginTransaction()
      eventMessage.forEach((eventStore: EventStore) => {
        producer.send(new ProducerRecord[Long, Array[Byte]](topic, eventStore.id, eventStore.eventBinary), (metadata: RecordMetadata, exception: Exception) => {
          logger.info(
            s"""in transaction per msg ,send message to broker successful,
        id: ${eventStore.id}, topic: ${metadata.topic}, offset: ${metadata.offset}, partition: ${metadata.partition}""")
        })
      })
      producer.commitTransaction()
    } catch {
      case e: Exception =>
        producer.abortTransaction()
        logger.error(e.getMessage, e)
        logger.error("send message failed,topic: {}", topic)
        throw e
    }
  }

}
