package com.today.eventbus


import com.today.eventbus.config.KafkaConfigBuilder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}
import org.slf4j.{Logger, LoggerFactory}

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

  def send(topic: String, recordKey: Long, msg: Array[Byte]): Unit = {
    try {
      producer.beginTransaction()

      val metadata: RecordMetadata = producer.send(new ProducerRecord[Long, Array[Byte]](topic, recordKey, msg)).get()

      producer.commitTransaction()

      logger.info(
        s"""in transaction per msg ,send message to broker successful,
           |recordKey: $recordKey, topic: ${metadata.topic}, offset: ${metadata.offset}, partition: ${metadata.partition}""".stripMargin)

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
    */
  def batchSend(topic: String, eventMessage: List[EventStore]): Unit = {
    try {
      producer.beginTransaction()
      eventMessage.foreach((eventStore: EventStore) => {
        val recordKey = eventStore.eventBiz match {
          case Some(value) ⇒ value.hashCode.toLong
          case None ⇒ eventStore.id
        }
        producer.send(new ProducerRecord[Long, Array[Byte]](topic, recordKey, eventStore.eventBinary),
          (metadata: RecordMetadata, exception: Exception) => {
          if (exception != null) {
            logger.error(
              s"""生产者批量发送消息失败当前失败记录,id: ${eventStore.id}, recordKey: $recordKey,
                 | topic: ${metadata.topic}, offset: ${metadata.offset}, partition: ${metadata.partition}""".stripMargin)
            throw exception
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug(s"发送消息,id: ${eventStore.id}, topic: ${metadata.topic}, offset: ${metadata.offset}, partition: ${metadata.partition}")
            }
          }
        })
      })
      producer.commitTransaction()
      logger.info(s"bizProducer:批量发送消息 id:(${eventMessage.map(_.id).toString}),size:[${eventMessage.size}]  to kafka broker successful")
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        logger.error("send message failed,topic: {}", topic)
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

}
