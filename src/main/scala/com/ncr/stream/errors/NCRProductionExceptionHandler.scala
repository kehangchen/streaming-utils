package com.ncr.stream.errors

import java.util

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.errors.ProductionExceptionHandler
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.producer.KafkaProducer
/*

 */
/**
 * By default, Kafka provides couple default error handlers for de/serialization exceptions.  However,
 * they do not provide other features besides skip the record or stop the process in two seperated
 * handlers.  This handler combines both options above based on configuration and also allow to
 * produce the record to a Kafka topic for other process to handle the error records if it is
 * chosen to do so.
 */
class NCRProductionExceptionHandler extends ProductionExceptionHandler {

  lazy val log = LoggerFactory.getLogger(getClass)
  var isSkip: Boolean = true
  var sendToKafka: Boolean = false
  var errorTopicName: String = ""
  var dlqProducer: KafkaProducer[String, String] = null

  def handle(record: ProducerRecord[Array[Byte], Array[Byte]], exception: Exception):
    ProductionExceptionHandlerResponse = {
    log.error("Exception caught during Deserialization, sending to the dead queue topic - " +
      "topic: {}, partition: {}, key: {}",
      record.topic(), record.partition(), record.key(), exception);
    if (sendToKafka && !errorTopicName.isEmpty)
      ???  // dlqProducer.send(...)
    if (isSkip)
      ProductionExceptionHandlerResponse.CONTINUE;
    else
      ProductionExceptionHandlerResponse.FAIL;
  }

  override def configure(map: util.Map[String, _]): Unit = {
    isSkip = map.get("isSkip").asInstanceOf[Boolean]
    sendToKafka = map.get("sendToKafka").asInstanceOf[Boolean]
    errorTopicName = map.get("errorTopicName").asInstanceOf[String]
    dlqProducer = map.get("errorProducer").asInstanceOf[KafkaProducer[String, String]]
  }
}