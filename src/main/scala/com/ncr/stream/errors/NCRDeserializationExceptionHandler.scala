package com.ncr.stream.errors

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse
import org.apache.kafka.streams.processor.ProcessorContext
/*

 */
/**
 * By default, Kafka provides and uses the DefaultProductionExceptionHandler that always fails
 * when these exceptions occur. This exception handler will provide options to allow records
 * with error to be skipped or the process to be stopped.  The error can be size to large and
 * etc.
 */
class NCRDeserializationExceptionHandler extends DeserializationExceptionHandler {
  lazy val log = LoggerFactory.getLogger(getClass)
  var isSkip: Boolean = true
  var sendToKafka: Boolean = false
  var errorTopicName: String = ""
  var dlqProducer: KafkaProducer[String, String] = null

  def handle(context: ProcessorContext, record: ConsumerRecord[Array[Byte], Array[Byte]], exception: Exception):
  DeserializationHandlerResponse = {
    log.error("Exception caught during Deserialization - topic: {}, exception: {}", exception);
    if (sendToKafka && !errorTopicName.isEmpty)
      ???  // dlqProducer.send(...)
    if (isSkip)
      DeserializationHandlerResponse.CONTINUE;
    else
      DeserializationHandlerResponse.FAIL;
  }

  override def configure(map: util.Map[String, _]): Unit = {
    isSkip = map.get("isSkip").asInstanceOf[Boolean]
    sendToKafka = map.get("sendToKafka").asInstanceOf[Boolean]
    errorTopicName = map.get("errorTopicName").asInstanceOf[String]
    dlqProducer = map.get("errorProducer").asInstanceOf[KafkaProducer[String, String]]
  }
}