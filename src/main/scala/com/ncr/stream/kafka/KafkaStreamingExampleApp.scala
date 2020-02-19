package com.ncr.stream.kafka

import com.ncr.stream.processor.KafkaStreaming
import net.liftweb.json._

object KafkaStreamingExampleApp extends App {

  val ks = new KafkaStreamingExample("./application.conf")
  val pro = ks.start(ks.init())
}
