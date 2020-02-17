package com.ncr.stream.kafka

import com.ncr.stream.processor.KafkaStreaming
import net.liftweb.json._

object KafkaStreamingApp extends App {

  val ks = new KafkaStreaming {
    override def businessLogicProcessor(msg: org.apache.kafka.streams.scala.kstream.KStream[String, String]): org.apache.kafka.streams.scala.kstream.KStream[String, String] = {
      msg.flatMapValues(txt => {
        val raw = JsonParser.parse(txt)
        val metadata = raw \\ "header"
        val JObject(body) = (raw \\ "Body")
        val JArray(a) = body(0).value
        ((a map (_ merge metadata)) map (JsonAST.compactRender(_).replace("header", "metadata"))).toArray.mkString("\n").split("\\n")
      })
    }
  }

  val pro = ks.init("./application.conf")
}