package com.ncr.stream.kafka

import com.ncr.stream.processor._
import net.liftweb.json._

object KafkaStreamingTest extends App {

  val ks = new KafkaStreaming {
    override def businessLogicProcessor(msg: String): Array[String] = {
      val raw = JsonParser.parse(msg)
      val metadata = raw \\ "header"
      val JObject(body) = (raw \\ "Body")
      val JArray(a) = body(0).value
      ((a map (_ merge metadata)) map (JsonAST.compactRender(_).replace("header", "metadata"))).toArray.mkString("\n").split("\\n")

    }
  }
}
