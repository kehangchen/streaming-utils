package com.ncr.stream.kafka

import com.ncr.stream.processor.KafkaStreaming
import net.liftweb.json.JsonAST.{JArray, JObject}
import net.liftweb.json.{JsonAST, JsonParser}
import org.apache.kafka.streams.scala.kstream.KStream

class KafkaStreamingExample(path: String) extends KafkaStreaming(path: String) {
  override def businessLogicProcessor(stream: KStream[String, String]): KStream[String, String] = {
    stream.flatMapValues(txt => {
      val raw = JsonParser.parse(txt)
      val metadata = raw \\ "header"
      val JObject(body) = (raw \\ "Body")
      val JArray(a) = body(0).value
      ((a map (_ merge metadata)) map (JsonAST.compactRender(_).replace("header", "metadata"))).toArray.mkString("\n").split("\\n")
    })
  }
}
