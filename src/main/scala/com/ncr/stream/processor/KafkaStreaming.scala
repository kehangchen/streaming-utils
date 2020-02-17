package com.ncr.stream.processor

import com.ncr.stream.errors._
import java.time.Duration
import java.util.Properties

import com.ncr.stream.config.NCRConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import net.liftweb.json._
import org.apache.kafka.streams.kstream.KStream

abstract class KafkaStreaming {

  def init(path: String) = {
    val conf = new NCRConfig(Option(path))
    val input_topic = conf.getString("ncr-config.kafka.streaming.input.topic")
    val output_topic = conf.getString("ncr-config.kafka.streaming.output.topic")
    val config = conf.getProperties("ncr-config.kafka.streaming")
    val builder = new StreamsBuilder()
    val from = builder.stream[String, String](input_topic)

    val process = businessLogicProcessor(from.asInstanceOf[org.apache.kafka.streams.scala.kstream.KStream[String, String]])
    //val aggregation = aggregationProcessor(process)
    process.to(output_topic)

    // perform required aggregation here and then push the result to Operation topic
    //aggregationProcessor(process).to("Operation-topic")

    val topology = builder.build(config)
    System.out.println(topology.describe())
    val streams: KafkaStreams = new KafkaStreams(topology, config)
    streams.cleanUp()
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(1))
    }
    process
  }

  def businessLogicProcessor(stream: org.apache.kafka.streams.scala.kstream.KStream[String, String]): org.apache.kafka.streams.scala.kstream.KStream[String, String]

//  def aggregationProcessor(value: org.apache.kafka.streams.scala.kstream.KStream[String, String]): org.apache.kafka.streams.kstream.KStream[String, java.lang.Long] = {
//    // implement aggregation logic
//    ???
//  }
}