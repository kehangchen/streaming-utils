package com.ncr.stream.processor

import java.time.Duration

import com.ncr.stream.config.NCRConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

abstract class KafkaStreaming(path: String) {

  val InputTopicPath = "ncr-config.kafka.streaming.input.topic"
  val OutputTopicPath = "ncr-config.kafka.streaming.output.topic"
  val StreamingConfigPath = "ncr-config.kafka.streaming"

  val config = new NCRConfig(Option(path))

  def init() = {
    val input_topic = config.getString(InputTopicPath)
    val output_topic = config.getString(OutputTopicPath)
    val builder = new StreamsBuilder()
    val from = builder.stream[String, String](input_topic)

    val process = businessLogicProcessor(from.asInstanceOf[org.apache.kafka.streams.scala.kstream.KStream[String, String]])
    //val aggregation = aggregationProcessor(process)
    process.to(output_topic)

    // perform required aggregation here and then push the result to Operation topic
    //aggregationProcessor(process).to("Operation-topic")

    val topology = builder.build()
    System.out.println(topology.describe())
    topology
  }

  def start(topology: Topology) = {
    val streams: KafkaStreams = new KafkaStreams(topology, config.getProperties(StreamingConfigPath))
    streams.cleanUp()
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(1))
    }
  }

  def businessLogicProcessor(stream: org.apache.kafka.streams.scala.kstream.KStream[String, String]): org.apache.kafka.streams.scala.kstream.KStream[String, String]

//  def aggregationProcessor(value: org.apache.kafka.streams.scala.kstream.KStream[String, String]): org.apache.kafka.streams.kstream.KStream[String, java.lang.Long] = {
//    // implement aggregation logic
//    ???
//  }
}