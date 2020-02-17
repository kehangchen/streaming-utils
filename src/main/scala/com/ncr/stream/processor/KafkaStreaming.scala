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

  def init = {
    val configFile = "/Volumes/Macintosh HD2/Users/kehangchen/Documents/mes/scala/streaming-utils/application.conf"
    val conf = new NCRConfig(Option(configFile))
    val input_topic = conf.getString("ncr-config.kafka.streaming.input.topic")
    val output_topic = conf.getString("ncr-config.kafka.streaming.output.topic")
    val config = conf.getProperties("ncr-config.kafka.streaming")

//    val config: Properties = {
//      val p = new Properties()
//      // this parameter must be unique within a Kafka cluster
//      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-application")
//      val bootstrapServers = "localhost:9092"
//      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
//      p.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, classOf[NCRDeserializationExceptionHandler].getName)
//      p.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, classOf[NCRProductionExceptionHandler].getName)
//      p.put(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000)
//      p.put(StreamsConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000)
//      p
//    }

    val builder = new StreamsBuilder()
    val from = builder.stream[String, String](input_topic)
    //val process = from.flatMapValues(textLine => businessLogicProcessor(textLine))
    val process = businessLogicProcessor(from.asInstanceOf[org.apache.kafka.streams.scala.kstream.KStream[String, String]])
    process.to(output_topic)

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
}