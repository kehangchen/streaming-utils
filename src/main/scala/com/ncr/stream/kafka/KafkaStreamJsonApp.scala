package com.ncr.stream.kafka

import com.ncr.stream.errors._
import java.time.Duration
import java.util.Properties
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import net.liftweb.json._

object KafkaStreamJsonApp extends App {

  val config: Properties = {
    val p = new Properties()
    // this parameter must be unique within a Kafka cluster
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, classOf[NCRDeserializationExceptionHandler].getName)
    p.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, classOf[NCRProductionExceptionHandler].getName)
    p.put(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000)
    p.put(StreamsConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000)
    p
  }

  val builder = new StreamsBuilder()
  builder.stream[String, String]("kafka-streams-input")
    .flatMapValues(textLine => businessLogicProcessor(textLine))
    .to("kafka-streams-output")

  val topology = builder.build(config)
  System.out.println(topology.describe())
  val streams: KafkaStreams = new KafkaStreams(topology, config)
  streams.cleanUp()
  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread { streams.close(Duration.ofSeconds(1)) }

  def businessLogicProcessor(msg: String): Array[String] = {
    val raw = JsonParser.parse(msg)
    val metadata = raw \\ "header"
    val JObject(body) = (raw \\ "Body")
    val JArray(a) = body(0).value
    ((a map (_ merge metadata)) map (JsonAST.compactRender(_).replace("header", "metadata"))).toArray.mkString("\n").split("\\n")
  }
}