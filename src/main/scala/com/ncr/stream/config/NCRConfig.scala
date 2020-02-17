package com.ncr.stream.config

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

/**
 * A wrapper class of Lightbend to simplify the API for most of our use cases
 *
 * @param fileNameOption
 */
class NCRConfig(fileNameOption: Option[String] = None) {

  val config: com.typesafe.config.Config = {
    if (fileNameOption.getOrElse("").isEmpty()) ConfigFactory.load()
    else ConfigFactory
      .systemProperties()
      .withFallback(ConfigFactory.systemEnvironment()
        .withFallback(ConfigFactory.parseFile(new File(fileNameOption.getOrElse(""))))
        .resolve())
  }

  def envOrElseConfig(name: String): String = {
    scala.util.Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }

  //
  /**
   * returns a com.typesafe.config.Config object for the specified path.
   * This object contains all the original APIs;
   *
   * @param path - json path to a specific node
   * @return - a com.typesafe.config.Config object
   */
  def getConfig(path: String): com.typesafe.config.Config = {
    config.getConfig(path)
  }

  /**
   * returns a string object based on the json path
   *
   * @param path - json path to a specific node
   * @return - a string
   */
  def getString(path: String): String = {
    config.getString(path)
  }

  /**
   * returns a java.util.Properties object that can be directly set in
   * Kafka consumer, producer, or Streaming API
   *
   * @param path - json path to a specific node
   * @return - a java.util.properties object
   */
  def getProperties(path: String): java.util.Properties = {
    import scala.collection.JavaConversions._

    val properties: java.util.Properties = new java.util.Properties()

    config.getConfig(path).entrySet().map({ entry =>
        properties.setProperty(entry.getKey, entry.getValue.unwrapped().toString)
    })

    properties
  }
}