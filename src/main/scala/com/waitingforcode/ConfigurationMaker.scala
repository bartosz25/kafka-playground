package com.waitingforcode

import java.util.Properties

object ConfigurationMaker {

  private val ServerAddress = "localhost:29092"

  def configureProducer(clientId: String = s"auto_${System.currentTimeMillis()}",
                        keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
                        valueSerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
                        extras: Map[String, String] = Map.empty): Properties = {
    val configuration = new Properties()
    configuration.setProperty("client.id", clientId)
    configuration.setProperty("bootstrap.servers", ServerAddress)
    configuration.setProperty("key.serializer", keySerializer)
    configuration.setProperty("value.serializer", valueSerializer)
    extras.foreach {
      case (configKey, configValue) => configuration.setProperty(configKey, configValue)
    }
    configuration
  }

  def configureAdmin(): Properties = {
    val configuration = new Properties()
    configuration.setProperty("bootstrap.servers", ServerAddress)
    configuration
  }

}
