package com.waitingforcode.idempotent_producer

import com.waitingforcode.{ConfigurationMaker, Topics}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object IdempotentProducerExample {
  def main(args: Array[String]): Unit = {
    val topicName = "idempotent_producer_topic"
    Topics.recreateTopic(topicName)

    val idempotentProducer = new KafkaProducer[String, String](ConfigurationMaker.configureProducer(
      extras = Map("enable.idempotence" -> "true", "max.in.flight.requests.per.connection" -> "5",
        ProducerConfig.RETRIES_CONFIG -> "3")
    ))

    (1 to 3).foreach(number => {
      idempotentProducer.send(new ProducerRecord[String, String](topicName, s"value${number}"),
        new Callback() {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            println(s"${number} ===> Got=${metadata} and ${exception}")
          }
        })
      idempotentProducer.flush()
    })
  }
}
