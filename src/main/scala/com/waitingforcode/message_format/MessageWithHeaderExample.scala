package com.waitingforcode.message_format

import java.util.Date

import com.waitingforcode.{ConfigurationMaker, Topics}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import scala.collection.JavaConverters._

object MessageWithHeaderExample {

  def main(args: Array[String]): Unit = {
    val topicName = "messages_headers_topic"
    Topics.recreateTopic(topicName)


    val kafkaProducer = new KafkaProducer[String, String](ConfigurationMaker.configureProducer())
    while (true) {
      val headers: Iterable[Header] = Iterable(new RecordHeader("producer_type", "kafka-playground".getBytes()))
      val message = new ProducerRecord[String, String](topicName, 0, System.currentTimeMillis(),
        s"key${System.currentTimeMillis()}", s"test from ${new Date()}", headers.asJava)
      kafkaProducer.send(message,
        (metadata: RecordMetadata, exception: Exception) => {
          println(s"${message.key} ===> Got=${metadata} and ${exception}")
        })
      kafkaProducer.flush()
    }
  }

}
