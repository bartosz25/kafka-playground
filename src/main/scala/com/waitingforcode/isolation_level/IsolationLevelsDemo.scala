package com.waitingforcode.isolation_level

import java.time.Duration

import com.waitingforcode.Topics

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object IsolationLevelsDemo extends App {
  val TopicName = "isolation_level_test"
  Topics.recreateTopic(TopicName)

  new Thread(Producer).start()
  new Thread(new IsolationLevelConsumer("read_committed")).start()
//  new Thread(new IsolationLevelConsumer("read_uncommitted")).start()

  Thread.sleep(30000L)
}


object Producer extends Runnable {

  private val commonConfig: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:29092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )
  private val transactionalConfigFailure: Map[String, AnyRef] = Map(
    "transactional.id" -> "isolation_level_demo_fail",
    "enable.idempotence" -> "true"
  ) ++ commonConfig
  private val transactionalConfigSuccess: Map[String, AnyRef] = Map(
    "transactional.id" -> "isolation_level_demo_success",
    "enable.idempotence" -> "true"
  ) ++ commonConfig
  private val transactionalProducerFailure = new KafkaProducer[String, String](transactionalConfigFailure.asJava)
  private val nonTransactionalProducer = new KafkaProducer[String, String](commonConfig.asJava)
  private val transactionalProducerSuccess = new KafkaProducer[String, String](transactionalConfigSuccess.asJava)
  override def run(): Unit = {
    transactionalProducerFailure.initTransactions()
    transactionalProducerSuccess.initTransactions()
    while (true) {
      transactionalProducerFailure.beginTransaction()
      val abortedRecords = (1 to 15).map(id => s"aborted_${id}")
      abortedRecords.zipWithIndex.foreach {
        case (value, index) => {
          if (index < 10) {
            transactionalProducerFailure.send(new ProducerRecord[String, String](IsolationLevelsDemo.TopicName, value))
          } else if (index == 10) {
            transactionalProducerFailure.flush()
            transactionalProducerFailure.abortTransaction()
          }
        }
      }

      val nonTransactionalRecords = (1 to 15).map(id => s"non_transactional_${id}")
      nonTransactionalRecords.foreach(value => {
        nonTransactionalProducer.send(new ProducerRecord[String, String](IsolationLevelsDemo.TopicName, value))
      })

      transactionalProducerSuccess.beginTransaction()
      val successfulTransactionalRecords = (1 to 15).map(id => s"successful_${id}")
      successfulTransactionalRecords.foreach(value => {
        transactionalProducerSuccess.send(new ProducerRecord[String, String](IsolationLevelsDemo.TopicName, value))
      })
      transactionalProducerSuccess.commitTransaction()


      Thread.sleep(10000L)
    }
  }
}

class IsolationLevelConsumer(isolationLevel: String) extends Runnable {

  private val config: Map[String, AnyRef] = Map(
    "group.id" -> s"consumer_${isolationLevel}_${System.currentTimeMillis()}",
    "bootstrap.servers" -> "localhost:29092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "isolation.level" -> isolationLevel,
    "auto.offset.reset" -> "latest" // "earliest"
  )
  import scala.collection.JavaConverters._
  private val consumer = new KafkaConsumer[String, String](config.asJava)
  override def run(): Unit = {
    consumer.subscribe(Seq(IsolationLevelsDemo.TopicName).asJava)
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(3L))
      records.asScala.foreach(record => {
        println(s"[${isolationLevel}]=${record.value()}")
      })
      Thread.sleep(5000L)
    }
  }
}