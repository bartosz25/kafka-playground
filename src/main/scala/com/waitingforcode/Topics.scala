package com.waitingforcode

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

import scala.collection.JavaConverters._

object Topics {

  def recreateTopic(topicName: String, partitions: Int = 1, replicationFactor: Short = 1) = {
    val adminClient = AdminClient.create(ConfigurationMaker.configureAdmin())

    def createTopic(deleteResult: Void, deleteActionException: Throwable) = {
      assert(deleteActionException == null || deleteActionException.isInstanceOf[UnknownTopicOrPartitionException],
        "Delete action should execute successfully or, at worse, should return UnknownTopicOrPartitionException if " +
          "the topic doesn't exist"
      )
      println(s"Creating ${topicName}")
      val newTopicResult = adminClient.createTopics(Seq(
        new NewTopic(topicName, partitions, replicationFactor)
      ).asJava)
    }

    println(s"Deleting ${topicName}")
    val deleteResult = adminClient.deleteTopics(Seq(topicName).asJava)
    // As for complete, 1 minute should be enough
    val cleanResult = deleteResult.all().whenComplete(createTopic)
    while (!cleanResult.isDone) {}

    adminClient.close()
  }

}
