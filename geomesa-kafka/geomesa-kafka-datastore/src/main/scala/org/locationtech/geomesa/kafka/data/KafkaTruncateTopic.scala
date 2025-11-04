/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, OffsetSpec, RecordsToDelete}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.locationtech.geomesa.kafka.data.KafkaTruncateTopic.{CLEANUP_POLICY, CLEANUP_POLICY_DELETE}

import java.util
import java.util.Collections
import java.util.concurrent.ExecutionException
import scala.collection.JavaConverters._

class KafkaTruncateTopic(private val admin: Admin) extends LazyLogging {
  /**
   * Truncation of a Kafka topic.
   *
   * @param topic the topic name to truncate
   * @param force whether to force truncation even without delete cleanup policy
   */
  def truncate(topic: String, force: Boolean = false): Unit = {
    val topicPartitions = getTopicPartitions(topic)
    val earliestOffsets = getEarliestOffsets(topicPartitions)
    val latestOffsets = getLatestOffsets(topicPartitions)

    val count = numberOfMessages(topic, earliestOffsets, latestOffsets)

    if (count == 0) {
      logger.info(s"truncate: topic: $topic has no messages to delete")
      return
    }

    logger.info(s"truncate: topic: $topic has $count messages to be deleted over ${topicPartitions.size} partitions")

    val deleteCleanupPolicy = hasDeleteCleanupPolicy(topic)

    if (!deleteCleanupPolicy && !force) {
      logger.error(s"truncate: topic: $topic does not have a delete cleanup policy with $CLEANUP_POLICY_DELETE, use 'force' which will add and then remove the 'delete' cleanup policy")
      return
    }

    if (deleteCleanupPolicy && force) {
      logger.warn(s"truncate: topic: $topic has a delete cleanup policy, 'force' is not necessary")
    }

    try {
      if (!deleteCleanupPolicy) {
        addDeleteCleanupPolicy(topic)
      }
      logger.info(s"truncate: topic: $topic starting")
      deleteRecords(latestOffsets)
      logger.info(s"truncate: topic: $topic completed")
    } finally {
      if (!deleteCleanupPolicy) {
        removeDeleteCleanupPolicy(topic)
      }
    }
  }

  private def numberOfMessages(topic: String,
                               earliestOffsets: Map[TopicPartition, ListOffsetsResultInfo],
                               latestOffsets: Map[TopicPartition, ListOffsetsResultInfo]): Long =
    getTopicPartitions(topic).map { tp =>
      val earliestOffset = earliestOffsets(tp).offset()
      val latestOffset = latestOffsets(tp).offset()
      latestOffset - earliestOffset
    }.sum

  private def deleteRecords(latestOffsets: Map[TopicPartition, ListOffsetsResultInfo]): Unit =
    try {
      val recordsToDelete = generateRecordsToDelete(latestOffsets)
      admin.deleteRecords(recordsToDelete.asJava).all().get()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("the deleteRecords operation was interrupted, aborting; it may have still completed.")
      case e: ExecutionException =>
        throw convertExecutionException(e)
    }

  private def generateRecordsToDelete(latestOffsets: Map[TopicPartition, ListOffsetsResultInfo]): Map[TopicPartition, RecordsToDelete] =
    latestOffsets.map {
      case (tp, info) => tp -> RecordsToDelete.beforeOffset(info.offset())
    }

  private def getLatestOffsets(partitions: List[TopicPartition]): Map[TopicPartition, ListOffsetsResultInfo] = {
    val input = partitions.map(tp => tp -> OffsetSpec.latest()).toMap
    getOffsets(input)
  }

  private def getEarliestOffsets(partitions: List[TopicPartition]): Map[TopicPartition, ListOffsetsResultInfo] = {
    val input = partitions.map(tp => tp -> OffsetSpec.earliest()).toMap
    getOffsets(input)
  }

  private def getOffsets(offsetSpecs: Map[TopicPartition, OffsetSpec]): Map[TopicPartition, ListOffsetsResultInfo] =
    try {
      admin.listOffsets(offsetSpecs.asJava).all().get().asScala.toMap
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("listOffsets operation interrupted, deleteRecords will not executed.")
      case e: ExecutionException =>
        throw convertExecutionException(e)
    }

  private def getTopicPartitions(topic: String): List[TopicPartition] =
    try {
      val topicInfo = admin.describeTopics(Collections.singleton(topic)).allTopicNames().get().get(topic)
      topicInfo.partitions().asScala.map(info => new TopicPartition(topic, info.partition())).toList
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("describeTopics operation interrupted, deleteRecords will not executed.")
      case e: ExecutionException =>
        throw convertExecutionException(e)
    }

  private def hasDeleteCleanupPolicy(topicName: String): Boolean =
    try {
      val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName)
      val configsResult = admin.describeConfigs(Collections.singleton(configResource))
      val config = configsResult.all().get().get(configResource)

      config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value().contains(CLEANUP_POLICY_DELETE)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("describeTopics operation interrupted, deleteRecords will not executed.")
      case e: ExecutionException =>
        throw convertExecutionException(e)
    }

  private def alterConfigOpColl(alterConfigOp: AlterConfigOp): util.Collection[AlterConfigOp] =
    Collections.singleton(alterConfigOp)

  private def addDeleteCleanupPolicy(topic: String): Unit =
    try {
      val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val alterConfigOp = new AlterConfigOp(new ConfigEntry(CLEANUP_POLICY, CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.APPEND)
      val configs: util.Map[ConfigResource, util.Collection[AlterConfigOp]] = Map(configResource -> alterConfigOpColl(alterConfigOp)).asJava
      admin.incrementalAlterConfigs(configs).all().get()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("incrementalAlterConfigs operation interrupted, ...")
      case e: ExecutionException =>
        throw convertExecutionException(e)
    }

  private def removeDeleteCleanupPolicy(topic: String): Unit =
    try {
      val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val alterConfigOp = new AlterConfigOp(new ConfigEntry(CLEANUP_POLICY, CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.SUBTRACT)
      val configs: util.Map[ConfigResource, util.Collection[AlterConfigOp]] = Map(configResource -> alterConfigOpColl(alterConfigOp)).asJava
      admin.incrementalAlterConfigs(configs).all().get()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("incrementalAlterConfigs operation interrupted, was not able to remove the 'delete' cleanup.policy.")
      case e: ExecutionException =>
        throw convertExecutionException(e)
    }

  /**
   * Convert ExecutionException to RuntimeException, preserving the underlying cause.
   */
  private def convertExecutionException(e: ExecutionException): RuntimeException = {
    val cause = Option(e.getCause).getOrElse(e)
    new RuntimeException(cause.getMessage, cause)
  }
}

object KafkaTruncateTopic {
  val CLEANUP_POLICY = "cleanup.policy"
  val CLEANUP_POLICY_DELETE = "delete"

  def apply(admin: Admin): KafkaTruncateTopic = new KafkaTruncateTopic(admin)
}