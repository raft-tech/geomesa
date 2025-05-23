/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.{Cluster, TopicPartition}
import org.geotools.api.data.{DataStore, Query}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.planning.QueryRunner.QueryResult
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.kafka.versions.KafkaConsumerVersions
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.MessageTypes
import org.locationtech.geomesa.lambda.stream.{OffsetManager, TransientStore}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.Flushable
import java.time.Clock
import java.util.{Collections, Properties, UUID}
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

class KafkaStore(
    ds: DataStore,
    val sft: SimpleFeatureType,
    authProvider: Option[AuthorizationsProvider],
    offsetManager: OffsetManager,
    config: LambdaConfig)
   (implicit clock: Clock = Clock.systemUTC()
   ) extends TransientStore with Flushable with LazyLogging {

  private val topic = LambdaDataStore.topic(sft, config.zkNamespace)

  private val producer = KafkaStore.producer(sft, config.producerConfig)

  private val cache = new KafkaFeatureCache(ds, sft, offsetManager, topic, config.expiry, config.persistBatchSize)

  private val serializer = {
    // use immutable so we can return query results without copying or worrying about user modification
    // use lazy so that we don't create lots of objects that get replaced/updated before actually being read
    val options = SerializationOptions.builder.withUserData.withoutFidHints.immutable.`lazy`.build
    KryoFeatureSerializer(sft, options)
  }

  private val interceptors = QueryInterceptorFactory(ds)

  private val queryRunner = new KafkaQueryRunner(cache, authProvider, interceptors)

  private val loader = {
    val consumers = KafkaStore.consumers(config.consumerConfig, topic, offsetManager, config.consumers, cache.partitionAssigned)
    val frequency = KafkaStore.LoadIntervalProperty.toDuration.get.toMillis
    new KafkaCacheLoader(consumers, topic, frequency, config.offsetCommitInterval, serializer, cache)
  }

  override def createSchema(): Unit = {
    val props = new Properties()
    config.producerConfig.foreach { case (k, v) => props.put(k, v) }

    WithClose(AdminClient.create(props)) { admin =>
      if (admin.listTopics().names().get.contains(topic)) {
        logger.warn(s"Topic [$topic] already exists - it may contain stale data")
      } else {
        val replication = SystemProperty("geomesa.kafka.replication").option.map(_.toInt).getOrElse(1)
        val newTopic = new NewTopic(topic, config.partitions, replication.toShort)
        admin.createTopics(Collections.singletonList(newTopic)).all().get
      }
    }
  }

  override def removeSchema(): Unit = {
    offsetManager.deleteOffsets(topic)
    val props = new Properties()
    config.producerConfig.foreach { case (k, v) => props.put(k, v) }

    WithClose(AdminClient.create(props)) { admin =>
      if (admin.listTopics().names().get.contains(topic)) {
        admin.deleteTopics(Collections.singletonList(topic)).all().get
      } else {
        logger.warn(s"Topic [$topic] does not exist, can't delete it")
      }
    }
  }

  override def read(
      filter: Option[Filter] = None,
      transforms: Option[Array[String]] = None,
      hints: Option[Hints] = None,
      explain: Explainer = new ExplainLogging): QueryResult = {
    val query = new Query()
    filter.foreach(query.setFilter)
    transforms.foreach(query.setPropertyNames(_: _*))
    hints.foreach(query.setHints)
    queryRunner.runQuery(sft, query, explain)
  }

  override def write(original: SimpleFeature): Unit = {
    val feature = GeoMesaFeatureWriter.featureWithFid(original)
    val key = KafkaStore.serializeKey(clock.millis(), MessageTypes.Write)
    producer.send(new ProducerRecord(topic, key, serializer.serialize(feature)))
    logger.trace(s"Wrote feature to [$topic]: $feature")
  }

  override def delete(original: SimpleFeature): Unit = {
    // send a message to delete from all transient stores
    val feature = GeoMesaFeatureWriter.featureWithFid(original)
    val key = KafkaStore.serializeKey(clock.millis(), MessageTypes.Delete)
    producer.send(new ProducerRecord(topic, key, serializer.serialize(feature)))
  }

  override def persist(): Unit = cache.persist()

  override def flush(): Unit = producer.flush()

  override def close(): Unit = {
    CloseWithLogging(loader)
    CloseWithLogging(interceptors)
    CloseWithLogging(cache)
  }
}

object KafkaStore {

  val SimpleFeatureSpecConfig = "geomesa.sft.spec"

  val LoadIntervalProperty: SystemProperty = SystemProperty("geomesa.lambda.load.interval", "100ms")

  object MessageTypes {
    val Write:  Byte = 0
    val Delete: Byte = 1
  }

  @deprecated("Replaced with LambdaDataStore.topic")
  def topic(ns: String, sft: SimpleFeatureType): String = LambdaDataStore.topic(sft, ns)

  @deprecated("Does not return correct topic if topic is overridden in the feature type - replaced with LambdaDataStore.topic")
  def topic(ns: String, typeName: String): String = s"${ns}_$typeName".replaceAll("[^a-zA-Z0-9_\\-]", "_")

  def producer(sft: SimpleFeatureType, connect: Map[String, String]): Producer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig._
    val props = new Properties()
    // set some defaults but allow them to be overridden
    props.put(ACKS_CONFIG, "1") // mix of reliability and performance
    props.put(RETRIES_CONFIG, Int.box(3))
    props.put(LINGER_MS_CONFIG, Int.box(3)) // helps improve batching at the expense of slight delays in write
    props.put(PARTITIONER_CLASS_CONFIG, classOf[FeatureIdPartitioner].getName)
    props.put(SimpleFeatureSpecConfig, SimpleFeatureTypes.encodeType(sft, includeUserData = false))
    connect.foreach { case (k, v) => props.put(k, v) }
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  def consumer(connect: Map[String, String], group: String): Consumer[Array[Byte], Array[Byte]] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig._
    val props = new Properties()
    props.put(GROUP_ID_CONFIG, group)
    connect.foreach { case (k, v) => props.put(k, v) }
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  // creates a consumer and sets to the latest offsets
  private [kafka] def consumers(connect: Map[String, String],
                                topic: String,
                                manager: OffsetManager,
                                parallelism: Int,
                                callback: (Int, Long) => Unit): Seq[Consumer[Array[Byte], Array[Byte]]] = {
    require(parallelism > 0, "Parallelism must be greater than 0")

    val group = UUID.randomUUID().toString

    Seq.fill(parallelism) {
      val consumer = KafkaStore.consumer(connect, group)
      val listener = new OffsetRebalanceListener(consumer, manager, callback)
      KafkaConsumerVersions.subscribe(consumer, topic, listener)
      consumer
    }
  }

  private [kafka] def serializeKey(time: Long, action: Byte): Array[Byte] = {
    val result = Array.ofDim[Byte](9)

    result(0) = ((time >> 56) & 0xff).asInstanceOf[Byte]
    result(1) = ((time >> 48) & 0xff).asInstanceOf[Byte]
    result(2) = ((time >> 40) & 0xff).asInstanceOf[Byte]
    result(3) = ((time >> 32) & 0xff).asInstanceOf[Byte]
    result(4) = ((time >> 24) & 0xff).asInstanceOf[Byte]
    result(5) = ((time >> 16) & 0xff).asInstanceOf[Byte]
    result(6) = ((time >> 8)  & 0xff).asInstanceOf[Byte]
    result(7) = (time & 0xff        ).asInstanceOf[Byte]
    result(8) = action

    result
  }

  private [kafka] def deserializeKey(key: Array[Byte]): (Long, Byte) = (ByteArrays.readLong(key), key(8))

  private [kafka] class OffsetRebalanceListener(consumer: Consumer[Array[Byte], Array[Byte]],
                                                manager: OffsetManager,
                                                callback: (Int, Long) => Unit)
      extends ConsumerRebalanceListener with LazyLogging {

    override def onPartitionsRevoked(topicPartitions: java.util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsAssigned(topicPartitions: java.util.Collection[TopicPartition]): Unit = {
      import scala.collection.JavaConverters._

      // ensure we have queues for each partition
      // read our last committed offsets and seek to them
      topicPartitions.asScala.foreach { tp =>

        // seek to earliest existing offset and return the offset
        def seekToBeginning(): Long = {
          KafkaConsumerVersions.seekToBeginning(consumer, tp)
          consumer.position(tp) - 1
        }

        val lastRead = manager.getOffset(tp.topic(), tp.partition())

        KafkaConsumerVersions.pause(consumer, tp)

        val offset = if (lastRead < 0) { seekToBeginning() } else {
          try { consumer.seek(tp, lastRead + 1); lastRead } catch {
            case NonFatal(e) =>
              logger.warn(s"Error seeking to initial offset: [${tp.topic}:${tp.partition}:$lastRead]" +
                  s", seeking to beginning: $e")
              seekToBeginning()
          }
        }
        callback.apply(tp.partition, offset)

        KafkaConsumerVersions.resume(consumer, tp)
      }
    }
  }

  /**
    * Ensures that updates to a given feature go to the same partition, so that they maintain order
    */
  class FeatureIdPartitioner extends Partitioner {

    private var serializer: KryoFeatureSerializer = _

    private val features = new ThreadLocal[KryoBufferSimpleFeature]() {
      override def initialValue(): KryoBufferSimpleFeature = serializer.getReusableFeature
    }

    override def partition(
        topic: String,
        key: scala.Any,
        keyBytes: Array[Byte],
        value: scala.Any,
        valueBytes: Array[Byte],
        cluster: Cluster): Int = {
      val numPartitions = cluster.partitionsForTopic(topic).size
      if (numPartitions < 2) { 0 } else {
        val feature = features.get
        feature.setBuffer(valueBytes)
        Math.abs(MurmurHash3.stringHash(feature.getID)) % numPartitions
      }
    }

    override def configure(configs: java.util.Map[String, _]): Unit = {
      val spec = configs.get(SimpleFeatureSpecConfig) match {
        case s: String => s
        case s => throw new IllegalStateException(s"Invalid spec config for $SimpleFeatureSpecConfig: $s")
      }
      val options = SerializationOptions.builder.immutable.`lazy`.build
      serializer = KryoFeatureSerializer(SimpleFeatureTypes.createType("", spec), options)
    }

    override def close(): Unit = {}
  }
}
