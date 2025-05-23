/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.convert.redis

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EnrichmentCacheFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import redis.clients.jedis.Jedis

import java.util.{Collections, ServiceLoader}

class MockRedis extends Jedis {
  var count = 0
  override def hgetAll(key: String): java.util.Map[String, String] = {
    if(count == 0) {
      count += 1
      Collections.singletonMap("foo", "bar")
    } else {
      Collections.singletonMap("foo", "baz")
    }
  }

  override def close(): Unit = {}
}

@RunWith(classOf[JUnitRunner])
class RedisEnrichmentCacheTest extends Specification {

  import scala.collection.JavaConverters._

  sequential

  "Redis enrichment cache" should {
    "work" >> {
      val redis = new MockRedis
      val connBuilder = new RedisConnectionBuilder {
        override def getConn: Jedis = redis
        override def close(): Unit = {}
      }

      val cache = new RedisEnrichmentCache(connBuilder, -1, true)
      val res = cache.get(Array("1", "foo")).asInstanceOf[String]
      res must be equalTo "bar"
    }

    "respect timeouts" >> {
      val redis = new MockRedis
      val connBuilder = new RedisConnectionBuilder {
        override def getConn: Jedis = redis
        override def close(): Unit = {}
      }

      val cache = new RedisEnrichmentCache(connBuilder, 1, true)
      val res = cache.get(Array("1", "foo")).asInstanceOf[String]
      res must be equalTo "bar"

      Thread.sleep(2)
      val res2 = cache.get(Array("1", "foo")).asInstanceOf[String]
      res2 must be equalTo "baz"
    }

    "respect no-local-cache" >> {
      val redis = new MockRedis
      val connBuilder = new RedisConnectionBuilder {
        override def getConn: Jedis = redis
        override def close(): Unit = {}
      }

      val cache = new RedisEnrichmentCache(connBuilder, -1, false)
      val res = cache.get(Array("1", "foo")).asInstanceOf[String]
      res must be equalTo "bar"

      Thread.sleep(2)
      val res2 = cache.get(Array("1", "foo")).asInstanceOf[String]
      res2 must be equalTo "baz"
    }

    "load via SPI" >> {

      val conf = ConfigFactory.parseString(
        """
          |{
          |   type = "redis"
          |   redis-url = "redis://localhost:0"
          |   expiration = 10
          |}
        """.stripMargin
      )

      val cache = ServiceLoader.load(classOf[EnrichmentCacheFactory]).asScala.find(_.canProcess(conf)).map(_.build(conf))
      try {
        cache must beSome
      } finally {
        cache.foreach(_.close())
      }
    }
  }
}
