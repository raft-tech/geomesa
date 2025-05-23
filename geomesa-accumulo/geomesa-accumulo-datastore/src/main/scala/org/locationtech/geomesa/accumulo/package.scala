/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.accumulo.util.TableManager
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object accumulo {

  object AccumuloProperties {

    object TableProperties {
      val TableCreationSync: SystemProperty =
        SystemProperty("geomesa.accumulo.table.sync", TableManager.TableSynchronization.ZooKeeper.toString)
      val TableCacheExpiry: SystemProperty = SystemProperty("geomesa.accumulo.table.cache.expiry", "10 minutes")
    }

    object AccumuloMapperProperties {
      val DESIRED_SPLITS_PER_TSERVER = SystemProperty("geomesa.mapreduce.splits.tserver.max")
      val DESIRED_ABSOLUTE_SPLITS = SystemProperty("geomesa.mapreduce.splits.max")
    }

    object BatchWriterProperties {
      val WRITER_LATENCY      = SystemProperty("geomesa.batchwriter.latency", "60 seconds")
      val WRITER_MEMORY_BYTES = SystemProperty("geomesa.batchwriter.memory", "50mb")
      val WRITER_THREADS      = SystemProperty("geomesa.batchwriter.maxthreads", "10")
      val WRITE_TIMEOUT       = SystemProperty("geomesa.batchwriter.timeout")
    }

    object StatsProperties {
      val STAT_COMPACTION_INTERVAL = SystemProperty("geomesa.stats.compact.interval", "1 hour")
    }

    object RemoteProcessingProperties {
      val RemoteArrowProperty: SystemProperty = SystemProperty("geomesa.accumulo.remote.arrow.enable")
      val RemoteBinProperty: SystemProperty = SystemProperty("geomesa.accumulo.remote.bin.enable")
      val RemoteDensityProperty: SystemProperty = SystemProperty("geomesa.accumulo.remote.density.enable")
      val RemoteStatsProperty: SystemProperty = SystemProperty("geomesa.accumulo.remote.stats.enable")
    }
  }
}
