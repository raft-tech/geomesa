/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureReadSupport
import org.locationtech.geomesa.security.VisibilityUtils.IsVisible
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.Transform.Transforms

import scala.annotation.tailrec
import scala.util.control.NonFatal

class ParquetPathReader(
    conf: Configuration,
    readSft: SimpleFeatureType,
    parquetFilter: FilterCompat.Filter,
    gtFilter: Option[org.geotools.api.filter.Filter],
    visFilter: IsVisible,
    transform: Option[(String, SimpleFeatureType)]
  ) extends FileSystemPathReader with LazyLogging {

  private val gtf = gtFilter.orNull

  private val transformFeature: SimpleFeature => SimpleFeature = transform match {
    case None => null
    case Some((tdefs, tsft)) =>
      val definitions = Transforms(readSft, tdefs).toArray
      f => new TransformSimpleFeature(tsft, definitions, f)
  }

  override def read(path: Path): CloseableIterator[SimpleFeature] = {
    logger.debug(s"Opening reader for path $path")
    new ParquetFileIterator(path)
  }

  private class ParquetFileIterator(path: Path) extends CloseableIterator[SimpleFeature] {

    private val reader: ParquetReader[SimpleFeature] =
      ParquetReader.builder(new SimpleFeatureReadSupport(), path).withConf(conf).withFilter(parquetFilter).build()

    private var staged: SimpleFeature = _

    override def close(): Unit = {
      logger.debug(s"Closing reader for path $path")
      reader.close()
    }

    override def next(): SimpleFeature = {
      val res = staged
      staged = null
      res
    }

    @tailrec
    override final def hasNext: Boolean = {
      if (staged != null) { true } else {
        val read = try { reader.read() } catch {
          case NonFatal(e) => logger.error(s"Error reading file '$path'", e); null
        }
        if (read == null) {
          false
        } else if (visFilter(read) && (gtf == null || gtf.evaluate(read))) {
          staged = if (transformFeature == null) { read } else { transformFeature(read) }
          true
        } else {
          hasNext
        }
      }
    }
  }
}
