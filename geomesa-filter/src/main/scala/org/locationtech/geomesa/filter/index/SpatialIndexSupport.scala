/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.index

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.index.SpatialIndex

trait SpatialIndexSupport {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def sft: SimpleFeatureType

  def index: SpatialIndex[SimpleFeature]

  /**
    * Query based on a geotools filter
    *
    * @param filter filter
    * @return
    */
  def query(filter: Filter): Iterator[SimpleFeature] = {
    if (filter == Filter.INCLUDE) { index.query() } else {
      val geometries = FilterHelper.extractGeometries(filter, sft.getGeomField, intersect = false)
      if (geometries.isEmpty) { index.query().filter(filter.evaluate) } else {
        val env = geometries.values.head.getEnvelopeInternal
        geometries.values.tail.foreach(g => env.expandToInclude(g.getEnvelopeInternal))
        index.query(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY).filter(filter.evaluate)
      }
    }
  }
}
