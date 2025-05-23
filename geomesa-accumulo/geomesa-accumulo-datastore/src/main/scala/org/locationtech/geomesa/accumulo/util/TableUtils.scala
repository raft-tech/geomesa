/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.apache.accumulo.core.client.admin.{NewTableConfiguration, TimeType}
import org.apache.accumulo.core.client.{AccumuloClient, NamespaceExistsException, TableExistsException}

@deprecated("use AccumuloDataStore.adapter or TableManager to ensure caching and distributed synchronization")
object TableUtils {

  /**
   * Creates the table if it doesn't exist
   *
   * @param connector connector
   * @param table table name
   * @param logical use logical time?
   * @return true if table was created, false if it already existed
   */
  def createTableIfNeeded(connector: AccumuloClient, table: String, logical: Boolean = true): Boolean = {
    val tableOps = connector.tableOperations()
    if (tableOps.exists(table)) { false } else {
      val dot = table.indexOf('.')
      if (dot > 0) {
        createNamespaceIfNeeded(connector, table.substring(0, dot))
      }
      val config = new NewTableConfiguration().setTimeType(if (logical) { TimeType.LOGICAL } else { TimeType.MILLIS })
      try { tableOps.create(table, config); true } catch {
        // this can happen with multiple threads but shouldn't cause any issues
        case _: TableExistsException => false
      }
    }
  }

  /**
   * Creates the namespace if it doesn't exist
   *
   * @param connector connector
   * @param namespace namespace
   * @return true if namespace was created, false if it already existed
   */
  def createNamespaceIfNeeded(connector: AccumuloClient, namespace: String): Boolean = {
    val nsOps = connector.namespaceOperations
    if (nsOps.exists(namespace)) { false } else {
      try { nsOps.create(namespace); true } catch {
        // this can happen with multiple threads but shouldn't cause any issue
        case _: NamespaceExistsException => false
      }
    }
  }
}
