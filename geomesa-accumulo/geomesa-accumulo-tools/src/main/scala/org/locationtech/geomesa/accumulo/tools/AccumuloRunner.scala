/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * Portions Crown Copyright (c) 2016-2025 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools

import com.beust.jcommander.ParameterException
import org.apache.accumulo.core.conf.ClientProperty
import org.locationtech.geomesa.accumulo.data.AccumuloClientConfig
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.Prompt

object AccumuloRunner extends RunnerWithAccumuloEnvironment {

  import org.locationtech.geomesa.accumulo.tools

  override val name: String = "geomesa-accumulo"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new tools.data.AccumuloAgeOffCommand,
      new tools.data.AccumuloCompactCommand,
      new tools.data.AccumuloManagePartitionsCommand,
      new tools.data.AddAttributeIndexCommand,
      new tools.data.AddIndexCommand,
      new tools.data.TableConfCommand,
      new tools.export.AccumuloExplainCommand,
      new tools.export.AccumuloExportCommand,
      new tools.export.AccumuloPlaybackCommand,
      new tools.ingest.AccumuloBulkCopyCommand,
      new tools.ingest.AccumuloBulkIngestCommand,
      new tools.ingest.AccumuloDeleteFeaturesCommand,
      new tools.ingest.AccumuloIngestCommand,
      new tools.schema.AccumuloCreateSchemaCommand,
      new tools.schema.AccumuloDeleteCatalogCommand,
      new tools.schema.AccumuloRemoveSchemaCommand,
      new tools.schema.AccumuloUpdateSchemaCommand,
      new tools.status.AccumuloDescribeSchemaCommand,
      new tools.status.AccumuloGetSftConfigCommand,
      new tools.status.AccumuloGetTypeNamesCommand,
      new tools.status.AccumuloVersionRemoteCommand,
      new tools.stats.AccumuloQueryAuditCommand,
      new tools.stats.AccumuloStatsAnalyzeCommand,
      new tools.stats.AccumuloStatsBoundsCommand,
      new tools.stats.AccumuloStatsConfigureCommand,
      new tools.stats.AccumuloStatsCountCommand,
      new tools.stats.AccumuloStatsTopKCommand,
      new tools.stats.AccumuloStatsHistogramCommand
    )
  }
}

trait RunnerWithAccumuloEnvironment extends Runner {

  /**
    * Loads Accumulo properties from `accumulo-client.properties` or `accumulo.conf`
    */
  override def resolveEnvironment(command: Command): Unit = {
    lazy val config = AccumuloClientConfig.load()

    Option(command.params).collect { case p: AccumuloConnectionParams => p }.foreach { p =>
      if (p.user == null) {
        config.getProperty(ClientProperty.AUTH_PRINCIPAL.getKey) match {
          case null => throw new ParameterException("Parameter '--user' is required")
          case u => p.user = u
        }
      }
      if (p.password != null && p.keytab != null) {
        // error if both password and keytab supplied
        throw new ParameterException("Cannot specify both password and keytab")
      } else if (p.password == null && p.keytab == null &&
          AccumuloClientConfig.PasswordAuthType.equalsIgnoreCase(ClientProperty.AUTH_TYPE.getValue(config))) {
        // if password not supplied, and not using keytab, prompt for it
        p.password = Option(config.getProperty(ClientProperty.AUTH_TOKEN.getKey)).getOrElse(Prompt.readPassword())
      }
    }

    Option(command.params).collect { case p: InstanceNameParams => p }.foreach { p =>
      if (p.instance == null) {
        Option(config.getProperty(ClientProperty.INSTANCE_NAME.getKey)).foreach(p.instance = _)
      }
      if (p.zookeepers == null) {
        Option(config.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey)).foreach(p.zookeepers = _)
        if (p.zkTimeout == null) {
          Option(config.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey)).foreach(p.zkTimeout = _)
        }
      }
    }
  }

  override protected def classpathEnvironments: Seq[String] = Seq("ACCUMULO_HOME", "HADOOP_HOME")
}
