/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import org.apache.commons.io.FileUtils
import org.geotools.api.data.{DataStoreFinder, SimpleFeatureStore}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.file.{Files, Path}

@RunWith(classOf[JUnitRunner])
class BucketVsLeafStorageTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  sequential

  var tempDir: Path = _

  def mkSft(name: String) =  SimpleFeatureTypes.createType(name, "attr:String,dtg:Date,*geom:Point:srid=4326")
  lazy val ds = DataStoreFinder.getDataStore(Map(
    "fs.path" -> tempDir.toFile.getPath,
    "fs.encoding" -> "parquet",
    "fs.config.xml" -> "<configuration><property><name>parquet.compression</name><value>gzip</value></property></configuration>"
  ).asJava).asInstanceOf[FileSystemDataStore]

  def features(sft: SimpleFeatureType) = List[SimpleFeature](
    ScalaSimpleFeature.create(sft, "1", "first",  "2016-01-01", "POINT (-5 5)"), // z2 = 2
    ScalaSimpleFeature.create(sft, "2", "second", "2016-01-02", "POINT (5 5)"),  // z2 = 3
    ScalaSimpleFeature.create(sft, "3", "third",  "2016-01-03", "POINT (5 -5)"), // z2 = 1
    ScalaSimpleFeature.create(sft, "3", "fourth", "2016-01-04", "POINT (-5 -5)") // z2 = 0
  )

  def toList(sfi: SimpleFeatureIterator): List[SimpleFeature] = SelfClosingIterator(sfi).toList

  step {
    tempDir = Files.createTempDirectory("geomesa")
  }

  "DataStores" should {

    "store data in leaves" >> {

      "in one scheme" >> {
        val typeName = "leaf-one"
        val sft = mkSft(typeName)
        sft.setScheme("daily")
        ds.createSchema(sft)
        ds.getTypeNames.length mustEqual 1
        ds.getTypeNames.head mustEqual typeName
        ds.storage(typeName).metadata.leafStorage must beTrue
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).take(2).asJava))

        val fp = tempDir.resolve("leaf-one")
        fp.toFile.exists must beTrue
        fp.resolve("2016/01").toFile.exists must beTrue

        Seq(
          "2016/01/01_W[0-9a-f]{32}\\.parquet",
          "2016/01/02_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 1
        }
        toList(ds.getFeatureSource(sft.getTypeName).getFeatures.features).size mustEqual 2

        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).drop(2).asJava))

        Seq(
          "2016/01/01_W[0-9a-f]{32}\\.parquet",
          "2016/01/02_W[0-9a-f]{32}\\.parquet",
          "2016/01/03_W[0-9a-f]{32}\\.parquet",
          "2016/01/04_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 4

        // For now adding new features will create new parquet files on disk
        // and for leaf file stroage we should get a one up sequence file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).asJava))
        Seq(
          "2016/01/01_W[0-9a-f]{32}\\.parquet",
          "2016/01/02_W[0-9a-f]{32}\\.parquet",
          "2016/01/03_W[0-9a-f]{32}\\.parquet",
          "2016/01/04_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 8

      }

      "in two schemes" >> {
        val typeName = "leaf-two"
        val sft = mkSft(typeName)
        sft.setScheme("daily,z2-2bits")
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain(typeName)
        ds.storage(typeName).metadata.leafStorage must beTrue
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).asJava))

        val fp = tempDir.resolve("leaf-two")
        fp.toFile.exists must beTrue

        Seq(
          "2016/01/01/2_W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3_W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1_W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(x => x.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 4
        // For now adding more features results in a next seq file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).asJava))

        Seq(
          "2016/01/01/2_W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3_W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1_W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(x => x.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 8
      }
    }

    "store data in buckets" >> {
      "in one scheme" >> {
        val typeName = "bucket-one"
        val sft = mkSft(typeName)
        sft.setScheme("daily")
        sft.setLeafStorage(false)
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain(typeName)
        ds.storage(typeName).metadata.leafStorage must beFalse
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).take(2).asJava))

        val fp = tempDir.resolve("bucket-one")

        Seq(
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 1
        }
        toList(ds.getFeatureSource(sft.getTypeName).getFeatures.features).size mustEqual 2

        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).drop(2).asJava))

        Seq(
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 4

        // For now adding new features will create new parquet files on disk
        // and for leaf file stroage we should get a one up sequence file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).asJava))
        Seq(
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/W[0-9a-f]{32}\\.parquet",
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 8
      }

      "with two schemes" >> {
        val typeName = "bucket-two"
        val sft = mkSft(typeName)
        sft.setScheme("daily,z2-2bits")
        sft.setLeafStorage(false)
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain(typeName)
        ds.storage(typeName).metadata.leafStorage must beFalse
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).asJava))

        val fp = tempDir.resolve(typeName)
        fp.toFile.exists must beTrue

        Seq(
          "2016/01/01/2/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 4
        // For now adding more features results in a next seq file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).asJava))

        forall(Seq(
          "2016/01/01/2/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0/W[0-9a-f]{32}\\.parquet",
          "2016/01/01/2/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _)) { f =>
          Files.walk(fp).iterator.asScala.count(_.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 8
      }

      "support deprecated leaf-storage config" >> {
        val sft = mkSft("bucket-three")
        sft.setScheme("daily", Map("leaf-storage" -> "false"))
        ds.createSchema(sft)
        ds.storage("bucket-three").metadata.leafStorage must beFalse
      }
    }
  }

  step {
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
