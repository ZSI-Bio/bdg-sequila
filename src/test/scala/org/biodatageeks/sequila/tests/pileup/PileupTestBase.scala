package org.biodatageeks.sequila.tests.pileup

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, ShortType, StringType, StructField, StructType}
import org.biodatageeks.sequila.pileup.conf.QualityConstants
import org.biodatageeks.sequila.pileup.model.Quals._
import org.eclipse.jetty.server.Authentication.Wrapped
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable

class PileupTestBase extends FunSuite
  with DataFrameSuiteBase
  with BeforeAndAfter
  with SharedSparkContext{

  val sampleId = "NA12878.multichrom.md"
  val samResPath: String = getClass.getResource("/multichrom/mdbam/samtools.pileup").getPath
  val referencePath: String = getClass.getResource("/reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta").getPath
  val bamPath: String = getClass.getResource(s"/multichrom/mdbam/${sampleId}.bam").getPath
  val cramPath : String = getClass.getResource(s"/multichrom/mdcram/${sampleId}.cram").getPath
  val tableName = "reads_bam"
  val tableNameCRAM = "reads_cram"

  val schema: StructType = StructType(
    List(
      StructField("contig", StringType, nullable = true),
      StructField("position", IntegerType, nullable = true),
      StructField("reference", StringType, nullable = true),
      StructField("coverage", ShortType, nullable = true),
      StructField("pileup", StringType, nullable = true),
      StructField("quality", StringType, nullable = true)
    )
  )
  before {
    System.setProperty("spark.kryo.registrator", "org.biodatageeks.sequila.pileup.serializers.CustomKryoRegistrator")
    spark
      .conf.set("spark.sql.shuffle.partitions",1) //FIXME: In order to get orderBy in Samtools tests working - related to exchange partitions stage
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(
      s"""
         |CREATE TABLE $tableName
         |USING org.biodatageeks.sequila.datasources.BAM.BAMDataSource
         |OPTIONS(path "$bamPath")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS $tableNameCRAM")
    spark.sql(
      s"""
         |CREATE TABLE $tableNameCRAM
         |USING org.biodatageeks.sequila.datasources.BAM.CRAMDataSource
         |OPTIONS(path "$cramPath", refPath "$referencePath" )
         |
      """.stripMargin)

    val mapToString = (map: Map[Byte, Short]) => {
      if (map == null)
        "null"
      else
        map.map({
          case (k, v) => k.toChar -> v}).mkString.replace(" -> ", ":")
    }

    val byteToString = ((byte: Byte) => byte.toString)

    val qualMapToCoverage = (map: Map[Byte, mutable.WrappedArray[Short]], cov: Short) => {
      if (map == null) cov
      else map.map({case (k,v) => v.sum}).sum
    }

    val qualMapAgg = (map: Map[Byte, mutable.WrappedArray[Short]]) => {
//      val nestedMap = new mutable.HashMap[String, Short]()

      if (map == null) null
      else map.map({case (k,v) => {
        val nestedMap = new mutable.HashMap[String, Short]()
        for (i <- v.indices)
          if (v(i) != 0)
            nestedMap += (i + QualityConstants.OFFSET).toChar.toString -> v(i)
        k-> nestedMap
      }

      })
    }

    val covEquality = (originalCov:Short, qualityCov:Short) => originalCov==qualityCov


    spark.udf.register("mapToString", mapToString)
    spark.udf.register("byteToString", byteToString)
    spark.udf.register("qualMapToCoverage", qualMapToCoverage)
    spark.udf.register("covEquality", covEquality)
    spark.udf.register("qualMapAgg", qualMapAgg)

  }

}
