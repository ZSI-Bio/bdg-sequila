package org.biodatageeks.sequila.pileup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{PileupTemplate, SparkSession, Strategy}
import org.apache.spark.unsafe.types.UTF8String
import org.biodatageeks.sequila.datasources.BAM.BDGAlignFileReaderWriter
import org.biodatageeks.sequila.datasources.InputDataType
import org.biodatageeks.sequila.inputformats.BDGAlignInputFormat
import org.biodatageeks.sequila.pileup.conf.QualityConstants.{DEFAULT_BIN_SIZE, DEFAULT_MAX_QUAL}
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.model.PileupRecord
import org.biodatageeks.sequila.utils.{InternalParams, TableFuncs}
import org.seqdoop.hadoop_bam.{BAMBDGInputFormat, CRAMBDGInputFormat}

import scala.reflect.ClassTag

class PileupStrategy (spark:SparkSession) extends Strategy with Serializable {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case PileupTemplate(tableName, sampleId, refPath, qual, binSize, output) =>
        val inputFormat = TableFuncs.getTableMetadata(spark, tableName).provider
        inputFormat match {
          case Some(f) =>
            if (f == InputDataType.BAMInputDataType)
              PileupPlan[BAMBDGInputFormat](plan, spark, tableName, sampleId, refPath, qual, binSize, output) :: Nil
            else if (f == InputDataType.CRAMInputDataType)
              PileupPlan[CRAMBDGInputFormat](plan, spark, tableName, sampleId, refPath, qual, binSize, output) :: Nil
            else Nil
          case None => throw new RuntimeException("Only BAM and CRAM file formats are supported in pileup function.")
        }
      case _ => Nil
    }
  }
}

case class PileupPlan [T<:BDGAlignInputFormat](plan:LogicalPlan, spark:SparkSession,
                                               tableName:String,
                                               sampleId:String,
                                               refPath: String,
                                               qual: Boolean,
                                               binSize: Option[Int],
                                               output:Seq[Attribute])(implicit c: ClassTag[T])
  extends SparkPlan with Serializable  with BDGAlignFileReaderWriter [T]{

  override def children: Seq[SparkPlan] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    setupPileupConfiguration()
    val out = new Pileup(spark).handlePileup(tableName, sampleId, refPath, output)
    projectOutput(out)
  }

  private def projectOutput(rdd:RDD[PileupRecord]):RDD[InternalRow] = {
    val schema = plan.schema
    rdd.mapPartitions(p  => {
      val projection = UnsafeProjection.create(schema)
      p.map { r =>
        projection.apply(InternalRow.fromSeq(Seq(
          UTF8String.fromString(r.contig),
          r.start, r.end,
          UTF8String.fromString(r.ref),
          r.cov, r.countRef, r.countNonRef,
          CatalystTypeConverters.convertToCatalyst(r.alts),
          CatalystTypeConverters.convertToCatalyst(r.quals))))
      }
    })
  }

  private def setupPileupConfiguration():Unit = {
    val maxQual = spark.conf.get(InternalParams.maxBaseQualityValue, DEFAULT_MAX_QUAL.toString).toInt
    Conf.maxQuality = maxQual
    Conf.maxQualityIndex = maxQual + 1
    Conf.includeBaseQualities = qual
    if(binSize.isDefined) {
      Conf.isBinningEnabled = true
      Conf.binSize = binSize.get
      Conf.qualityArrayLength = Math.round(Conf.maxQuality  / Conf.binSize.toDouble).toInt + 2
    } else {
      Conf.isBinningEnabled = false
      Conf.binSize = DEFAULT_BIN_SIZE
      Conf.qualityArrayLength = Conf.maxQuality + 2
    }
  }

}
