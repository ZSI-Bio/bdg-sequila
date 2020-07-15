package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}

class BaseQualityTestSuite extends PileupTestBase {

  val splitSize = "1000000"

  val qualCoverageCol = "qual_coverage"
  val covEquality = "cov_equal"
  val pileupQuery =
    s"""
       |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END},
       | ${Columns.REF}, ${Columns.COVERAGE},
       | ${Columns.ALTS}, ${Columns.QUALS},
       | qualMapToCoverage(${Columns.QUALS}, ${Columns.COVERAGE}) as $qualCoverageCol,
       | covEquality (${Columns.COVERAGE}, qualMapToCoverage(${Columns.QUALS}, ${Columns.COVERAGE}) ) as $covEquality
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true)
       |ORDER BY ${Columns.CONTIG}
                 """.stripMargin
  
  test("Simple Quals lookup Single partition") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)
    //result.show(100, truncate = false)
//    assert(result.count()==14671)
    println(result.count())

//    result.where(s"$covEquality=false").show(20)

    val equals = result.select(covEquality).distinct()
    assert(equals.count()==1)
    assert(equals.head.getBoolean(0))
  }

  test("Simple Quals lookup Multiple partitions") {
    spark.sqlContext.setConf(InternalParams.InputSplitSize, splitSize)
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)
    //result.show(100, truncate = false)
//    assert(result.count()==14671)
    println(result.count())

//        result.where(s"$covEquality=false").orderBy(s"${Columns.START}").show(20)

    val equals = result.select(covEquality).distinct()
    assert(equals.count()==1)
    assert(equals.head.getBoolean(0))
  }
}
