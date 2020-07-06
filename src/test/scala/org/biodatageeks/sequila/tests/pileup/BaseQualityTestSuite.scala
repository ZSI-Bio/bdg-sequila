package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}

class BaseQualityTestSuite extends PileupTestBase {

  val splitSize = "1000000"

  val pileupQuery =
    s"""
       |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE}, ${Columns.ALTS}, ${Columns.QUALS}
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath')
       |ORDER BY ${Columns.CONTIG}
                 """.stripMargin
  
  test("Simple Quals lookup") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)
    result.show(20, truncate = false)
  }

}
