package org.biodatageeks.sequila.tests.pileup

import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.utils.{Columns, InternalParams, SequilaRegister}

class BaseQualityTestSuite extends PileupTestBase {

  val splitSize = "1000000"

  val pileupQuery =
    s"""
       |SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE}, ${Columns.ALTS}
       |FROM  pileup('$tableName', '${sampleId}', '$referencePath', true)
       |ORDER BY ${Columns.CONTIG}
                 """.stripMargin
  
  test("Normal split") {
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sparkContext.setLogLevel("ERROR")

    val result = ss.sql(pileupQuery)
    assert(result.isEmpty)
  }

}
