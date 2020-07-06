package org.biodatageeks.sequila.tests.pileup

import org.biodatageeks.sequila.pileup.model.{QualityCache, ReadQualSummary}
import org.scalatest.FunSuite

class QualityCacheTestSuite extends FunSuite{

  test("Qual cache test"){

    val qualCache = new QualityCache
    assert(qualCache.length==0)

    qualCache.resize(2)
    assert(qualCache.length==2)

    val read0 = ReadQualSummary(10,12,"32423423", "89M")
    val read1 = ReadQualSummary(13,14,"44234", "100M")
    val read2 = ReadQualSummary(19,21,"44234", "101M")
    val read3 = ReadQualSummary(31,41,"44234", "101M")

    //add
    qualCache(0)= read0
    qualCache.add(read1, 1) // different syntax for adding to cache

    assert(qualCache(0).start==read0.start)
    assert(qualCache(1).start==read1.start)

    qualCache.resize(4)
    assert(qualCache.length==4)

    qualCache.add(read2, 2)

    //check previous values exist
    assert(qualCache(0).start==read0.start)
    assert(qualCache(1).start==read1.start)
    assert(qualCache(2).start==read2.start)

    qualCache(0) = read3 // replace read0 value
    assert(qualCache(0).start==read3.start)
  }

}
