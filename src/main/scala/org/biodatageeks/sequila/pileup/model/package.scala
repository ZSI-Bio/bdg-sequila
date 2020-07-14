package org.biodatageeks.sequila.pileup

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

package object model {
  type SingleLocusAlts = mutable.HashMap[Byte,Short]
  val SingleLocusAlts = mutable.HashMap[Byte,Short] _

  type MultiLociAlts= mutable.LongMap[SingleLocusAlts]
  val MultiLociAlts = mutable.LongMap[SingleLocusAlts] _

  type SingleLocusQuals = mutable.HashMap[Byte, ArrayBuffer[Short]]
  val SingleLocusQuals = mutable.HashMap[Byte, ArrayBuffer[Short]] _

  type MultiLociQuals = mutable.LongMap[SingleLocusQuals]
  val MultiLociQuals = mutable.LongMap[SingleLocusQuals] _

  type QualityCacheByContig = mutable.HashMap[String, QualityCache]
  val QualityCacheByContig = mutable.HashMap[String, QualityCache] _


  implicit class SingleLocusQualsExtension(val map: SingleLocusQuals) {
    def derivedCoverage:Short = map.foldLeft(0)(_+_._2.length).toShort
  }

  implicit class SingleLocusAltsExtension(val map: SingleLocusAlts) {
    def derivedAltsNumber:Short = map.foldLeft(0)(_+_._2).toShort
  }

}
