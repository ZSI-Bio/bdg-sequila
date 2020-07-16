package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.utils.FastMath
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Quals {
  type SingleLocusQuals = mutable.HashMap[Byte, ArrayBuffer[Short]]
  val SingleLocusQuals = mutable.HashMap[Byte, ArrayBuffer[Short]] _

  type MultiLociQuals = mutable.LongMap[Quals.SingleLocusQuals]
  val MultiLociQuals = mutable.LongMap[Quals.SingleLocusQuals] _

  implicit class SingleLocusQualsExtension(val map: Quals.SingleLocusQuals) {
    def derivedCoverage:Short = map.foldLeft(0)(_+_._2.length).toShort

    def merge(mapOther: SingleLocusQuals): SingleLocusQuals = {
      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return FastMath.merge(map, mapOther).get.asInstanceOf[SingleLocusQuals]

      val keyset = map.keySet++mapOther.keySet
      val mergedMap = new SingleLocusQuals()
      for (k <- keyset)
        mergedMap(k) = map.getOrElse(k, ArrayBuffer.empty[Short])++(mapOther.getOrElse(k, ArrayBuffer.empty[Short]))
      mergedMap
    }
  }


  implicit class MultiLociQualsExtension (val map: Quals.MultiLociQuals) {
    def ++ (that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that).asInstanceOf[Quals.MultiLociQuals]

    def updateQuals(pos: Int, alt: Char, quality: Short): Unit = {
      val position = pos // naturally indexed
      val altByte = alt.toByte

      val qualMap = map.getOrElse(position, new Quals.SingleLocusQuals())
      val array = qualMap.getOrElse(altByte, new ArrayBuffer[Short]())
      array.append(quality)
      qualMap.update(altByte,array)
      map.update(position, qualMap)
    }

    def merge(mapOther: MultiLociQuals): MultiLociQuals = {
      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return FastMath.merge(map, mapOther).get.asInstanceOf[MultiLociQuals]

      val keyset = map.keySet++mapOther.keySet
      var mergedQualsMap = new MultiLociQuals()
      for (k <- keyset)
        mergedQualsMap += k -> map.getOrElse(k, new SingleLocusQuals()).merge(mapOther.getOrElse(k, new SingleLocusQuals()))
      mergedQualsMap
    }
  }
}
