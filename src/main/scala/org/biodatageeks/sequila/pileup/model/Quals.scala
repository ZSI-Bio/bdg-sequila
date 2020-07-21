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
    def derivedCoverage:Short = {
      var sum =0
      map.foreach({case (k,v) =>
        for (index <- 0 until v.length-1 by 2)
          sum += v(index+1)
      })
      sum.toShort
    }


    def getTotalEntries:Long = map.foldLeft(0)(_ + _._2.length).toLong

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

    def addQualityForAlt(alt: Char, quality: Short):Unit ={
      val altByte = alt.toByte
      val array = map.getOrElse(altByte, new ArrayBuffer[Short]())

      val qualityIndex =findQualityIndex(array,quality)
      qualityIndex match {
        case Some(ind) =>
          array(ind+1) = (array(ind+1)+ 1).toShort

        case None => {
          array.append(quality)
          array.append(1)
        }
      }
      map.update(altByte,array)
    }


    def findQualityIndex(array:ArrayBuffer[Short], quality:Short): Option[Int] ={

      if (array.isEmpty)
        return None

      val qualityIndex =array.indexOf(quality)
      if (qualityIndex == -1)
        return None

      else if (qualityIndex >= 0 && qualityIndex% 2 == 0)
        return Some(qualityIndex)

      else {
        val newArr = new ArrayBuffer[Short]()
        array.copyToBuffer(newArr)
        for (i <- 0 to qualityIndex)
          newArr(i)=0
        return findQualityIndex(newArr,quality)
      }
    }
  }




  implicit class MultiLociQualsExtension (val map: Quals.MultiLociQuals) {
    def ++ (that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that).asInstanceOf[Quals.MultiLociQuals]

    def updateQuals(position: Int, alt: Char, quality: Short): Unit = {

      val singleLocusQualMap = map.getOrElse(position, new SingleLocusQuals())
      singleLocusQualMap.addQualityForAlt(alt,quality)
      map.update(position, singleLocusQualMap)
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

    def getTotalEntries: Long =  {
      map.map{case(k,v) => k -> map(k).getTotalEntries}.foldLeft(0L)(_ + _._2)
    }

    def getQualitiesCount: mutable.LongMap[Int] = {
      val res = new mutable.LongMap[Int]()
      map.map{ case(k,v) =>
        v.map{ case (kk,vv) =>
          for (index <- vv.indices by 2) {
            val item = vv(index)
            if(res.contains(item)) res.update(item, res(item)+1)
            else res.update(item, 1)
          }
        }
      }
      res
    }

  }
}
