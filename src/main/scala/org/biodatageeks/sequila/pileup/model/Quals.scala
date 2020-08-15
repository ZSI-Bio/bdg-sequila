package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.pileup.conf.{Conf, QualityConstants}
import org.biodatageeks.sequila.utils.FastMath
import org.biodatageeks.sequila.pileup.timers.PileupTimers._

import scala.collection.mutable

object Quals {
  type SingleLocusQuals = Array[Array[Short]]
  val SingleLocusQuals = Array[Array[Short]] _

  type MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals]
  val MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals] _

  implicit class SingleLocusQualsExtension(val arr: Quals.SingleLocusQuals) {
    def derivedCoverage: Short = arr.map(r => r.sum).sum

    def totalEntries: Long = arr.filter(_ != null).length

    def merge(arrOther: SingleLocusQuals): SingleLocusQuals = {

      val fastMerge = FastMath.merge2DEqualArrays(arr, arrOther)
      if (! fastMerge.isEmpty)
        return fastMerge.asInstanceOf[SingleLocusQuals]

      val keyset = arr.keySet ++ arrOther.keySet
      val mergedMap = new SingleLocusQuals()
      for (k <- keyset)
        mergedMap(k) = addArrays(arr.get(k), arrOther.get(k))
      mergedMap
    }

    def addArrays(arrOp1: Option[Array[Short]], arrOp2: Option[Array[Short]]): Array[Short] = {
      if (arrOp1.isEmpty)
        return arrOp2.get
      if (arrOp2.isEmpty)
        return arrOp1.get

      val arr1 = arrOp1.get
      val arr2 = arrOp2.get

      if (arr1.length >= arr2.length) {
        for (ind <- arr2.indices)
          arr1(ind) = (arr1(ind) + arr2(ind)).toShort
        arr1
      } else {
        for (ind <- arr1.indices)
          arr2(ind) = (arr1(ind) + arr2(ind)).toShort
        arr2
      }

    }

    def trim: SingleLocusQuals = {
      arr.arr({ case (k, v) => k -> v.take(v(Conf.qualityArrayLength - 1) + 1) })
    }

    def addQualityForAlt(alt: Char, quality: Byte, updateMax:Boolean): Unit = {
      val altByte = alt.toByte
      val qualityIndex = if (Conf.isBinningEnabled) (quality/Conf.binSize).toShort else quality
      val arrSize = Conf.qualityArrayLength

      if (!arr.contains(altByte)) {
        val array = new Array[Short](arrSize)
        array(qualityIndex) = 1.toShort // no need for incrementing. first and last time here.
        array(arrSize-1) = qualityIndex
        arr.update(altByte, array)
        return
      }

      if(updateMax) {
        arr(altByte)(qualityIndex) = (arr(altByte)(qualityIndex) + 1).toShort
        if (qualityIndex > arr(altByte).last)
          arr(altByte)(arrSize-1) = qualityIndex
        return
      }

      if (qualityIndex >= arr(altByte).length){
        val array = new Array[Short](arrSize)
        System.arraycopy(arr(altByte),0,array, 0, arr(altByte).length)
        array(qualityIndex) = 1.toShort
        arr.update(altByte, array)
        return
      }

      arr(altByte)(qualityIndex) = (arr(altByte)(qualityIndex) + 1).toShort

    }
  }

    implicit class MultiLociQualsExtension(val map: Quals.MultiLociQuals) {
      def ++(that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that).asInstanceOf[Quals.MultiLociQuals]

      def trim: MultiLociQuals = map.map({ case (k, v) => k -> v.trim })

      @inline
      def updateQuals(position: Int, alt: Char, quality: Byte, firstUpdate:Boolean = false, updateMax:Boolean=false): Unit = {
        if( !firstUpdate || map.contains(position) ) {
          map(position).addQualityForAlt(alt, quality, updateMax)
        }
        else {
          val singleLocusQualMap = new SingleLocusQuals()
          singleLocusQualMap.addQualityForAlt(alt, quality, updateMax)
          map.update(position, singleLocusQualMap)
        }
      }

      def merge(mapOther: MultiLociQuals): MultiLociQuals = {
        val fastMerge = FastMath.merge(map, mapOther)
        if (fastMerge.isDefined)
          return FastMath.merge(map, mapOther).get.asInstanceOf[MultiLociQuals]

        val keyset = map.keySet ++ mapOther.keySet
        var mergedQualsMap = new MultiLociQuals()
        for (k <- keyset)
          mergedQualsMap += k -> map.getOrElse(k, new SingleLocusQuals()).merge(mapOther.getOrElse(k, new SingleLocusQuals()))
        mergedQualsMap
      }

      def getTotalEntries: Long = {
        map.map { case (k, v) => k -> map(k).totalEntries }.foldLeft(0L)(_ + _._2)
      }

      def getQualitiesCount: mutable.IntMap[Int] = {
        val res = new mutable.IntMap[Int]()
        map.map { case (k, v) =>
          v.arr { case (kk, vv) =>
            for (index <- vv.indices by 2) {
              val item = vv(index)
              if (res.contains(item)) res.update(item, res(item) + 1)
              else res.update(item, 1)
            }
          }
        }
        res
      }

    }

  }
