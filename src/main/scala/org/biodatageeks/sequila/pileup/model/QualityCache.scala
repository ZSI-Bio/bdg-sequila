package org.biodatageeks.sequila.pileup.model

import java.util.function.BiFunction

import org.biodatageeks.formats.Interval
import org.biodatageeks.sequila.pileup.conf.QualityConstants

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class IntervalTreeSer[V] extends htsjdk.samtools.util.IntervalTree[V] with Serializable

abstract class BiFunctionSer[T,V,U] extends BiFunction[T,V,U] with Serializable {
}

class QualityCache(size: Int) extends Serializable {

  var cache = new Array[Array[ReadQualSummary]](QualityConstants.CACHE_EXPANDER*size)
  var cacheTree = new IntervalTreeSer[Int]()
  val rollingIndexStart = size
  var lastInterval =  Interval(Int.MaxValue, Int.MinValue)
  var currentIndex = 0
  var isFull = false // necessary for resize method, otherwise can be removed

//  def mergeArrays(a: Array[ReadQualSummary], b: Array[ReadQualSummary]) = a ++ b



  def this (qualityArray:Array[Array[ReadQualSummary]] ,qualityTree:IntervalTreeSer[Int]) {
    this(qualityArray.size/2)
    this.cache = qualityArray
    this.cacheTree = qualityTree
  }

  def copy:QualityCache = {
    val newCache = new QualityCache(size)
    cache.copyToArray(newCache.cache)
    newCache
  }
  def length: Int = cache.length
  def apply(index: Int):Array[ReadQualSummary] = cache(index)

  def ++ (that:QualityCache):QualityCache = {
    val mergedArray = new Array[Array[ReadQualSummary]](length + that.length)
    System.arraycopy(this.cache, 0, mergedArray, 0, length)
    System.arraycopy(that.cache, 0, mergedArray, length, that.length)
    val it = that.cacheTree.iterator()
    while (it.hasNext){
      val rs = it.next()
      this.cacheTree.put(rs.getStart, rs.getEnd, rs.getValue)
    }
    new QualityCache(mergedArray, this.cacheTree)
  }

  def addOrReplace(readSummary: ReadQualSummary):Unit = {
    if(readSummary.start != lastInterval.pos_start || readSummary.end != lastInterval.pos_end){
      //adding a new interval - cleanup
      val arr = cache(currentIndex)
      if (arr != null) {
        val rs = cache(currentIndex)(0)
        cacheTree.remove(rs.start, rs.end)
      }
      cache(currentIndex) = Array(readSummary)
      cacheTree.put(readSummary.start, readSummary.end, currentIndex)
      currentIndex = currentIndex + 1
      lastInterval = Interval(readSummary.start, readSummary.end)
    }
    else {
      cache(currentIndex) = cache(currentIndex) ++ Array(readSummary)
    }

    if (currentIndex + 1 >= length) {
      currentIndex = rollingIndexStart
      isFull = true
    }
  }

  def initSearchIndex:Int ={
    if(currentIndex==0) 0
    else if (!isFull) currentIndex -1
    else if (isFull && currentIndex == rollingIndexStart) cache.length-1
    else currentIndex-1
  }


  def getReadsOverlappingPosition(position: Long): Array[ReadQualSummary] = {

    val indexes = cacheTree.overlappers(position.toInt, position.toInt)
      .toArray
      .map(_.getValue)
    val buffer = new ArrayBuffer[ReadQualSummary]()
      for(i <- indexes){
        buffer ++= cache(i).filter(_.overlapsPosition(position))
      }
//      .filter(rs => rs.overlapsPosition(position) )
//    val buffer = new ArrayBuffer[ReadQualSummary]()
//        for (rs <- cache) {
//          if (rs == null )
//            return buffer.toArray
//          else if (rs.overlapsPosition(position))
//            buffer.append(rs)
//        }
    buffer.toArray

  }


    // currently not used
    def resize (newSize: Int): Unit =  {
      if (newSize <= length)
        return
      val newCache= new Array[Array[ReadQualSummary]](newSize)
      if (isFull) {
        System.arraycopy(cache, currentIndex, newCache, 0, length-currentIndex)
        System.arraycopy(cache, 0, newCache, length-currentIndex, currentIndex)
        currentIndex = length
      } else
        System.arraycopy(cache, 0, newCache, 0, length)

      cache = newCache
    }

  // currently not used
//  def getCacheTailFromPosition(position:Long):QualityCache ={
//    val buffer = new ArrayBuffer[ReadQualSummary]()
//    for (rs <- cache) {
//      if(rs == null)
//        return new QualityCache(buffer.toArray)
//      else if (rs.start >=position)
//        buffer.append(rs)
//    }
//    new QualityCache(buffer.toArray)
//  }

  //  val mergeArrays = new BiFunctionSer[Array[ReadQualSummary], Array[ReadQualSummary], Array[ReadQualSummary]] {
  //    def apply(a: Array[ReadQualSummary], b: Array[ReadQualSummary]) =  a ++ b
  //  }
}

