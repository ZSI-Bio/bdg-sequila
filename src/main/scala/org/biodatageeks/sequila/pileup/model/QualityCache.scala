package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.pileup.conf.QualityConstants
import scala.collection.mutable.ArrayBuffer

class QualityCache(size: Int) extends Serializable {
  var cache = new Array[ReadQualSummary](QualityConstants.CACHE_EXPANDER*size)
  val rollingIndexStart = size
  var currentIndex = 0
  var isFull = false // necessary for resize method, otherwise can be removed

  def this (qualityArray:Array[ReadQualSummary] ) {
    this(qualityArray.size)
    this.cache=qualityArray
  }
  def length: Int = cache.length
  def apply(index: Int):ReadQualSummary = cache(index)

  def ++ (that:QualityCache):QualityCache = {
    val mergedArray = new Array[ReadQualSummary](length + that.length)
    System.arraycopy(this.cache, 0, mergedArray, 0, length)
    System.arraycopy(that.cache, 0, mergedArray, length, that.length)
    new QualityCache(mergedArray)
  }

  def addOrReplace(readSummary: ReadQualSummary):Unit = {
    cache(currentIndex) = readSummary
    if (currentIndex + 1 >= length) {
      currentIndex = rollingIndexStart
      isFull = true
    }
    else currentIndex = currentIndex + 1
  }

  def getReadsOverlappingPosition(position: Long): Array[ReadQualSummary] = {
    val buffer = new ArrayBuffer[ReadQualSummary]()
    for (rs <- cache) {
      if (rs == null)
        return buffer.toArray
      else if (rs.overlapsPosition(position))
        buffer.append(rs)
    }
    buffer.toArray
  }

    // currently not used
    def resize (newSize: Int): Unit =  {
      if (newSize <= length)
        return
      val newCache= new Array[ReadQualSummary](newSize)
      if (isFull) {
        System.arraycopy(cache, currentIndex, newCache, 0, length-currentIndex)
        System.arraycopy(cache, 0, newCache, length-currentIndex, currentIndex)
        currentIndex = length
      } else
        System.arraycopy(cache, 0, newCache, 0, length)

      cache = newCache
    }

  // currently not used
  def getCacheTailFromPosition(position:Long):QualityCache ={
    val buffer = new ArrayBuffer[ReadQualSummary]()
    for (rs <- cache) {
      if(rs == null)
        return new QualityCache(buffer.toArray)
      else if (rs.start >=position)
        buffer.append(rs)
    }
    new QualityCache(buffer.toArray)
  }

}

