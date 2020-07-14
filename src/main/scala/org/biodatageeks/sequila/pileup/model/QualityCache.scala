package org.biodatageeks.sequila.pileup.model

import scala.collection.mutable.ArrayBuffer

class QualityCache(size: Int) {
  var cache = new Array[ReadQualSummary](size)
  var currentIndex = 0
  var isFull = false

  def length: Int = cache.length

  def apply(index: Int):ReadQualSummary = cache(index)

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


  def addOrReplace(readSummary: ReadQualSummary):Unit = {
    cache(currentIndex) = readSummary
    if (currentIndex + 1 >= length) {
      currentIndex = 0
      isFull = true
    }
    else currentIndex = currentIndex + 1
  }

  def getReadsOverlappingPosition(position: Long): Array[ReadQualSummary] = {
    val buffer = new ArrayBuffer[ReadQualSummary]()
    for (rs <- cache)
      if (rs != null && rs.overlapsPosition(position))
        buffer.append(rs)

    buffer.toArray
  }
}

