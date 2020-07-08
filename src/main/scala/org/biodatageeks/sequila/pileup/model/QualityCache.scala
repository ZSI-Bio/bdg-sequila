package org.biodatageeks.sequila.pileup.model

import scala.collection.mutable.ArrayBuffer

class QualityCache(size: Int) {
  var cache = new Array[ReadQualSummary](size)

  def length = cache.length

  def resize (newSize: Int): Unit =  {
    if (newSize <= length)
      return
    var newCache= new Array[ReadQualSummary](newSize)
    if (cache.length != 0)
      System.arraycopy(cache, 0, newCache, 0, length)
    cache = newCache
  }

  def apply(index: Int):ReadQualSummary = cache(index)
  def add(readSummary: ReadQualSummary, index:Int):Unit = cache(index) = readSummary
  def update(index: Int, readSummary: ReadQualSummary):Unit = cache(index)=readSummary

  def getReadsOverlappingPosition(position: Int): Array[ReadQualSummary] = {
    val buffer = new ArrayBuffer[ReadQualSummary]()
    for (rs <- this.cache) {
      if (rs != null && rs.start<= position && rs.end >= position)
        buffer.append(rs)
    }
    buffer.toArray
  }

}

case class ReadQualSummary (start: Int, end: Int, qualString: String, cigar: String) {

  def getBaseQualityForPosition(position: Int): Short = {
    val relPos = relativePosition(position)
    //fixme make sure, not going out of bounds
    val finalPos = if(relPos >= qualString.length) qualString.length-1 else relPos
    qualString.charAt(finalPos).toShort
  }

  private def relativePosition(absPosition: Int):Int = absPosition - start // FIXME take clips into account


}
