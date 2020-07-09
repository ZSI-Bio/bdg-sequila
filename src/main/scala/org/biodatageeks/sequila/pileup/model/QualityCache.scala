package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

import scala.collection.mutable.ArrayBuffer

class QualityCache(size: Int) {
  var cache = new Array[ReadQualSummary](size)
  var currentIndex = 0
  var isFull=false

  def length: Int = cache.length

  def resize (newSize: Int): Unit =  {
    if (newSize <= length)
      return
    var newCache= new Array[ReadQualSummary](newSize)
    if (isFull) {
      System.arraycopy(cache, currentIndex, newCache, 0, length-currentIndex)
      System.arraycopy(cache, 0, newCache, length-currentIndex, currentIndex)
      currentIndex = length
    } else
      System.arraycopy(cache, 0, newCache, 0, length)

    cache = newCache
  }

  def apply(index: Int):ReadQualSummary = cache(index)
  def addOrReplace(readSummary: ReadQualSummary):Unit = {
    cache(currentIndex) = readSummary
    if (currentIndex + 1 >= length) {
      currentIndex = 0
      isFull = true
    }
    else currentIndex = currentIndex + 1
  }
//  def update(index: Int, readSummary: ReadQualSummary):Unit = cache(index)=readSummary

  def getReadsOverlappingPosition(position: Long): Array[ReadQualSummary] = {
    val buffer = new ArrayBuffer[ReadQualSummary]()
    for (rs <- cache) {
      if (rs != null && rs.overlapsPosition(position))
        buffer.append(rs)
    }
    buffer.toArray
  }

}

case class ReadQualSummary (name:String, start: Int, end: Int, qualString: String, cigar: Cigar) {

  def getBaseQualityForPosition(position: Int): Short = {
    val relPos = relativePosition(position)
    //fixme make sure, not going out of bounds
    val finalPos = if(relPos >= qualString.length) qualString.length-1 else relPos
    qualString.charAt(finalPos).toShort
  }


  def overlapsPosition(pos:Long):Boolean = isPositionInRange(pos) && !hasDeletionOnPosition(pos)
  private def relativePosition(absPosition: Int):Int = absPosition - start // FIXME take clips into account
  private def isPositionInRange(pos:Long) = start<= pos && end >= pos

  def hasDeletionOnPosition(pos:Long):Boolean = {
    if(!cigar.containsOperator(CigarOperator.DELETION))
      return false

    val cigarIterator = cigar.iterator()
    var positionFromCigar = start

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.DELETION) {
        val delStart = positionFromCigar
        val delEnd = positionFromCigar + cigarOperatorLen
        if (pos >= delStart && pos < delEnd)
          return true
      }

      positionFromCigar += cigarOperatorLen

      if (positionFromCigar > pos)
        return false
    }
 false
  }

}
