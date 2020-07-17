package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{Cigar, CigarOperator}

case class ReadQualSummary (start: Int, end: Int, qualString: String, cigar: Cigar) {

  def getBaseQualityForPosition(position: Int): Short = {
    qualString.charAt(relativePosition(position)).toShort
  }

  def overlapsPosition(pos:Long):Boolean = isPositionInRange(pos) && !hasDeletionOnPosition(pos)

  private def relativePosition(absPosition: Int):Int = absPosition - start - numDeletionsBeforePositions(absPosition)

  private def isPositionInRange(pos:Long) = start<= pos && end >= pos

  private def numDeletionsBeforePositions(pos: Int):Int = {
    if(!cigar.containsOperator(CigarOperator.DELETION))
      return 0

    traverseCigar(cigar, pos, countDelsBeforePosition = true, hasDelOnPosition = false)
  }

  def hasDeletionOnPosition(pos:Long):Boolean = {
    if(!cigar.containsOperator(CigarOperator.DELETION))
      return false

    if (traverseCigar(cigar,pos,countDelsBeforePosition = false, hasDelOnPosition = true) == 0)  false else true
  }

  private def traverseCigar(cigar:Cigar, pos: Long, countDelsBeforePosition: Boolean, hasDelOnPosition: Boolean): Int = {
    val cigarIterator = cigar.iterator()
    var positionFromCigar = start
    var numDeletions = 0

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.DELETION) {
        val delStart = positionFromCigar
        val delEnd = positionFromCigar + cigarOperatorLen

        if (pos >= delStart && pos < delEnd && countDelsBeforePosition) {
          val diff = pos.toInt - delStart
          return numDeletions + diff
        }
        if (pos >= delStart && pos < delEnd && hasDelOnPosition)
          return 1

        numDeletions += cigarOperatorLen
      }

      positionFromCigar += cigarOperatorLen

      if (positionFromCigar > pos && hasDelOnPosition)
        return 0

      if (positionFromCigar > pos && countDelsBeforePosition)
        return numDeletions
    }
    0
  }
}


