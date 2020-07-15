package org.biodatageeks.sequila.pileup.broadcast

import org.biodatageeks.sequila.pileup.broadcast.Correction.PartitionCorrections
import org.biodatageeks.sequila.pileup.broadcast.Shrink.PartitionShrinks
import org.biodatageeks.sequila.pileup.model.{MultiLociAlts, MultiLociQuals, QualityCache}
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable.ArrayBuffer


case class Tail(
                     contig: String,
                     minPos: Int,
                     startPoint: Int,
                     events: Array[Short],
                     alts: MultiLociAlts,
                     quals: MultiLociQuals,
                     cumSum: Short,
                     cache: QualityCache
                   )


case class Range(contig: String, minPos: Int, maxPos: Int) {

  def findOverlappingTails(tails: ArrayBuffer[Tail]):ArrayBuffer[Tail] = {
    tails
      .filter(t => (t.contig == contig && t.startPoint + t.events.length > minPos) && t.minPos < minPos)
      .sortBy(r => (r.contig, r.minPos))
  }

  def precedingCumulativeSum(tails: ArrayBuffer[Tail]): Short = {
    val tailsFiltered = tails
      .filter(t => t.contig == contig && t.minPos < minPos)
    val cumSum = tailsFiltered.map(_.cumSum)
    val cumsSumArr = cumSum.toArray
    FastMath.sumShort(cumsSumArr)
  }
}


class PileupUpdate(
                    var tails: ArrayBuffer[Tail],
                    var ranges: ArrayBuffer[Range]
                  ) extends Serializable {

  def reset(): Unit = {
    tails = new ArrayBuffer[Tail]()
    ranges = new ArrayBuffer[Range]()
  }

  def add(p: PileupUpdate): PileupUpdate = {
    tails = tails ++ p.tails
    ranges = ranges ++ p.ranges
    this
  }

  def prepareOverlaps(): FullCorrections = {

    val correctionsMap = new PartitionCorrections()
    val shrinksMap = new PartitionShrinks()

    var it = 0
    for (range <- ranges.sortBy(r => (r.contig, r.minPos))) {

      val overlaps = range.findOverlappingTails(tails)
      val cumSum = range.precedingCumulativeSum(tails)
      if(range.contig=="MT" && range.minPos==7831)
        println()

      if(overlaps.isEmpty)
        correctionsMap += (range.contig, range.minPos) -> Correction(None, None, None, cumSum, null)
      else { // if there are  overlaps for this contigRange
        for(o <- overlaps) {
          val overlapLength = calculateOverlapLength(o, range, it, ranges)
          correctionsMap.updateWithOverlap(o, range, overlapLength, cumSum)
          shrinksMap.updateWithOverlap(o,range)
        }
      }
      it += 1
    }
    FullCorrections(correctionsMap, shrinksMap)
  }

  @inline private def calculateOverlapLength(o: Tail, range: Range, it: Int, ranges: ArrayBuffer[Range]) = {
    val length = if ((o.startPoint + o.events.length) > range.maxPos && ((ranges.length - 1 == it) || ranges(it + 1).contig != range.contig))
      o.startPoint + o.events.length - range.minPos + 1
    else if ((o.startPoint + o.events.length) > range.maxPos)
      range.maxPos - range.minPos
    //if it's the last part in contig or the last at all
    else
      o.startPoint + o.events.length - range.minPos + 1
    length
  }
}




