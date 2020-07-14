package org.biodatageeks.sequila.pileup.model

import org.apache.spark.broadcast.Broadcast
import org.biodatageeks.sequila.pileup.broadcast
import org.biodatageeks.sequila.pileup.broadcast.Correction.PartitionCorrections
import org.biodatageeks.sequila.pileup.broadcast.Shrink.PartitionShrinks
import org.biodatageeks.sequila.pileup.broadcast.{FullCorrections, PileupUpdate, Tail}
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{CalculateAltsTimer, CalculateEventsTimer, ShrinkAltsTimer, ShrinkArrayTimer, TailAltsTimer, TailCovTimer, TailEdgeTimer}
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable.ArrayBuffer


/** Events aggregation on contig
  */
case class ContigAggregate(
                            contig: String = "",
                            contigLen: Int = 0,
                            events: Array[Short],
                            alts: MultiLociAlts,
                            quals: MultiLociQuals,
                            startPosition: Int = 0,
                            maxPosition: Int = 0,
                            shrinkedEventsArraySize: Int = 0,
                            maxSeqLen:Int = 0
                                ) {

  def hasAltOnPosition(pos:Int):Boolean = alts.contains(pos)
  def getRange: broadcast.Range = broadcast.Range(contig, startPosition, maxPosition)
  def getPileupUpdate:PileupUpdate = new PileupUpdate(ArrayBuffer(getTail), ArrayBuffer(getRange))
  def getAltPositionsForRange(start: Int, end: Int): scala.collection.Set[Long] = {
    alts.keySet.filter(pos => pos >= start && pos <= end)
  }


  def calculateMaxLength(allPositions: Boolean): Int = {
    if (! allPositions)
      return events.length

    val firstBlockMaxLength = startPosition - 1
    val lastBlockMaxLength = contigLen - maxPosition
    firstBlockMaxLength + events.length + lastBlockMaxLength
  }

  def updateEvents(pos: Int, startPart: Int, delta: Short): Unit = {
    val position = pos - startPart
    events(position) = (events(position) + delta).toShort
  }

  def updateAlts(pos: Int, alt: Char): Unit = {
    val position = pos // naturally indexed
    val altByte = alt.toByte

    val altMap = alts.getOrElse(position, new SingleLocusAlts())
    altMap(altByte) = (altMap.getOrElse(altByte, 0.toShort) + 1).toShort
    alts.update(position, altMap)
  }

  def updateQuals(pos: Int, alt: Char, quality: Short): Unit = {
    val position = pos // naturally indexed
    val altByte = alt.toByte

    val qualMap = quals.getOrElse(position, new SingleLocusQuals())
    val array = qualMap.getOrElse(altByte, new ArrayBuffer[Short]())
    array.append(quality)
    qualMap.update(altByte,array)
    quals.update(position, qualMap)
  }

  def getTail:Tail ={
    val tailStartIndex = maxPosition - maxSeqLen
    val tailCov = TailCovTimer.time {
      if(maxSeqLen ==  events.length ) events
      else
        events.takeRight(maxSeqLen)
    }
    val tailAlts = TailAltsTimer.time {alts.filter(_._1 >= tailStartIndex)}
    val tailQuals = quals.filter(_._1 >= tailStartIndex)
    val cumSum = FastMath.sumShort(events)
    val tail = TailEdgeTimer.time {broadcast.Tail(contig, startPosition, tailStartIndex, tailCov, tailAlts, tailQuals,cumSum)}
    tail
  }

  def getAdjustedAggregate(b:Broadcast[FullCorrections]): ContigAggregate = {
    val upd: PartitionCorrections = b.value.corrections
    val shrink:PartitionShrinks = b.value.shrinks

    val adjustedEvents = CalculateEventsTimer.time { calculateAdjustedEvents(upd) }
    val adjustedAlts = CalculateAltsTimer.time{ calculateAdjustedAlts(upd) }
    val adjustedQuals = calculateAdjustedQuals(upd)

    val shrinkedEventsSize = ShrinkArrayTimer.time { calculateShrinkedEventsSize(shrink, adjustedEvents) }
    val shrinkedAltsMap = ShrinkAltsTimer.time { calculateShrinkedAlts(shrink, adjustedAlts) }
    val shrinkedQualsMap = calculateShrinkedQuals(shrink, adjustedQuals)

    ContigAggregate(contig, contigLen, adjustedEvents, shrinkedAltsMap, shrinkedQualsMap, startPosition, maxPosition, shrinkedEventsSize, maxSeqLen)
  }

  private def calculateAdjustedEvents(upd: PartitionCorrections): Array[Short] = {
    var eventsArrMutable = events

    upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some(correction) => // array of covs and cumSum
        correction.events match {
          case Some(overlapArray) =>
            if (overlapArray.length > events.length)
              eventsArrMutable =  events ++ Array.fill[Short](overlapArray.length - events.length)(0) // extend array

            var i = 0
            eventsArrMutable(i) = (eventsArrMutable(i) + correction.cumulativeSum).toShort // add cumSum to zeroth element

            while (i < overlapArray.length) {
              eventsArrMutable(i) = (eventsArrMutable(i) + overlapArray(i)).toShort
              i += 1
            }
            eventsArrMutable
          case None =>
            eventsArrMutable(0) = (eventsArrMutable(0) + correction.cumulativeSum).toShort
            eventsArrMutable
        }
      case None => eventsArrMutable
    }
  }

  private def calculateAdjustedAlts(upd:PartitionCorrections): MultiLociAlts = {
    upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some(correction) => // covs alts cumSum
        correction.alts match {
          case Some(overlapAlts) => FastMath.mergeNestedMaps(alts, overlapAlts)
          case None => alts
        }
      case None => alts
    }
  }

  private def calculateAdjustedQuals(upd:PartitionCorrections): MultiLociQuals = {
    upd.get((contig, startPosition)) match { // check if there is a value for contigName and minPos in upd, returning array of coverage and cumSum to update current contigRange
      case Some(correction) =>
        correction.quals match {
          case Some(overlapQuals) => FastMath.mergeQualMaps(quals, overlapQuals)
          case None => quals
        }
      case None => quals
    }
  }

  private def calculateShrinkedEventsSize[T](shrink: PartitionShrinks, updArray: Array[T]): Int = {
    shrink.get((contig, startPosition)) match {
      case Some(shrink) => shrink.index
      case None => updArray.length
    }
  }

  private def calculateShrinkedAlts[T](shrink: PartitionShrinks, altsMap: MultiLociAlts): MultiLociAlts = {
    shrink.get((contig, startPosition)) match {
      case Some(shrink) =>
        val cutoffPosition = shrink.index
        altsMap.filter(_._1 < cutoffPosition)
      case None => altsMap
    }
  }

  private def calculateShrinkedQuals[T](shrink: PartitionShrinks, qualsMap: MultiLociQuals): MultiLociQuals = {
    shrink.get((contig, startPosition)) match {
      case Some(shrink) =>
        val cutoffPosition = shrink.index
        qualsMap.filter(_._1 < cutoffPosition)
      case None => qualsMap
    }
  }
}
