package org.biodatageeks.sequila.pileup.broadcast

import org.biodatageeks.sequila.pileup.model.{MultiLociAlts, MultiLociQuals}
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable


case class Correction(
                       events: Option[Array[Short]],
                       alts: Option[MultiLociAlts],
                       quals: Option[MultiLociQuals],
                       cumulativeSum: Short
                     )


object Correction {
  type PartitionCorrections = mutable.HashMap[(String,Int), Correction]
  val PartitionCorrections = mutable.HashMap[(String,Int), Correction] _

  implicit class PartitionCorrectionsExtension(val map: PartitionCorrections) {
    def updateWithOverlap(overlap:Tail, range:Range, overlapLen: Int, cumSum: Int): Unit = {

      val arrEvents= Array.fill[Short](math.max(0, overlap.startPoint - range.minPos))(0) ++ overlap.events.takeRight(overlapLen)

      map.get((range.contig, range.minPos))  match {
        case Some(correction) => {
          val newArrEvents = correction.events.get.zipAll(arrEvents, 0.toShort, 0.toShort).map { case (x, y) => (x + y).toShort }
          val newAlts = (correction.alts.get ++ overlap.alts).asInstanceOf[MultiLociAlts]
          val newQuals=(correction.quals.get ++ overlap.quals).asInstanceOf[MultiLociQuals]
          val newCumSum = (correction.cumulativeSum - FastMath.sumShort(overlap.events.takeRight(overlapLen)) ).toShort

          val newCorrection = Correction(Some(newArrEvents), Some(newAlts), Some(newQuals), newCumSum)

          val coordinates = if (overlap.minPos < range.minPos)
            (range.contig, range.minPos)
          else
            (range.contig, overlap.minPos)

          map.update(coordinates, newCorrection)
        }
        case _ => {
          val newCumSum=(cumSum - FastMath.sumShort(overlap.events.takeRight(overlapLen))).toShort
          val newCorrection = Correction(Some(arrEvents), Some(overlap.alts), Some(overlap.quals),newCumSum)
          map += (range.contig, range.minPos) -> newCorrection
        }
      }

    }
  }
}

