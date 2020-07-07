package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{CigarOperator, SAMRecord}
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{AnalyzeReadsCalculateAltsParseMDTimer, AnalyzeReadsCalculateAltsTimer, AnalyzeReadsCalculateEventsTimer}
import org.biodatageeks.sequila.utils.ReadConsts

import scala.collection.mutable

object ReadOperations {
  object implicits {
    implicit def reads(r:SAMRecord) = ExtendedReads(r)
  }
}

case class ExtendedReads(r:SAMRecord) {

  def fillBaseQualitiesForExisitingAlts(aggregate: ContigAggregate) = {
    val altsPositions = aggregate.getAltPositionsForRange(r.getStart, r.getEnd)
    for (pos <- altsPositions)
      aggregate.updateQuals(pos.toInt,ReadConsts.REF_SYMBOL, 10)
  }

  def analyzeRead(contig: String,
                  aggregate: ContigAggregate,
                  contigMaxReadLen: mutable.HashMap[String, Int],
                  qual:Boolean, qualityCache: QualityCache): Unit = {

    if (qual) fillBaseQualitiesForExisitingAlts(aggregate)
    AnalyzeReadsCalculateEventsTimer.time { calculateEvents(contig, aggregate, contigMaxReadLen) }
    AnalyzeReadsCalculateAltsTimer.time{ calculateAlts(aggregate, qual, qualityCache) }


  }

  def addToCache(qualityCache: QualityCache, counter: Int, maxLen: Int) = {
    val readQualSummary = ReadQualSummary(r.getStart, r.getEnd, r.getBaseQualityString, r.getCigarString)
    if (maxLen > qualityCache.length)
      qualityCache.resize(maxLen)
    qualityCache((counter-1)%maxLen) = readQualSummary

  }

  def calculateEvents(contig: String, aggregate: ContigAggregate, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    val partitionStart = aggregate.startPosition
    var position = this.r.getStart
    val cigarIterator = this.r.getCigar.iterator()
    var cigarLen = 0

    while (cigarIterator.hasNext) {
      val cigarElement = cigarIterator.next()
      val cigarOperatorLen = cigarElement.getLength
      val cigarOperator = cigarElement.getOperator

      if (cigarOperator == CigarOperator.M ||
        cigarOperator == CigarOperator.X   ||
        cigarOperator == CigarOperator.EQ  ||
        cigarOperator == CigarOperator.N   ||
        cigarOperator == CigarOperator.D)
        cigarLen += cigarOperatorLen

      // update events array according to read alignment blocks start/end
      if (cigarOperator == CigarOperator.M || cigarOperator == CigarOperator.X || cigarOperator == CigarOperator.EQ) {

        aggregate.updateEvents(position, partitionStart, delta = 1)
        position += cigarOperatorLen
        aggregate.updateEvents(position, partitionStart, delta = -1)
      }
      else if (cigarOperator == CigarOperator.N || cigarOperator == CigarOperator.D)
        position += cigarOperatorLen

    }
    updateMaxCigarInContig(cigarLen, contig, contigMaxReadLen)
  }

  def calculatePositionInReadSeq( mdPosition: Int): Int = {
    val cigar = this.r.getCigar
    if (!cigar.containsOperator(CigarOperator.INSERTION))
      return mdPosition

    var numInsertions = 0
    val cigarIterator = cigar.iterator()
    var position = 0

    while (cigarIterator.hasNext){
      if (position > mdPosition + numInsertions)
        return mdPosition + numInsertions
      val cigarElement = cigarIterator.next()
      val cigarOpLength = cigarElement.getLength
      val cigarOp = cigarElement.getOperator

      if (cigarOp == CigarOperator.INSERTION) {
        numInsertions += cigarOpLength
      }

      position = position+cigarOpLength
    }
    mdPosition + numInsertions
  }

  def fillPastQualitiesFromCache(eventAggregate: ContigAggregate, altPosition: Int, qualityCache: QualityCache): Unit = {
    val reads = qualityCache.getReadsOverlappingPosition(altPosition)
    for (read <- reads) {
      val qual = read.getBaseQualityForPosition(altPosition)
      eventAggregate.updateQuals(altPosition, ReadConsts.REF_SYMBOL, qual )
    }
  }

  private def calculateAlts(eventAggregate: ContigAggregate, qual:Boolean, qualityCache: QualityCache): Unit = {
    val read = this.r
    var position = read.getStart
    val md = read.getAttribute("MD").toString
    val ops = AnalyzeReadsCalculateAltsParseMDTimer.time { MDTagParser.parseMDTag(md) }
    var delCounter = 0
    val clipLen =
      if (read.getCigar.getCigarElement(0).getOperator.isClipping)
        read.getCigar.getCigarElement(0).getLength else 0

    position += clipLen

    for (mdtag <- ops) {
      if (mdtag.isDeletion) {
        delCounter += 1
        position +=1
      } else if (mdtag.base != 'S') {
        position += 1

        val indexInSeq = calculatePositionInReadSeq(position - read.getStart -delCounter)
        val altBase = getAltBaseFromSequence(indexInSeq)
        val altPosition = position - clipLen - 1
        val newAlt = !eventAggregate.hasAltOnPosition(altPosition)
        eventAggregate.updateAlts(altPosition, altBase)
        if (newAlt && qual)
          fillPastQualitiesFromCache(eventAggregate, altPosition, qualityCache)
      }
      else if(mdtag.base == 'S')
        position += mdtag.length
    }
  }

  private def getAltBaseFromSequence(position: Int):Char = this.r.getReadString.charAt(position-1)

  private def updateMaxCigarInContig(cigarLen:Int, contig: String, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    if (cigarLen > contigMaxReadLen(contig))
      contigMaxReadLen(contig) = cigarLen
  }

}