package org.biodatageeks.sequila.pileup.model

import htsjdk.samtools.{CigarOperator, SAMRecord}
import org.biodatageeks.sequila.pileup.MDTagParser
import org.biodatageeks.sequila.pileup.conf.Conf
import org.biodatageeks.sequila.pileup.timers.PileupTimers.{AnalyzeReadsCalculateAltsParseMDTimer, AnalyzeReadsCalculateAltsTimer, AnalyzeReadsCalculateEventsTimer}
import org.biodatageeks.sequila.utils.ReadConsts

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ReadOperations {
  object implicits {
    implicit def reads(r:SAMRecord) = ExtendedReads(r)
  }
}

case class ExtendedReads(r:SAMRecord) {

  def analyzeRead(contig: String,
                  aggregate: ContigAggregate,
                  contigMaxReadLen: mutable.HashMap[String, Int],
                  qualityCache: QualityCache): Unit = {

    AnalyzeReadsCalculateEventsTimer.time { calculateEvents(contig, aggregate, contigMaxReadLen) }
    val foundAlts = AnalyzeReadsCalculateAltsTimer.time{calculateAlts(aggregate, qualityCache) }
    if (Conf.includeBaseQualities) {
      val readQualSummary = ReadQualSummary(r.getReadName, r.getStart, r.getEnd, r.getBaseQualityString, r.getCigar)
      fillBaseQualitiesForExistingAlts(aggregate, foundAlts, readQualSummary)
      addToCache(qualityCache, contigMaxReadLen(contig), readQualSummary)
    }
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

  private def calculateAlts(aggregate: ContigAggregate, qualityCache: QualityCache): Seq[Long] = {
    val read = this.r
    var position = read.getStart
    val md = read.getAttribute("MD").toString
    val ops = AnalyzeReadsCalculateAltsParseMDTimer.time { MDTagParser.parseMDTag(md) }
    var delCounter = 0
    var altsPositions = new ArrayBuffer[Long]()
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
        val altBaseQual = getAltBaseQualFromSequence(indexInSeq)
        val altPosition = position - clipLen - 1
        val newAlt = !aggregate.hasAltOnPosition(altPosition)
        aggregate.updateAlts(altPosition, altBase)
        aggregate.updateQuals(altPosition, altBase, altBaseQual)

        if (newAlt && Conf.includeBaseQualities)
          fillPastQualitiesFromCache(aggregate, altPosition, qualityCache)
        altsPositions.append(altPosition)
      }
      else if(mdtag.base == 'S')
        position += mdtag.length
    }
    altsPositions.toSeq
  }

  def fillBaseQualitiesForExistingAlts(agg: ContigAggregate, blackList:Seq[Long], readQualSummary: ReadQualSummary): Unit = {
    val altsPositions = agg.getAltPositionsForRange(r.getStart, r.getEnd)
    val updatePositions = altsPositions diff blackList
    for (pos <- updatePositions) {
      if(!readQualSummary.hasDeletionOnPosition(pos))
        agg.updateQuals(pos.toInt, ReadConsts.REF_SYMBOL, readQualSummary.getBaseQualityForPosition(pos.toInt)) //fixme get real values from BQString
    }
  }
  def addToCache(qualityCache: QualityCache, maxLen: Int, readQualSummary: ReadQualSummary):Unit = {
    if (maxLen * ReadConsts.CACHE_EXPANDER > qualityCache.length)
      qualityCache.resize(maxLen*ReadConsts.CACHE_EXPANDER)
    qualityCache.addOrReplace(readQualSummary)

  }

  def fillPastQualitiesFromCache(agg: ContigAggregate, altPosition: Int, qualityCache: QualityCache): Unit = {
    val reads = qualityCache.getReadsOverlappingPosition(altPosition)
    for (read <- reads) {
      val qual = read.getBaseQualityForPosition(altPosition)
      agg.updateQuals(altPosition, ReadConsts.REF_SYMBOL, qual )
    }
  }


  private def getAltBaseFromSequence(position: Int):Char = this.r.getReadString.charAt(position-1)

  private def getAltBaseQualFromSequence(position: Int):Short = this.r.getBaseQualityString.charAt(position-1).toShort

  private def updateMaxCigarInContig(cigarLen:Int, contig: String, contigMaxReadLen: mutable.HashMap[String, Int]): Unit = {
    if (cigarLen > contigMaxReadLen(contig))
      contigMaxReadLen(contig) = cigarLen
  }


}
