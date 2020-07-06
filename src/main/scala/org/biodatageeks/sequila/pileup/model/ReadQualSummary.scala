package org.biodatageeks.sequila.pileup.model

class QualityCache {
  var cache = new Array[ReadQualSummary](0)

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

}

case class ReadQualSummary (start: Int, end: Int, qualString: String, cigar: String)
