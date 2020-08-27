package org.biodatageeks.sequila.pileup

import org.biodatageeks.sequila.pileup.BenchmarkRunner.generateRandomAlts
import org.biodatageeks.sequila.pileup.conf.QualityConstants

import scala.collection.mutable
import scala.util.Random

object BenchmarkRunner {

  final val posStart = 1
  final val postStop = 1000000
  final val shift = 65
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    val nestedIntHashMap = new mutable.IntMap[mutable.HashMap[Byte, Array[Short]]]()
    val nestedInt2DArray = new mutable.IntMap[Array[Array[Short]]] ()
    val nestedInt3DArray = new Array[Array[Array[Short]]](postStop + 1)
    val positions = generatePositions(posStart, postStop + 1)
    prepareTestHashMap(positions, nestedIntHashMap)
    benchmarkUpdateMap(positions, nestedIntHashMap)

    prepareTest2DArray(positions, nestedInt2DArray)
    benchmarkUpdate2DArray(positions, nestedInt2DArray)


    prepareTest3DArray(positions, nestedInt3DArray)
    benchmarkUpdate3DArray(positions, nestedInt3DArray)


  }

  private def benchmarkUpdateMap(updates: Range, map: mutable.IntMap[mutable.HashMap[Byte, Array[Short]]] ) = {
    val t1 = System.nanoTime
    for(i <- updates){
      val index = i
      map(index)('A')(1) = (map(index)('A')(1) + 1).toShort
    }
    val t2 = System.nanoTime
    println(s"HashMap - Time taken: ${(t2- t1) / 1e9d} s")
  }


  private def benchmarkUpdate2DArray(updates: Range, map: mutable.IntMap[Array[Array[Short]]] ) = {
    val t1 = System.nanoTime
    for(i <- updates){
      val index = i
      val index2 = 'A'.toInt - shift
      map(index)(index2)(1) = (map(index)(index2)(1) + 1).toShort
    }
    val t2 = System.nanoTime
    println(s"2D Array - Time taken: ${(t2 - t1) / 1e9d} s")
  }


  private def benchmarkUpdate3DArray(updates: Range, map: Array[Array[Array[Short]]] ) = {
    val t1 = System.nanoTime
    for(i <- updates){
      val index = i
      val index2 = 'A'.toInt - shift
      map(index)(index2)(1) = (map(index)(index2)(1) + 1).toShort
    }
    val t2 = System.nanoTime

    println(s"3D Array - Time taken: ${(t2 - t1) / 1e9d} s")
  }

  private def prepareTestHashMap(positions:Range, map: mutable.IntMap[mutable.HashMap[Byte, Array[Short]]]): Unit = {
    for(p <- positions){
      val alts = generateRandomAlts
      val innerMap = new mutable.HashMap[Byte, Array[Short]] ()
      for(a <- alts){
        innerMap(a.toByte) = generateRandomQuals
      }
      map(p) = innerMap
    }
  }

  private def prepareTest2DArray(positions:Range, map: mutable.IntMap[Array[Array[Short]]]) = {

    for(p <- positions) {
      val arr = new Array[Array[Short]](30)
      val alts = generateRandomAlts
      for(a <- alts) {
        arr(a.toInt - shift) = generateRandomQuals
      }
      map(p) = arr
    }
    map
  }

  private def prepareTest3DArray(positions:Range, arr: Array[Array[Array[Short]]]) = {
    for(p <- positions) {
      val arrInner = new Array[Array[Short]](30)
      val alts = generateRandomAlts
      for(a <- alts) {
        arrInner(a.toInt - shift) = generateRandomQuals
      }
      arr(p) = arrInner
    }
    arr
  }

  private def generatePositions(start: Int, stop: Int) = {
    Range(start, stop)
  }

  private def generateRandomAlts() = {
    val arr = Array('A', 'G', 'T', 'C')
    val altsNum = 1 + Random.nextInt(arr.length - 1 )
    for(i <- 1 to altsNum)
      yield arr(i - 1)
  }

  private def generateRandomQuals = {
    val arr = new Array[Short](QualityConstants.MAX_QUAL)
    for(i <- 0 to arr.length - 1 ){
      arr(i) = Random.nextInt(10).toShort
    }
    arr
  }

}
