package org.biodatageeks.sequila.utils

import scala.collection.mutable

object FastMath {
  def getSubArrayForRange(input: Array[Long], start: Int, end: Int):Array[Long] = {
    val sortedArray = input.sorted
    val output = new Array[Long](input.size)
    var currPosition = input.size-1
    var outputPos = 0

    while(currPosition >=0){
      val item = sortedArray(currPosition)
      if (item >= start && item <= end) {
        output(outputPos) = item
        outputPos += 1
      }
      currPosition -=1

      if (item < start)
        return output.take(outputPos)
    }
    output.take(outputPos)

  }

  def sumShort(a: Array[Short]) = {
    var i = 0
    var cumSum = 0
    while(i < a.length){
      cumSum += a(i)
      i+=1
    }
    cumSum.toShort
  }

  /**
    * finds index of the last non zero element of array
    * @param array array of Short elements
    * @return index
    */

  def findMaxIndex(array: Array[Short]): Int = {
    var i = array.length - 1

    while (i > 0) {
      if (array(i) != 0)
        return i
      i -= 1
    }
    -1
  }

  def merge [A,B](map1: mutable.Map[A,B], map2:mutable.Map[A,B] ): Option[mutable.Map[A,B]] ={
    if (map1 == null || map1.isEmpty)
      return Some(map2)
    if (map2 == null || map2.isEmpty)
      return Some(map1)
    if(map1.keySet.intersect(map2.keySet).isEmpty)
      return Some(map1 ++ map2)
    None
  }


  def addArrays(arr1: Array[Short], arr2 :Array[Short]): Array[Short] = {
    if (arr1.isEmpty)
      return arr2
    if (arr2.isEmpty)
      return arr1
    if (arr1.length >= arr2.length) {
      for (ind <- arr2.indices)
        arr1(ind) = (arr1(ind) + arr2(ind)).toShort
      arr1
    } else {
      for (ind <- arr1.indices)
        arr2(ind) = (arr1(ind) + arr2(ind)).toShort
      arr2
    }
  }

  def merge2DEqualArrays(arr1: Array[Array[Short]], arr2: Array[Array[Short]] ): Array[Array[Short]] = {
    if(arr1 == null || arr1.isEmpty)
      return arr2
    if(arr2 == null || arr2.isEmpty)
      return arr1
    val result = new Array[Array[Short]](arr1.length)
    var id = 0
    val length = arr1.length
    while(id < length){
      result(id) = addArrays(arr1(id), arr2(id))
      id += 1
    }
    result
  }
}
