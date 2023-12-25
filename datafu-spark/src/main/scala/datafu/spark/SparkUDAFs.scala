/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package datafu.spark

import scala.collection.{mutable, Map}

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, _}


/**
 * UserDefineAggregateFunction is deprecated and will be removed in DataFu 2.1.0 in order to allow compilation with Spark 3.2 and up.
 * Please use the methods in @Aggregators instead
 */
@Deprecated
object SparkUDAFs {

  /**
    * Like Google's MultiSets.
    * Aggregate function that creates a map of key to its count.
    */
  @Deprecated
  class MultiSet() extends UserDefinedAggregateFunction {

    def inputSchema: StructType = new StructType().add("key", StringType)

    def bufferSchema: StructType =
      new StructType().add("mp", MapType(StringType, IntegerType))

    def dataType: DataType = MapType(StringType, IntegerType, false)

    def deterministic: Boolean = true

    // This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = mutable.Map()
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val key = input.getString(0)
      if (key != null) {
        buffer(0) = buffer.getMap(0) + (key -> (buffer
          .getMap(0)
          .getOrElse(key, 0) + 1))
      }
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val mp = mutable.Map[String, Int]() ++= buffer1.getMap(0)
      buffer2
        .getMap(0)
        .keys
        .foreach((key: String) =>
          if (key != null) {
            mp.put(key,
              mp.getOrElse(key, 0) + buffer2.getMap(0).getOrElse(key, 0))
          })
      buffer1(0) = mp
    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row): Any = {
      buffer(0)
    }

  }

  /**
    * Essentially the same as MultiSet, but gets an Array for input.
    * There is an extra option to limit the number of keys (like CountDistinctUpTo)
    */
  @Deprecated
  class MultiArraySet[T: Ordering](dt: DataType = StringType, maxKeys: Int = -1)
      extends UserDefinedAggregateFunction {

    def inputSchema: StructType = new StructType().add("key", ArrayType(dt))

    def bufferSchema: StructType = new StructType().add("mp", dataType)

    def dataType: DataType = MapType(dt, IntegerType, false)

    def deterministic: Boolean = true

    // This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = mutable.Map()
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val mp = mutable.Map[T, Int]() ++= buffer.getMap(0)
      val keyArr: Seq[T] = Option(input.getAs[Seq[T]](0)).getOrElse(Nil)
      for (key <- keyArr; if key != null)
        mp.put(key, mp.getOrElse(key, 0) + 1)

      buffer(0) = limitKeys(mp, 3)
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val mp = mutable.Map[T, Int]() ++= buffer1.getMap(0)
      buffer2
        .getMap(0)
        .keys
        .foreach((key: T) =>
          if (key != null) {
            mp.put(key,
              mp.getOrElse(key, 0) + buffer2.getMap(0).getOrElse(key, 0))
          })

      buffer1(0) = limitKeys(mp, 3)
    }

    private def limitKeys(mp: Map[T, Int], factor: Int = 1): Map[T, Int] = {
      if (maxKeys > 0 && maxKeys * factor < mp.size) {
        val k = mp.toList.map(_.swap).sorted.reverse(maxKeys - 1)._1
        var mp2 = mutable.Map[T, Int]() ++= mp.filter((t: (T, Int)) =>
          t._2 >= k)
        var toRemove = mp2.size - maxKeys
        if (toRemove > 0) {
          mp2 = mp2.filter((t: (T, Int)) => {
            if (t._2 > k) {
              true
            } else {
              if (toRemove >= 0) {
                toRemove = toRemove - 1
              }
              toRemove < 0
            }
          })
        }
        mp2
      } else {
        mp
      }
    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row): Map[T, Int] = {
      limitKeys(buffer.getMap(0).asInstanceOf[Map[T, Int]])
    }

  }

  /**
    * Merge maps of kind string -> set<string>
    */
  @Deprecated
  class MapSetMerge extends UserDefinedAggregateFunction {

    def inputSchema: StructType = new StructType().add("key", dataType)

    def bufferSchema: StructType = inputSchema

    def dataType: DataType = MapType(StringType, ArrayType(StringType))

    def deterministic: Boolean = true

    // This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = mutable.Map()
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val mp0 = input.getMap(0)
      if (mp0 != null) {
        val mp = mutable.Map[String, mutable.WrappedArray[String]]() ++= input
          .getMap(0)
        buffer(0) =
          merge(mp, buffer.getMap[String, mutable.WrappedArray[String]](0))
      }
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val mp = mutable.Map[String, mutable.WrappedArray[String]]() ++= buffer1
        .getMap(0)
      buffer1(0) =
        merge(mp, buffer2.getMap[String, mutable.WrappedArray[String]](0))
    }

    def merge(mpBuffer: mutable.Map[String, mutable.WrappedArray[String]],
              mp: Map[String, mutable.WrappedArray[String]])
      : mutable.Map[String, mutable.WrappedArray[String]] = {
      if (mp != null) {
        mp.keys.foreach((key: String) => {
          val blah1: mutable.WrappedArray[String] =
            mpBuffer.getOrElse(key, mutable.WrappedArray.empty)
          val blah2: mutable.WrappedArray[String] =
            mp.getOrElse(key, mutable.WrappedArray.empty)
          mpBuffer.put(
            key,
            mutable.WrappedArray.make(
              (Option(blah1).getOrElse(mutable.WrappedArray.empty) ++ Option(
                blah2).getOrElse(mutable.WrappedArray.empty)).toSet.toArray)
          )
        })
      }

      mpBuffer
    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row): Any = {
      buffer(0)
    }

  }

  /**
    * Counts number of distinct records, but only up to a preset amount -
    * more efficient than an unbounded count
    */
  @Deprecated
  class CountDistinctUpTo(maxItems: Int = -1)
      extends UserDefinedAggregateFunction {

    def inputSchema: StructType = new StructType().add("key", StringType)

    def bufferSchema: StructType =
      new StructType().add("mp", MapType(StringType, BooleanType))

    def dataType: DataType = IntegerType

    def deterministic: Boolean = true

    // This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = mutable.Map()
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (buffer.getMap(0).size < maxItems) {
        val key = input.getString(0)
        if (key != null) {
          buffer(0) = buffer.getMap(0) + (key -> true)
        }
      }
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      if (buffer1.getMap(0).size < maxItems) {
        val mp = mutable.Map[String, Boolean]() ++= buffer1.getMap(0)
        buffer2
          .getMap(0)
          .keys
          .foreach((key: String) =>
            if (key != null) {
              mp.put(key, true)
            })
        buffer1(0) = mp
      }

    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row): Int = {
      math.min(buffer.getMap(0).size, maxItems)
    }

  }

}
