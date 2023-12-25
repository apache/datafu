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

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

import scala.reflect.runtime.universe._
import scala.collection.mutable.{Map, Set}
import scala.reflect.ClassTag

object Aggregators {

  /**
   * Like Google's MultiSets.
   * Aggregate function that creates a map of key to its count.
   */
  class MultiSet extends Aggregator[String, Map[String, Int], Map[String, Int]] with Serializable {
    
    override def zero: Map[String, Int] = Map[String, Int]()

    override def reduce(buffer: Map[String, Int], newItem: String) : Map[String, Int] = {
      buffer.put(newItem, buffer.getOrElse(newItem, 0) + 1)
      buffer
    }

    override def merge(buffer1: Map[String, Int], buffer2: Map[String, Int]): Map[String, Int] = {
      for (entry <- buffer2.iterator) {
        buffer1.put(entry._1, buffer1.getOrElse(entry._1, 0) + entry._2)
      }
      buffer1
    }

    val ss = SparkSession.builder.getOrCreate()
    import ss.implicits._
    override def finish(reduction: Map[String, Int]): Map[String, Int] = reduction

    def bufferEncoder: Encoder[Map[String, Int]] = implicitly[Encoder[Map[String, Int]]]
    def outputEncoder: Encoder[Map[String, Int]] = implicitly[Encoder[Map[String, Int]]]
  }

  // ----------------------------------------------------------------------------------------

  /**
   * Essentially the same as MultiSet, but gets an Array for input.
   * There is an extra option to limit the number of keys (like @CountDistinctUpTo)
   */

  class MultiArraySet[IN : Ordering : TypeTag]( maxKeys: Int = -1)(implicit t:ClassTag[IN]) extends Aggregator[Array[IN], Map[IN, Int], Map[IN, Int]] with Serializable {

    override def zero: Map[IN, Int] = Map[IN, Int]()

    // Taken from our deprecated @SparkUDAFs.MultiArraySet implementation
    private def limitKeys(mp: Map[IN, Int], factor: Int = 1): Map[IN, Int] = {
      if (maxKeys > 0 && maxKeys * factor < mp.size) {
        val k = mp.toList.map(_.swap).sorted.reverse(maxKeys - 1)._1
        var mp2 = Map[IN, Int]() ++= mp.filter((t: (IN, Int)) =>
          t._2 >= k)
        var toRemove = mp2.size - maxKeys
        if (toRemove > 0) {
          mp2 = mp2.filter((t: (IN, Int)) => {
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
    override def reduce(buffer: Map[IN, Int], newItem: Array[IN]): Map[IN, Int] = {
      for (i <- newItem.iterator) {
        buffer.put(i, buffer.getOrElse(i, 0) + 1)
      }
      limitKeys(buffer, 3)
    }

    override def merge(buffer1: Map[IN, Int], buffer2: Map[IN, Int]): Map[IN, Int] = {
      for (i <- buffer2.iterator) {
        buffer1.put(i._1, buffer1.getOrElse(i._1, 0) + i._2)
      }
      limitKeys(buffer1, 3)
    }

    override def finish(reduction: Map[IN, Int]): Map[IN, Int] = reduction

    implicit val inEncoder : Encoder[IN] = Encoders.kryo[IN]

    def bufferEncoder: Encoder[Map[IN, Int]] = implicitly(ExpressionEncoder[Map[IN, Int]])

    def outputEncoder: Encoder[Map[IN, Int]] = implicitly(ExpressionEncoder[Map[IN, Int]])
  }

  // ----------------------------------------------------------------------------------------

  /**
   * Merge maps of kind string -> set<string>
   */
  class MapSetMerge extends Aggregator[Map[String, Array[String]], Map[String, Set[String]], Map[String, Array[String]]] with Serializable {

      override def zero: Map[String, Set[String]] = Map[String, Set[String]]()

      override def reduce(buffer: Map[String, Set[String]], newItem: Map[String, Array[String]]) : Map[String, Set[String]] = {
        for (entry <- newItem.iterator) {
            buffer.getOrElse(entry._1, Set[String]()) ++= entry._2
        }
        buffer
      }

      override def merge(buffer1: Map[String, Set[String]], buffer2: Map[String, Set[String]]): Map[String, Set[String]] = {
        for (entry <- buffer2.iterator) {
          if (buffer1.isDefinedAt(entry._1)) {
            buffer1(entry._1) ++= entry._2
          } else {
            buffer1(entry._1) = entry._2
          }
        }
        buffer1
      }

      override def finish(reduction: Map[String, Set[String]]): Map[String, Array[String]] = {
        val result = Map[String, Array[String]]()

        for (entry <- reduction.iterator) {
          result.put(entry._1, entry._2.toArray)
        }
        result
      }

      val ss = SparkSession.builder.getOrCreate()

      import ss.implicits._

     // implicit val setEncoder : Encoder[Set[String]] = Encoders.kryo[Set[String]]

    //implicit val setEncoder : Encoder[Set[String]] = implicitly[Encoder[Set[String]]]
    //implicit val arrayEncoder : Encoder[Array[String]] = Encoders.kryo[Array[String]]

      def bufferEncoder: Encoder[Map[String, Set[String]]] = implicitly[Encoder[Map[String,Set[String]]]]
      //def bufferEncoder: Encoder[Map[String, Set[String]]] = implicitly(ExpressionEncoder[Map[String, Set[String]]])
      def outputEncoder: Encoder[Map[String, Array[String]]] = implicitly(ExpressionEncoder[Map[String, Array[String]]])
    }

  // ----------------------------------------------------------------------------------------

  /**
   * Counts number of distinct records, but only up to a preset amount -
   * more efficient than an unbounded count
   */
  class CountDistinctUpTo(maxItems: Int) extends Aggregator[String, Set[String], Int] with Serializable {

    override def zero: Set[String] = Set[String]()

    override def reduce(buffer: Set[String], newItem: String): Set[String] = {
      if (buffer.size < maxItems) buffer += newItem
      buffer
    }

    // it doesn't matter which items get put in our set if we've reached the maximum
    override def merge(buffer1: Set[String], buffer2: Set[String]): Set[String] = {
      if (buffer1.size >= maxItems) {
        buffer1
      } else if (buffer2.size >= maxItems) {
        buffer2
      } else {
        val it = buffer2.iterator
        while (buffer1.size < maxItems && it.hasNext) {
          buffer1 += it.next
        }
        buffer1
      }
    }

    override def finish(reduction: Set[String]): Int = reduction.size

    val ss = SparkSession.builder.getOrCreate()
    import ss.implicits._

    def bufferEncoder: Encoder[Set[String]] = Encoders.kryo[Set[String]]
    //def bufferEncoder: Encoder[Set[String]] = implicitly[Encoder[Set[String]]]

    //def bufferEncoder: Encoder[Set[String]] = ExpressionEncoder()
    def outputEncoder: Encoder[Int] = Encoders.scalaInt
  }
}
