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
package org.apache.spark.sql.datafu.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, DeclarativeAggregate, ImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryComparison, ExpectsInputTypes, Expression, GreaterThan, If, IsNull, LessThan, Literal}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType}

import scala.collection.generic.Growable
import scala.collection.mutable

object SparkOverwriteUDAFs {
  def minValueByKey(key: Column, value: Column): Column =
    Column(MinValueByKey(key.expr, value.expr).toAggregateExpression(false))
  def maxValueByKey(key: Column, value: Column): Column =
    Column(MaxValueByKey(key.expr, value.expr).toAggregateExpression(false))
  def collectLimitedList(e: Column, maxSize: Int): Column =
    Column(CollectLimitedList(e.expr, howMuchToTake = maxSize).toAggregateExpression(false))
}

case class MinValueByKey(child1: Expression, child2: Expression)
    extends ExtramumValueByKey(child1, child2, LessThan)
case class MaxValueByKey(child1: Expression, child2: Expression)
    extends ExtramumValueByKey(child1, child2, GreaterThan)

abstract class ExtramumValueByKey(
    child1: Expression,
    child2: Expression,
    bComp: (Expression, Expression) => BinaryComparison)
    extends DeclarativeAggregate
    with ExpectsInputTypes {

  override def children: Seq[Expression] = child1 :: child2 :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child2.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child1.dataType, "function minmax")

  private lazy val minmax = AttributeReference("minmax", child1.dataType)()
  private lazy val data = AttributeReference("data", child2.dataType)()

  override lazy val aggBufferAttributes
    : Seq[AttributeReference] = minmax :: data :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(null, child1.dataType),
    Literal.create(null, child2.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] =
    chooseKeyValue(minmax, data, child1, child2)

  override lazy val mergeExpressions: Seq[Expression] =
    chooseKeyValue(minmax.left, data.left, minmax.right, data.right)

  private def chooseKeyValue(key1: Expression,
                     value1: Expression,
                     key2: Expression,
                     value2: Expression) = Seq(
    If(IsNull(key1),
       key2,
       If(IsNull(key2), key1, If(bComp(key1, key2), key1, key2))),
    If(IsNull(key1),
       value2,
       If(IsNull(key2), value1, If(bComp(key1, key2), value1, value2)))
  )

  override lazy val evaluateExpression: AttributeReference = data
}

/** *
 *
 * This code is copied from CollectList, just modified the method it extends
 *
 */
case class CollectLimitedList(child: Expression,
                              mutableAggBufferOffset: Int = 0,
                              inputAggBufferOffset: Int = 0,
                              howMuchToTake: Int = 10) extends LimitedCollect[mutable.ArrayBuffer[Any]](howMuchToTake) {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def prettyName: String = "collect_limited_list"

}

/** *
 *
 * This modifies the collect list / set to keep only howMuchToTake random elements
 *
 */
abstract class LimitedCollect[T <: Growable[Any] with Iterable[Any]](howMuchToTake: Int) extends Collect[T] with Serializable {

  override def update(buffer: T, input: InternalRow): T = {
    if (buffer.size < howMuchToTake)
      super.update(buffer, input)
    else
      buffer
  }

  override def merge(buffer: T, other: T): T = {
    if (buffer.size == howMuchToTake)
      buffer
    else if (other.size == howMuchToTake)
      other
    else {
      val howMuchToTakeFromOtherBuffer = howMuchToTake - buffer.size
      buffer ++= other.take(howMuchToTakeFromOtherBuffer)
    }
  }
}
