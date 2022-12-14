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

import java.util.{List => JavaList}

import org.apache.spark.sql.datafu.types.SparkOverwriteUDAFs
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}
import scala.language.implicitConversions
import DataFrameOps.columnToColumns
import org.apache.spark.storage.StorageLevel

/**
 * class definition so we could expose this functionality in PySpark
 */
class SparkDFUtilsBridge {

  def dedupWithOrder(df: DataFrame,
            groupCol: Column,
            orderCols: JavaList[Column]): DataFrame = {
    val converted = convertJavaListToSeq(orderCols)
    SparkDFUtils.dedupWithOrder(df = df, groupCol = groupCol, orderCols = converted: _*)
  }

  def dedupTopN(df: DataFrame,
                n: Int,
                groupCol: Column,
                orderCols: JavaList[Column]): DataFrame = {
    val converted = convertJavaListToSeq(orderCols)
    SparkDFUtils.dedupTopN(df = df,
                           n = n,
                           groupCol = groupCol,
                           orderCols = converted: _*)
  }

  def dedupWithCombiner(df: DataFrame,
             groupCol: JavaList[Column],
             orderByCol: JavaList[Column],
             desc: Boolean,
             columnsFilter: JavaList[String],
             columnsFilterKeep: Boolean): DataFrame = {
    val columnsFilter_converted = convertJavaListToSeq(columnsFilter)
    val groupCol_converted = convertJavaListToSeq(groupCol)
    val orderByCol_converted = convertJavaListToSeq(orderByCol)
    SparkDFUtils.dedupWithCombiner(
      df = df,
      groupCol = groupCol_converted,
      orderByCol = orderByCol_converted,
      desc = desc,
      moreAggFunctions = Nil,
      columnsFilter = columnsFilter_converted,
      columnsFilterKeep = columnsFilterKeep
    )
  }

  def changeSchema(df: DataFrame, newScheme: JavaList[String]): DataFrame = {
    val newScheme_converted = convertJavaListToSeq(newScheme)
    SparkDFUtils.changeSchema(df = df, newScheme = newScheme_converted: _*)
  }

  def joinSkewed(dfLeft: DataFrame,
                 dfRight: DataFrame,
                 joinExprs: Column,
                 numShards: Int,
                 joinType: String): DataFrame = {
    SparkDFUtils.joinSkewed(dfLeft = dfLeft,
                            dfRight = dfRight,
                            joinExprs = joinExprs,
                            numShards = numShards,
                            joinType = joinType)
  }

  def broadcastJoinSkewed(notSkewed: DataFrame,
                          skewed: DataFrame,
                          joinCol: String,
                          numRowsToBroadcast: Int,
                          filterCnt: Long,
                          joinType: String): DataFrame = {
    SparkDFUtils.broadcastJoinSkewed(notSkewed = notSkewed,
      skewed = skewed,
      joinCol = joinCol,
      numRowsToBroadcast = numRowsToBroadcast,
      filterCnt = Option(filterCnt),
      joinType)
  }

  def joinWithRange(dfSingle: DataFrame,
                    colSingle: String,
                    dfRange: DataFrame,
                    colRangeStart: String,
                    colRangeEnd: String,
                    DECREASE_FACTOR: Long): DataFrame = {
    SparkDFUtils.joinWithRange(dfSingle = dfSingle,
                               colSingle = colSingle,
                               dfRange = dfRange,
                               colRangeStart = colRangeStart,
                               colRangeEnd = colRangeEnd,
                               DECREASE_FACTOR = DECREASE_FACTOR)
  }

  def joinWithRangeAndDedup(dfSingle: DataFrame,
                            colSingle: String,
                            dfRange: DataFrame,
                            colRangeStart: String,
                            colRangeEnd: String,
                            DECREASE_FACTOR: Long,
                            dedupSmallRange: Boolean): DataFrame = {
    SparkDFUtils.joinWithRangeAndDedup(
      dfSingle = dfSingle,
      colSingle = colSingle,
      dfRange = dfRange,
      colRangeStart = colRangeStart,
      colRangeEnd = colRangeEnd,
      DECREASE_FACTOR = DECREASE_FACTOR,
      dedupSmallRange = dedupSmallRange
    )
  }

  def explodeArray(df: DataFrame, arrayCol: Column, alias: String): DataFrame = {
    SparkDFUtils.explodeArray(df, arrayCol, alias)
  }

  def dedupRandomN(df: DataFrame, groupCol: Column, maxSize: Int): DataFrame = {
    SparkDFUtils.dedupRandomN(df, groupCol, maxSize)
  }

  private def convertJavaListToSeq[T](list: JavaList[T]): Seq[T] = {
    scala.collection.JavaConverters
      .asScalaIteratorConverter(list.iterator())
      .asScala
      .toList
  }
}

object SparkDFUtils {

  /**
    * Used to get the 'latest' record (after ordering according to the provided order columns)
    * in each group.
    * Different from {@link org.apache.spark.sql.Dataset#dropDuplicates} because order matters.
    *
    * @param df DataFrame to operate on
    * @param groupCol column to group by the records
    * @param orderCols columns to order the records according to
    * @return DataFrame representing the data after the operation
    */
  def dedupWithOrder(df: DataFrame, groupCol: Column, orderCols: Column*): DataFrame = {
    dedupTopN(df, 1, groupCol, orderCols: _*)
  }

  /**
    * Used get the top N records (after ordering according to the provided order columns)
    * in each group.
    *
    * @param df DataFrame to operate on
    * @param n number of records to return from each group
    * @param groupCol column to group by the records
    * @param orderCols columns to order the records according to
    * @return DataFrame representing the data after the operation
    */
  def dedupTopN(df: DataFrame,
                n: Int,
                groupCol: Column,
                orderCols: Column*): DataFrame = {
    val w = Window.partitionBy(groupCol).orderBy(orderCols: _*)
    df.withColumn("rn", row_number.over(w)).where(col("rn") <= n).drop("rn")
  }

  /**
    * Used to get the 'latest' record (after ordering according to the provided order columns)
    * in each group.
    * the same functionality as {@link #dedupWithOrder} but implemented using UDAF to utilize
    * map side aggregation.
    * this function should be used in cases when you expect a large number of rows to get combined,
    * as they share the same group column.
    *
    * @param df DataFrame to operate on
    * @param groupCol column to group by the records
    * @param orderByCol column to order the records according to
    * @param desc have the order as desc
    * @param moreAggFunctions more aggregate functions
    * @param columnsFilter columns to filter
    * @param columnsFilterKeep indicates whether we should filter the selected columns 'out'
    *                          or alternatively have only those columns in the result
    * @return DataFrame representing the data after the operation
    */
  def dedupWithCombiner(df: DataFrame,
                        groupCol: Seq[Column],
                        orderByCol: Seq[Column],
                        desc: Boolean = true,
                        moreAggFunctions: Seq[Column] = Nil,
                        columnsFilter: Seq[String] = Nil,
                        columnsFilterKeep: Boolean = true): DataFrame = {
    val newDF =
      if (columnsFilter == Nil) {
        df.withColumn("sort_by_column", struct(orderByCol: _*))
      } else {
        if (columnsFilterKeep) {
          df.withColumn("sort_by_column", struct(orderByCol: _*))
            .select("sort_by_column", columnsFilter: _*)
        } else {
          df.select(
            df.columns
              .filter(colName => !columnsFilter.contains(colName))
              .map(colName => new Column(colName)): _*)
            .withColumn("sort_by_column", struct(orderByCol: _*))
        }
      }

    val aggFunc =
      if (desc) SparkOverwriteUDAFs.maxValueByKey(_: Column, _: Column)
      else SparkOverwriteUDAFs.minValueByKey(_: Column, _: Column)

    val df2 = newDF
      .groupBy(groupCol:_*)
      .agg(aggFunc(expr("sort_by_column"), expr("struct(sort_by_column, *)"))
        .as("h1"),
        struct(lit(1).as("lit_placeholder_col") +: moreAggFunctions: _*)
          .as("h2"))
      .selectExpr("h1.*", "h2.*")
      .drop("lit_placeholder_col")
      .drop("sort_by_column")
    val ns = StructType((df.schema++df2.schema.filter(s2 => !df.schema.map(_.name).contains(s2.name)))
      .filter(s2 => columnsFilter == Nil || (columnsFilterKeep && columnsFilter.contains(s2.name)) || (!columnsFilterKeep && !columnsFilter.contains(s2.name))).toList)

    df2.sparkSession.createDataFrame(df2.rdd,ns)
  }

  /**
    * Returns a DataFrame with the given column (should be a StructType)
    * replaced by its inner fields.
    * This method only flattens a single level of nesting.
    *
    * +-------+----------+----------+----------+
    * |id     |s.sub_col1|s.sub_col2|s.sub_col3|
    * +-------+----------+----------+----------+
    * |123    |1         |2         |3         |
    * +-------+----------+----------+----------+
    *
    * +-------+----------+----------+----------+
    * |id     |sub_col1  |sub_col2  |sub_col3  |
    * +-------+----------+----------+----------+
    * |123    |1         |2         |3         |
    * +-------+----------+----------+----------+
    *
    * @param df DataFrame to operate on
    * @param colName column name for a column of type StructType
    * @return DataFrame representing the data after the operation
    */
  def flatten(df: DataFrame, colName: String): DataFrame = {
    assert(df.schema(colName).dataType.isInstanceOf[StructType],
           s"Column $colName must be of type Struct")
    val outerFields = df.schema.fields.map(_.name).toSet
    val flattenFields = df
      .schema(colName)
      .dataType
      .asInstanceOf[StructType]
      .fields
      .filter(f => !outerFields.contains(f.name))
      .map("`" + colName + "`.`" + _.name + "`")
    df.selectExpr("*" +: flattenFields: _*).drop(colName)
  }

  /**
    * Returns a DataFrame with the column names renamed to the column names in the new schema
    *
    * @param df DataFrame to operate on
    * @param newScheme new column names
    * @return DataFrame representing the data after the operation
    */
  def changeSchema(df: DataFrame, newScheme: String*): DataFrame =
    df.select(df.columns.zip(newScheme).map {
      case (oldCol: String, newCol: String) => col(oldCol).as(newCol)
    }: _*)

  /**
    * Used to perform a join when the right df is relatively small
    * but still too big to fit in memory to perform map side broadcast join.
    * Use cases:
    * a. excluding keys that might be skewed from a medium size list.
    * b. join a big skewed table with a table that has small number of very large rows.
    *
    * @param dfLeft left DataFrame
    * @param dfRight right DataFrame
    * @param joinExprs join expression
    * @param numShards number of shards - number of times to duplicate the right DataFrame
    * @param joinType join type
    * @return joined DataFrame
    */
  def joinSkewed(dfLeft: DataFrame,
                 dfRight: DataFrame,
                 joinExprs: Column,
                 numShards: Int = 10,
                 joinType: String = "inner"): DataFrame = {
    // skew join based on salting
    // salts the left DF by adding another random column and join with the right DF after
    // duplicating it
    val ss = dfLeft.sparkSession
    import ss.implicits._
    val shards = 1.to(numShards).toDF("shard")
    dfLeft
      .withColumn("randLeft", ceil(rand() * numShards))
      .join(dfRight.crossJoin(shards),
            joinExprs and $"randLeft" === $"shard",
            joinType)
      .drop($"randLeft")
      .drop($"shard")
  }

  /**
    * Suitable to perform a join in cases when one DF is skewed and the other is not skewed.
    * splits both of the DFs to two parts according to the skewed keys.
    * 1. Map-join: broadcasts the skewed-keys part of the not skewed DF to the skewed-keys
    *    part of the skewed DF
    * 2. Regular join: between the remaining two parts.
    *
    * @param notSkewed not skewed DataFrame
    * @param skewed skewed DataFrame
    * @param joinCol join column
    * @param numRowsToBroadcast num of rows to broadcast
    * @param filterCnt filter out unskewed rows from the boardcast to ease limit calculation
    * @return DataFrame representing the data after the operation
    */
  def broadcastJoinSkewed(notSkewed: DataFrame,
                          skewed: DataFrame,
                          joinCol: String,
                          numRowsToBroadcast: Int,
                          filterCnt: Option[Long] = None,
                          joinType: String = "inner"): DataFrame = {
    val ss = notSkewed.sparkSession
    import ss.implicits._
    val keyCount = skewed
      .groupBy(joinCol)
      .count()
    val filteredKeyCount = filterCnt.map(cnt => keyCount.filter($"count" >= cnt)).getOrElse(keyCount)


    val skewedKeys = filteredKeyCount
      .sort($"count".desc)
      .limit(numRowsToBroadcast)
      .drop("count")
      .withColumnRenamed(joinCol, "skew_join_key")
      .cache()

    val notSkewedWithSkewIndicator = notSkewed
      .join(broadcast(skewedKeys), $"skew_join_key" === col(joinCol), "left")
      .withColumn("is_skewed_record", col("skew_join_key").isNotNull)
      .drop("skew_join_key")

    // broadcast map-join, sending the notSkewed data
    val bigRecordsJnd =
      broadcast(notSkewedWithSkewIndicator.filter("is_skewed_record"))
        .join(skewed.join(broadcast(skewedKeys), $"skew_join_key" === col(joinCol)).drop("skew_join_key"), List(joinCol), joinType)

    // regular join for the rest
    val skewedWithoutSkewedKeys = skewed
      .join(broadcast(skewedKeys), $"skew_join_key" === col(joinCol), "left")
      .where("skew_join_key is null")
      .drop("skew_join_key")
    val smallRecordsJnd = notSkewedWithSkewIndicator
      .filter("not is_skewed_record")
      .join(skewedWithoutSkewedKeys, List(joinCol), joinType)

    smallRecordsJnd
      .union(bigRecordsJnd)
      .drop("is_skewed_record", "skew_join_key")
  }

  /**
    * Helper function to join a table with point column to a table with range column.
    * For example, join a table that contains specific time in minutes with a table that
    * contains time ranges.
    * The main problem this function addresses is that doing naive explode on the ranges can result
    * in a huge table.
    * requires:
    * 1. point table needs to be distinct on the point column.
    * 2. the range and point columns need to be numeric.
    *
    * TIMES:
    * +-------+
    * |time   |
    * +-------+
    * |11:55  |
    * +-------+
    *
    * TIME RANGES:
    * +----------+---------+----------+
    * |start_time|end_time |desc      |
    * +----------+---------+----------+
    * |10:00     |12:00    | meeting  |
    * +----------+---------+----------+
    * |11:50     |12:15    | lunch    |
    * +----------+---------+----------+
    *
    * OUTPUT:
    * +-------+----------+---------+---------+
    * |time   |start_time|end_time |desc     |
    * +-------+----------+---------+---------+
    * |11:55  |10:00     |12:00    | meeting |
    * +-------+----------+---------+---------+
    * |11:55  |11:50     |12:15    | lunch   |
    * +-------+----------+---------+---------+
    *
    * @param dfSingle DataFrame that contains the point column
    * @param colSingle the point column's name
    * @param dfRange DataFrame that contains the range column
    * @param colRangeStart the start range column's name
    * @param colRangeEnd the end range column's name
    * @param DECREASE_FACTOR resolution factor. instead of exploding the range column directly,
    *                        we first decrease its resolution by this factor
    * @return
    */
  def joinWithRange(dfSingle: DataFrame,
                    colSingle: String,
                    dfRange: DataFrame,
                    colRangeStart: String,
                    colRangeEnd: String,
                    DECREASE_FACTOR: Long): DataFrame = {
    val dfJoined = joinWithRangeInternal(dfSingle,
                                         colSingle,
                                         dfRange,
                                         colRangeStart,
                                         colRangeEnd,
                                         DECREASE_FACTOR)
    dfJoined.drop("range_start",
                  "range_end",
                  "decreased_range_single",
                  "single",
                  "decreased_single",
                  "range_size")
  }

  private def joinWithRangeInternal(dfSingle: DataFrame,
                                    colSingle: String,
                                    dfRange: DataFrame,
                                    colRangeStart: String,
                                    colRangeEnd: String,
                                    DECREASE_FACTOR: Long): DataFrame = {

    import org.apache.spark.sql.functions.udf
    val rangeUDF = udf((start: Long, end: Long) => (start to end).toArray)
    val dfRange_exploded = dfRange
      .withColumn("range_start", col(colRangeStart).cast(LongType))
      .withColumn("range_end", col(colRangeEnd).cast(LongType))
      .withColumn("decreased_range_single",
                  explode(
                    rangeUDF(col("range_start") / lit(DECREASE_FACTOR),
                             col("range_end") / lit(DECREASE_FACTOR))))

    dfSingle
      .withColumn("single", floor(col(colSingle).cast(LongType)))
      .withColumn("decreased_single",
                  floor(col(colSingle).cast(LongType) / lit(DECREASE_FACTOR)))
      .join(dfRange_exploded,
            col("decreased_single") === col("decreased_range_single"),
            "left_outer")
      .withColumn("range_size", expr("(range_end - range_start + 1)"))
      .filter("single>=range_start and single<=range_end")
  }

  /**
    * Run joinWithRange and afterwards run dedup
    *
    * @param dedupSmallRange -  by small/large range
    *
    *  OUTPUT for dedupSmallRange = "true":
    * +-------+----------+---------+---------+
    * |time   |start_time|end_time |desc     |
    * +-------+----------+---------+---------+
    * |11:55  |11:50     |12:15    | lunch   |
    * +-------+----------+---------+---------+
    *
    *  OUTPUT for dedupSmallRange = "false":
    * +-------+----------+---------+---------+
    * |time   |start_time|end_time |desc     |
    * +-------+----------+---------+---------+
    * |11:55  |10:00     |12:00    | meeting |
    * +-------+----------+---------+---------+
    *
    */
  def joinWithRangeAndDedup(dfSingle: DataFrame,
                            colSingle: String,
                            dfRange: DataFrame,
                            colRangeStart: String,
                            colRangeEnd: String,
                            DECREASE_FACTOR: Long,
                            dedupSmallRange: Boolean): DataFrame = {

    val dfJoined = joinWithRangeInternal(dfSingle,
                                         colSingle,
                                         dfRange,
                                         colRangeStart,
                                         colRangeEnd,
                                         DECREASE_FACTOR)

    // "range_start" is here for consistency
    val dfDeduped = if (dedupSmallRange) {
      dedupWithCombiner(dfJoined,
             col(colSingle),
             struct("range_size", "range_start"),
             desc = false)
    } else {
      dedupWithCombiner(dfJoined,
             col(colSingle),
             struct(expr("-range_size"), col("range_start")),
             desc = true)
    }

    dfDeduped.drop("range_start",
                   "range_end",
                   "decreased_range_single",
                   "single",
                   "decreased_single",
                   "range_size")
  }

/**
   * Given an array column that you need to explode into different columns, use this method.
   * This function counts the number of output columns by executing the Spark job internally on the input array column.
   * Consider caching the input dataframe if this is an expensive operation.
   *
   * @param df
   * @param arrayCol
   * @param alias
   * @return
   *
   * input
   * +-----+----------------------------------------+
   * |label|sentence_arr                            |
   * +-----+----------------------------------------+
   * |0.0  |[Hi, I heard, about, Spark]             |
   * |0.0  |[I wish, Java, could use, case classes] |
   * |1.0  |[Logistic, regression, models, are neat]|
   * +-----+----------------------------------------+
   *
   * output
   * +-----+----------------------------------------+--------+----------+---------+------------+
   * |label|sentence_arr                            |token0  |token1    |token2   |token3      |
   * +-----+----------------------------------------+--------+----------+---------+------------+
   * |0.0  |[Hi, I heard, about, Spark]             |Hi      |I heard   |about    |Spark       |
   * |0.0  |[I wish, Java, could use, case classes] |I wish  |Java      |could use|case classes|
   * |1.0  |[Logistic, regression, models, are neat]|Logistic|regression|models   |are neat    |
   * +-----+----------------------------------------+--------+----------+---------+------------+
   */
  def explodeArray(df: DataFrame, arrayCol: Column, alias: String) = {
    val arrSize = df.agg(max(size(arrayCol))).collect()(0).getInt(0)

    val exprs = (0 until arrSize).map(i => arrayCol.getItem(i).alias(s"$alias$i"))
    df.select((col("*") +: exprs):_*)
  }

  /**
    * Used get the random n records in each group. Uses an efficient implementation
    * that doesn't order the data so it can handle large amounts of data.
    *
    * @param df        DataFrame to operate on
    * @param groupCol  column to group by the records
    * @param maxSize   The maximal number of rows per group
    * @return DataFrame representing the data after the operation
    */
  def dedupRandomN(df: DataFrame, groupCol: Column, maxSize: Int): DataFrame = {
    df.groupBy(groupCol).agg(SparkOverwriteUDAFs.collectLimitedList(expr("struct(*)"), maxSize).as("list"))
      .select(groupCol,expr("explode(list)"))
  }
}
