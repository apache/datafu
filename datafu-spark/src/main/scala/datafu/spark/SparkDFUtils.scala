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

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, SparkOverwriteUDAFs, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

object SparkDFUtils extends SparkDFUtilsTrait {

  /**
    * Used get the 'latest' record (after ordering according to the provided order columns) in each group.
    * @param df DataFrame to operate on
    * @param groupCol column to group by the records
    * @param orderCols columns to order the records according to
    * @return DataFrame representing the data after the operation
    */
  override def dedup(df: DataFrame, groupCol: Column, orderCols: Column*): DataFrame = {
    dedupTopN(df, 1, groupCol, orderCols: _*)
  }

  /**
    * Used get the top N records (after ordering according to the provided order columns) in each group.
    * @param df DataFrame to operate on
    * @param n number of records to return from each group
    * @param groupCol column to group by the records
    * @param orderCols columns to order the records according to
    * @return DataFrame representing the data after the operation
    */
  override def dedupTopN(df: DataFrame, n: Int, groupCol: Column, orderCols: Column*): DataFrame = {
    val w = Window.partitionBy(groupCol).orderBy(orderCols: _*)
    val ss = df.sparkSession
    import ss.implicits._
    df.withColumn("rn", row_number.over(w)).where($"rn" <= n).drop("rn")
  }

  /**
    * Used get the 'latest' record (after ordering according to the provided order columns) in each group.
    * @param df DataFrame to operate on
    * @param groupCol column to group by the records
    * @param orderByCol column to order the records according to
    * @param desc have the order as desc
    * @param moreAggFunctions more aggregate functions
    * @param columnsFilter columns to filter
    * @param columnsFilterKeep indicates whether we should filter the selected columns 'out' or alternatively have only
    *                          those columns in the result
    * @return DataFrame representing the data after the operation
    */
  override def dedup2(df: DataFrame, groupCol: Column, orderByCol: Column, desc: Boolean = true, moreAggFunctions: Seq[Column] = Nil, columnsFilter: Seq[String] = Nil, columnsFilterKeep: Boolean = true): DataFrame = {
    val newDF = if (columnsFilter == Nil)
      df.withColumn("sort_by_column", orderByCol)
    else {
      if(columnsFilterKeep)
        df.withColumn("sort_by_column", orderByCol).select("sort_by_column", columnsFilter: _*)
      else
        df.select(df.columns.filter(colName => !columnsFilter.contains(colName)).map(colName => new Column(colName)):_*).withColumn("sort_by_column", orderByCol)
    }


    val aggFunc = if (desc) SparkOverwriteUDAFs.maxValueByKey(_:Column, _:Column)
                  else      SparkOverwriteUDAFs.minValueByKey(_:Column, _:Column)

    val df2 = newDF
      .groupBy(groupCol.as("group_by_col"))
      .agg(aggFunc(expr("sort_by_column"), expr("struct(sort_by_column, *)")).as("h1"),
           struct(lit(1).as("lit_placeholder_col") +: moreAggFunctions: _*).as("h2"))
      .selectExpr("h2.*","h1.*")
      .drop("lit_placeholder_col")
      .drop("sort_by_column")
    df2
  }

  override def flatten(df: DataFrame, colName: String): DataFrame = {
    val outerFields = df.schema.fields.map(_.name).toSet
    val flattenFields = df.schema(colName).dataType.asInstanceOf[StructType].fields.filter(f => !outerFields.contains(f.name)).map("`" + colName + "`.`" + _.name + "`")
    df.selectExpr("*" +: flattenFields: _*).drop(colName)
  }

  /**
  * Returns a DataFrame with the column names renamed to the column names in the new schema
  * @param df DataFrame to operate on
  * @param newScheme new column names
  * @return DataFrame representing the data after the operation
  */
  override def changeSchema(df: DataFrame, newScheme: String*): DataFrame =
    df.select(df.columns.zip(newScheme).map {case (oldCol: String, newCol: String) => col(oldCol).as(newCol)}: _*)

  /**
    * Used to perform a join when the right df is relatively small but doesn't fit to perform broadcast join.
    * Use cases:
    * a. excluding keys that might be skew from a medium size list.
    * b. join a big skewed table with a table that has small number of very big rows.
    * @param dfLeft left DataFrame
    * @param dfRight right DataFrame
    * @param joinExprs join expression
    * @param numShards number of shards
    * @param joinType join type
    * @return DataFrame representing the data after the operation
    */
  override def joinSkewed(dfLeft: DataFrame, dfRight: DataFrame, joinExprs: Column, numShards: Int = 30, joinType: String = "inner"): DataFrame = {
    // skew join based on salting
    // salts the left DF by adding another random column and join with the right DF after duplicating it
    val ss = dfLeft.sparkSession
    import ss.implicits._
    val shards = 1.to(numShards).toDF("shard")
    dfLeft.withColumn("randLeft", ceil(rand() * numShards)).join(dfRight.crossJoin(shards), joinExprs and $"randLeft" === $"shard", joinType).drop($"randLeft").drop($"shard")
  }

  /**
    * Suitable to perform a join in cases when one DF is skewed and the other is not skewed.
    * splits both of the DFs to two parts according to the skewed keys.
    * 1. Map-join: broadcasts the skewed-keys part of the not skewed DF to the skewed-keys part of the skewed DF
    * 2. Regular join: between the remaining two parts.
    * @param notSkewed not skewed DataFrame
    * @param skewed skewed DataFrame
    * @param joinCol join column
    * @param numberCustsToBroadcast num of custs to broadcast
    * @return DataFrame representing the data after the operation
    */
  override def broadcastJoinSkewed(notSkewed: DataFrame, skewed: DataFrame, joinCol: String, numberCustsToBroadcast: Int): DataFrame = {
    val ss = notSkewed.sparkSession
    import ss.implicits._
    val skewedKeys = skewed.groupBy(joinCol).count().sort($"count".desc).limit(numberCustsToBroadcast).drop("count")
                           .withColumnRenamed(joinCol, "skew_join_key").cache()

    val notSkewedWithSkewIndicator = notSkewed.join(broadcast(skewedKeys), $"skew_join_key" === col(joinCol), "left")
      .withColumn("is_skewed_record", col("skew_join_key").isNotNull).drop("skew_join_key").persist(StorageLevel.DISK_ONLY)

    // broadcast map-join, sending the notSkewed data
    val bigRecordsJnd = broadcast(notSkewedWithSkewIndicator.filter("is_skewed_record"))
      .join(skewed, joinCol)

    // regular join for the rest
    val skewedWithoutSkewedKeys = skewed.join(broadcast(skewedKeys), $"skew_join_key" === col(joinCol), "left")
      .where("skew_join_key is null").drop("skew_join_key")
    val smallRecordsJnd = notSkewedWithSkewIndicator.filter("not is_skewed_record")
      .join(skewedWithoutSkewedKeys, joinCol)

    smallRecordsJnd.union(bigRecordsJnd).drop("is_skewed_record", "skew_join_key")
  }

  /**
    * Helper function to join a table with column to a table with range of the same column.
    * For example, ip table with whois data that has range of ips as lines.
    * The main problem which this handles is doing naive explode on the range can result in huge table.
    * requires:
    * 1. single table needs to be distinct on the join column, because there could be a few corresponding ranges so we dedup at the end - we choose the minimal range.
    * 2. the range and single columns to be numeric.
    *
    * IP:
    * +-------+---------+
    * |ip     |ip_val   |
    * +-------+---------+
    * |123    |0.0.0.123|
    * +-------+---------+
    *
    * IP RANGES:
    * +---------+---------+----------+
    * |ip_start |ip_end   |desc      |
    * +---------+---------+----------+
    * |100      |200      | nba1.com |
    * +---------+---------+----------+
    * |50       |300      | nba2.com |
    * +---------+---------+----------+
    *
    * OUTPUT:
    * +---------+---------+---------+---------+----------+
    * |ip       |ip_val   |ip_start |ip_end   |desc      |
    * +---------+---------+---------+---------+----------+
    * |123      |0.0.0.123|100      |200      | nba1.com |
    * +---------+---------+---------+---------+----------+
    *
    *
    * @param dfSingle
    * @param colSingle
    * @param dfRange
    * @param colRangeStart
    * @param colRangeEnd
    * @param DECREASE_FACTOR
    * @return
    */
  override def joinWithRange(dfSingle: DataFrame, colSingle: String, dfRange: DataFrame, colRangeStart: String, colRangeEnd: String, DECREASE_FACTOR: Long): DataFrame = {

    //sqlContext.udf.register("range", (start: Int, end: Int) => (start to end).toArray)
    import org.apache.spark.sql.functions.udf
    val rangeUDF = udf((start: Long, end: Long) => (start to end).toArray)
    val dfRange_exploded = dfRange.withColumn("range_start", col(colRangeStart).cast(LongType))
                                  .withColumn("range_end"  , col(colRangeEnd).cast(LongType))
                                  .withColumn("decreased_range_single", explode(rangeUDF(col("range_start")/lit(DECREASE_FACTOR),
                                                                                         col("range_end"  )/lit(DECREASE_FACTOR))))

    val dfJoined =
    dfSingle.withColumn("single", floor(col(colSingle).cast(LongType)))
            .withColumn("decreased_single", floor(col(colSingle).cast(LongType)/lit(DECREASE_FACTOR)))
            .join(dfRange_exploded, col("decreased_single") === col("decreased_range_single"), "left_outer")
            .withColumn("range_size", expr("(range_end - range_start + 1)"))
            .filter("single>=range_start and single<=range_end")

    dedup2(dfJoined, col(colSingle), col("range_size"), desc = false)
      .drop("range_start", "range_end", "decreased_range_single", "single", "decreased_single", "range_size")

  }
}


trait SparkDFUtilsTrait {
  def dedup(df: DataFrame, groupCol: Column, orderCols: Column*): DataFrame

  def dedupTopN(df: DataFrame, n: Int, groupCol: Column, orderCols: Column*): DataFrame

  def dedup2(df: DataFrame, groupCol: Column, orderByCol: Column, desc: Boolean, moreAggFunctions: Seq[Column], columnsFilter: Seq[String], columnsFilterKeep: Boolean): DataFrame

  def flatten(df: DataFrame, colName: String): DataFrame

  def changeSchema(df: DataFrame, newScheme: String*): DataFrame

  def joinSkewed(dfLeft: DataFrame, dfRight: DataFrame, joinExprs: Column, numShards: Int, joinType: String): DataFrame

  def broadcastJoinSkewed(notSkewed: DataFrame, skewed: DataFrame, joinCol: String, numberCustsToBroadcast: Int): DataFrame

  def joinWithRange(dfSingle: DataFrame, colSingle: String, dfRange: DataFrame, colRangeStart: String, colRangeEnd: String, DECREASE_FACTOR: Long): DataFrame
}
