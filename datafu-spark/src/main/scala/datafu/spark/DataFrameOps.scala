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

import org.apache.spark.sql.{Column, DataFrame}

/**
 * implicit class to enable easier usage e.g:
 *
 * df.dedup(..)
 *
 * instead of:
 *
 * SparkDFUtils.dedup(...)
 *
 */
object DataFrameOps {

  implicit class someDataFrameUtils(df: DataFrame) {

    def dedupWithOrder(groupCol: Column, orderCols: Column*): DataFrame =
      SparkDFUtils.dedupWithOrder(df, groupCol, orderCols: _*)

    def dedupTopN(n: Int, groupCol: Column, orderCols: Column*): DataFrame =
      SparkDFUtils.dedupTopN(df, n, groupCol, orderCols: _*)

    def dedupWithCombiner(groupCol: Column,
                          orderByCol: Column,
                          desc: Boolean = true,
                          moreAggFunctions: Seq[Column] = Nil,
                          columnsFilter: Seq[String] = Nil,
                          columnsFilterKeep: Boolean = true): DataFrame =
      SparkDFUtils.dedupWithCombiner(df,
                          groupCol,
                          orderByCol,
                          desc,
                          moreAggFunctions,
                          columnsFilter,
                          columnsFilterKeep)

    def flatten(colName: String): DataFrame = SparkDFUtils.flatten(df, colName)

    def changeSchema(newScheme: String*): DataFrame =
      SparkDFUtils.changeSchema(df, newScheme: _*)

    def joinWithRange(colSingle: String,
                      dfRange: DataFrame,
                      colRangeStart: String,
                      colRangeEnd: String,
                      DECREASE_FACTOR: Long = 2 ^ 8): DataFrame =
      SparkDFUtils.joinWithRange(df,
                                 colSingle,
                                 dfRange,
                                 colRangeStart,
                                 colRangeEnd,
                                 DECREASE_FACTOR)

    def joinWithRangeAndDedup(colSingle: String,
                              dfRange: DataFrame,
                              colRangeStart: String,
                              colRangeEnd: String,
                              DECREASE_FACTOR: Long = 2 ^ 8,
                              dedupSmallRange: Boolean = true): DataFrame =
      SparkDFUtils.joinWithRangeAndDedup(df,
                                         colSingle,
                                         dfRange,
                                         colRangeStart,
                                         colRangeEnd,
                                         DECREASE_FACTOR,
                                         dedupSmallRange)

    def broadcastJoinSkewed(skewed: DataFrame,
                            joinCol: String,
                            numberCustsToBroadcast: Int,
                            filterCnt: Option[Long] = None,
                            joinType: String = "inner"): DataFrame =
      SparkDFUtils.broadcastJoinSkewed(df,
                                       skewed,
                                       joinCol,
                                       numberCustsToBroadcast,
                                       filterCnt,
                                       joinType)

    def joinSkewed(notSkewed: DataFrame,
                   joinExprs: Column,
                   numShards: Int = 1000,
                   joinType: String = "inner"): DataFrame =
      SparkDFUtils.joinSkewed(df, notSkewed, joinExprs, numShards, joinType)
  
    def explodeArray(arrayCol: Column,
                     alias: String) =
      SparkDFUtils.explodeArray(df, arrayCol, alias)

    def dedupRandomN(df: DataFrame, groupCol: Column, maxSize: Int): DataFrame =
      SparkDFUtils.dedupRandomN(df, groupCol, maxSize)
  }
}
