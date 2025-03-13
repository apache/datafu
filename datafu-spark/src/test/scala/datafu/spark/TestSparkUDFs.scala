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

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.datafu.types.SparkOverwriteUDAFs
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{expr, _}
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class UdfTests extends FunSuite with DataFrameSuiteBase {

  import spark.implicits._

  /**
    * taken from https://github.com/holdenk/spark-testing-base/issues/234#issuecomment-390150835
    *
    * Solves problem with Hive in Spark 2.3.0 in spark-testing-base
    */
  override def conf: SparkConf =
    super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")

  val logger = LoggerFactory.getLogger(this.getClass)

  val inputSchema = List(
    StructField("col_ord", IntegerType, false),
    StructField("col_str", StringType, true)
  )

  lazy val inputRDD = sc.parallelize(
    Seq(Row(1, null),
        Row(2, ""),
        Row(3, "  "),
        Row(1, "asd4")))

  lazy val df =
    sqlContext.createDataFrame(inputRDD, StructType(inputSchema)).cache


  test("coalesceVal") {

    val actual = df.withColumn("col_str", SparkUDFs.coalesceValUDF($"col_str", lit("newVal")))

    val expected = sqlContext.createDataFrame(sc.parallelize(
        Seq(Row(1, "newVal"),
            Row(2, "newVal"),
            Row(3, "asd3"),
            Row(1, "asd4"))), StructType(inputSchema))

    //assertDataFrameEquals(expected, actual)

    expected.show()

    Assert.assertEquals("asd", SparkUDFs.coalesceVal("asd", "eqw"))
    Assert.assertEquals("eqw", SparkUDFs.coalesceVal(null, "eqw"))
    Assert.assertEquals("asd", SparkUDFs.coalesceVal("asd", null))
    //    Assert.assertEquals(null, SparkUDFs.coalesceVal(null, null))
    Assert.assertEquals("eqw", SparkUDFs.coalesceVal("", "eqw"))
    Assert.assertEquals("asd", SparkUDFs.coalesceVal("asd", ""))
    Assert.assertEquals("", SparkUDFs.coalesceVal(null, ""))

  }

  test("registered_udf") {

    UDFRegister.register(spark.sqlContext)

    //spark.udf.register("coalesceVal", SparkUDFs.coalesceVal _)
    val df = spark.sql("select coalesceVal('','asd')")

    Assert.assertEquals("asd", df.first().get(0))
  }

}
