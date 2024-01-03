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
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.functions.udaf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types._
import java.io.File
import java.nio.file.{Path, Paths, Files, SimpleFileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes

import datafu.spark.Aggregators._

@RunWith(classOf[JUnitRunner])
class TestAggregators extends FunSuite with DataFrameSuiteBase {

  import spark.implicits._

  /**
   * taken from https://github.com/holdenk/spark-testing-base/issues/234#issuecomment-390150835
   *
   * Solves problem with Hive in Spark 2.3.0 in spark-testing-base
   */
  override def conf: SparkConf =
    super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")

  val logger = LogManager.getLogger(this.getClass)

  val inputSchema = List(
    StructField("col_grp", StringType, true),
    StructField("col_ord", IntegerType, false),
    StructField("col_str", StringType, true)
  )

  lazy val inputRDD = sc.parallelize(
    Seq(Row("a", 1, "asd1"),
      Row("a", 2, "asd2"),
      Row("a", 3, "asd3"),
      Row("b", 1, "asd4")))

  lazy val df =
    sqlContext.createDataFrame(inputRDD, StructType(inputSchema)).cache

  case class mapExp(map_col: Map[String, Int])

  case class mapArrExp(map_col: Map[String, Array[String]])

  lazy val defaultDbLocation = spark.sql("describe database default").toDF
    .collect()
    .filter(_.getString(0) == "Location")(0)(1)
    .toString.replace("file:", "")

  def deleteLeftoverFiles(table: String): Unit = {

    val tablePath = Paths.get(defaultDbLocation + File.separator + table)

    // sanity check - only delete files if the path seems to be to a Hive warehouse
    if (defaultDbLocation.endsWith("warehouse") && Files.exists(tablePath)) Files.walkFileTree(tablePath, new SimpleFileVisitor[Path] {
      override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(path)
        FileVisitResult.CONTINUE
      }
    }
    )
  }

  test("test multiset simple") {
    val ms = udaf(new MultiSet())
    val expected: DataFrame =
      sqlContext.createDataFrame(List(mapExp(Map("b" -> 1, "a" -> 3))))
    assertDataFrameEquals(expected, df.agg(ms($"col_grp").as("map_col")))
  }

  val mas = udaf(new MultiArraySet[String]())

  test("test multiarrayset simple") {
    assertDataFrameEquals(
      sqlContext.createDataFrame(List(mapExp(Map("tre" -> 1, "asd" -> 2)))),
      spark
        .sql("select array('asd','tre','asd') arr")
        .groupBy()
        .agg(mas($"arr").as("map_col"))
    )
  }

  test("test multiarrayset all nulls") {
    // end case
    spark.sql("drop table if exists mas_table")
    deleteLeftoverFiles("mas_table")

    spark.sql("create table mas_table (arr array<string>)")
    spark.sql(
      "insert overwrite table mas_table select case when 1=2 then array('asd') end " +
        "from (select 1)")
    spark.sql(
      "insert into table mas_table select case when 1=2 then array('asd') end from (select 1)")
    spark.sql(
      "insert into table mas_table select case when 1=2 then array('asd') end from (select 1)")
    spark.sql(
      "insert into table mas_table select case when 1=2 then array('asd') end from (select 1)")
    spark.sql(
      "insert into table mas_table select case when 1=2 then array('asd') end from (select 1)")

    val expected = sqlContext.createDataFrame(List(mapExp(Map())))

    val actual =
      spark.table("mas_table").groupBy().agg(mas($"arr").as("map_col"))

    assertDataFrameEquals(expected, actual)
  }

  test("test multiarrayset max keys") {
    // max keys case
    spark.sql("drop table if exists mas_table2")
    deleteLeftoverFiles("mas_table2")

    spark.sql("create table mas_table2 (arr array<string>)")
    spark.sql(
      "insert overwrite table mas_table2 select array('asd','dsa') from (select 1)")
    spark.sql(
      "insert into table mas_table2 select array('asd','abc') from (select 1)")
    spark.sql(
      "insert into table mas_table2 select array('asd') from (select 1)")
    spark.sql(
      "insert into table mas_table2 select array('asd') from (select 1)")
    spark.sql(
      "insert into table mas_table2 select array('asd') from (select 1)")
    spark.sql(
      "insert into table mas_table2 select array('asd2') from (select 1)")

    val mas2 = udaf(new Aggregators.MultiArraySet[String](maxKeys = 2))

    assertDataFrameEquals(
      sqlContext.createDataFrame(List(mapExp(Map("dsa" -> 1, "asd" -> 5)))),
      spark.table("mas_table2").groupBy().agg(mas2($"arr").as("map_col")))

    val mas1 = new SparkUDAFs.MultiArraySet[String](maxKeys = 1)
    assertDataFrameEquals(
      sqlContext.createDataFrame(List(mapExp(Map("asd" -> 5)))),
      spark.table("mas_table2").groupBy().agg(mas1($"arr").as("map_col")))
  }

  test("test multiarrayset big input") {
    val N = 100000
    val blah = spark.sparkContext
      .parallelize(1 to N, 20)
      .toDF("num")
      .selectExpr("array('asd',concat('dsa',num)) as arr")
    val mas = udaf(new Aggregators.MultiArraySet[String](maxKeys = 3))
    val time1 = System.currentTimeMillis()
    val mp = blah
      .groupBy()
      .agg(mas($"arr"))
      .collect()
      .map(_.getMap[String, Int](0))
      .head
    Assert.assertEquals(3, mp.size)
    Assert.assertEquals("asd", mp.maxBy(_._2)._1)
    Assert.assertEquals(N, mp.maxBy(_._2)._2)
    val time2 = System.currentTimeMillis()
    logger.info("time took: " + (time2 - time1) / 1000 + " secs")
  }

  test("test mapmerge") {
    val mapMerge = udaf(new MapSetMerge())

    spark.sql("drop table if exists mapmerge_table")
    deleteLeftoverFiles("mapmerge_table")

    spark.sql("create table mapmerge_table (c map<string, array<string>>)")
    spark.sql(
      "insert overwrite table mapmerge_table select map('k1', array('v1')) from (select 1) z")
    spark.sql(
      "insert into table mapmerge_table select map('k1', array('v1')) from (select 1) z")
    spark.sql(
      "insert into table mapmerge_table select map('k2', array('v3')) from (select 1) z")

    assertDataFrameEquals(
      sqlContext.createDataFrame(
        List(mapArrExp(Map("k1" -> Array("v1"), "k2" -> Array("v3"))))),
      spark.table("mapmerge_table").groupBy().agg(mapMerge($"c").as("map_col"))
    )
  }

  case class Exp5(col_grp: String, col_ord: Option[Int])

  case class Exp6(col_ord: Option[Int], col_grp: Option[Int])

  test("countDistinctUpTo") {
    val countDistinctUpTo2 = udaf(new CountDistinctUpTo(2))
    val countDistinctUpTo3 = udaf(new CountDistinctUpTo(3))
    val countDistinctUpTo6 = udaf(new CountDistinctUpTo(6))

    val inputDF = sqlContext.createDataFrame(
      List(
        Exp5("c", Option(1)),
        Exp5("b", Option(1)),
        Exp5("a", Option(1)),
        Exp5("a", Option(2)),
        Exp5("a", Option(3)),
        Exp5("a", Option(4))
      ))

    val results3DF = sqlContext.createDataFrame(
      List(
        Exp5("c", Option(1)),
        Exp5("b", Option(1)),
        Exp5("a", Option(3))
      ))

    val results6DF = sqlContext.createDataFrame(
      List(
        Exp5("c", Option(1)),
        Exp5("b", Option(1)),
        Exp5("a", Option(4))
      ))

    val results2DF = sqlContext.createDataFrame(
      List(
        Exp6(Option(1), Option(2)),
        Exp6(Option(3), Option(1)),
        Exp6(Option(4), Option(1)),
        Exp6(Option(2), Option(1))
      ))

    assertDataFrameEquals(results3DF,
      inputDF
        .groupBy("col_grp")
        .agg(countDistinctUpTo3($"col_ord").as("col_ord")))

    assertDataFrameEquals(results6DF,
      inputDF
        .groupBy("col_grp")
        .agg(countDistinctUpTo6($"col_ord").as("col_ord")))

    assertDataFrameEquals(results2DF, inputDF
      .groupBy("col_ord")
      .agg(countDistinctUpTo2($"col_grp").as("col_grp")))
  }


}

