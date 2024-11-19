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
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


@RunWith(classOf[JUnitRunner])
class DataFrameOpsTests extends FunSuite with DataFrameSuiteBase {

  import DataFrameOps._

  import spark.implicits._

  val inputSchema = List(
    StructField("col_grp", StringType, true),
    StructField("col_ord", IntegerType, false),
    StructField("col_str", StringType, true)
  )

  val dedupSchema = List(
    StructField("col_grp", StringType, true),
    StructField("col_ord", IntegerType, false)
  )

  lazy val inputRDD = sc.parallelize(
    Seq(Row("a", 1, "asd1"),
        Row("a", 2, "asd2"),
        Row("a", 3, "asd3"),
        Row("b", 1, "asd4")))

  lazy val inputDataFrame =
    sqlContext.createDataFrame(inputRDD, StructType(inputSchema)).cache

  test("dedup") {
    val expected: DataFrame =
      sqlContext.createDataFrame(sc.parallelize(Seq(Row("b", 1), Row("a", 3))),
                                 StructType(dedupSchema))

    assertDataFrameNoOrderEquals(expected,
                          inputDataFrame
                            .dedupWithOrder($"col_grp", $"col_ord".desc)
                            .select($"col_grp", $"col_ord"))
  }

  case class dedupExp( col_grp: String,
                       col_ord: Int,
                       col_str: String,
                       col2: String)

  test("dedup2_by_int") {

    val expectedByIntDf: DataFrame = sqlContext.createDataFrame(
      List(dedupExp("b", 1, "asd4", "asd4"),
        dedupExp("a", 3, "asd3", "asd1")))

    val actual = inputDataFrame.dedupWithCombiner($"col_grp",
                                       $"col_ord",
                                       moreAggFunctions = Seq(min($"col_str")))

    assertDataFrameNoOrderEquals(expectedByIntDf, actual)
  }

  case class dedupExp2(col_grp: String, col_ord: Int, col_str: String)

  test("dedup2_by_string_asc") {

    val actual = inputDataFrame.dedupWithCombiner($"col_grp", $"col_str", desc = false)

    val expectedByStringDf: DataFrame = sqlContext.createDataFrame(
      List(dedupExp2("b", 1, "asd4"),
        dedupExp2("a", 1, "asd1")))

    assertDataFrameNoOrderEquals(expectedByStringDf, actual)
  }

  test("dedup2_with_filter") {

    val df = sqlContext.createDataFrame(
      Seq(("a", 2, "aa12", "a"),
        ("a", 1, "aa11", "a"),
        ("b", 2, "ab32", "a"),
        ("b", 1, "ba11", "a"))
    ).toDF("col_grp", "col_ord", "col_str", "filter_col")

    // Test case 1 - filter keep false
    val actual1 = df.dedupWithCombiner($"col_grp",
      $"col_ord",
      desc = false,
      columnsFilter = List("filter_col"),
      columnsFilterKeep = false)

    val expectedFilter1: DataFrame = sqlContext.createDataFrame(
      Seq(("a", 1, "aa11"),
        ("b", 1, "ba11"))
    ).toDF("col_grp", "col_ord", "col_str")

    assertDataFrameNoOrderEquals(expectedFilter1, actual1)

    // Test case 2 - filter keep true
    val actual2 = df.dedupWithCombiner($"col_grp",
      $"col_ord",
      desc = false,
      columnsFilter = List("col_grp", "col_ord", "filter_col"),
      columnsFilterKeep = true)

    val expectedFilter2: DataFrame = sqlContext.createDataFrame(
      Seq(("a", 1, "a"),
        ("b", 1, "a"))
    ).toDF("col_grp", "col_ord", "filter_col")

    assertDataFrameNoOrderEquals(expectedFilter2, actual2)
  }

  test("test_dedup2_by_complex_column") {

    val actual = inputDataFrame.dedupWithCombiner($"col_grp",
                                       expr("cast(concat('-',col_ord) as int)"),
                                       desc = false)

    val expectedComplex: DataFrame = sqlContext.createDataFrame(
      List(dedupExp2("b", 1, "asd4"),
        dedupExp2("a", 3, "asd3")))

    assertDataFrameNoOrderEquals(expectedComplex, actual)
  }

  test("test_dedup2_by_multi_column") {

    val df = sqlContext.createDataFrame(
          Seq(("a", "a", 1, 2, "aa12", "a"),
            ("a", "a", 1, 1, "aa11", "a"),
            ("a", "a", 2, 1, "aa21", "a"),
            ("a", "b", 3, 2, "ab32", "a"),
            ("b", "a", 1, 1, "ba11", "a"))
        ).toDF("col_grp1", "col_grp2", "col_ord1", "col_ord2", "col_str", "col_to_ignore")

    val actual = df.dedupWithCombiner(List($"col_grp1", $"col_grp2"),
                                      List($"col_ord1", $"col_ord2"),
                                      desc = false,
                                      columnsFilter = List("col_to_ignore"),
                                      columnsFilterKeep = false)

    val expectedMulti: DataFrame = sqlContext.createDataFrame(
      Seq(("a", "a", 1, 1, "aa11"),
        ("a", "b", 3, 2, "ab32"),
        ("b", "a", 1, 1, "ba11"))
      ).toDF("col_grp1", "col_grp2", "col_ord1", "col_ord2", "col_str")

    assertDataFrameNoOrderEquals(expectedMulti, actual)

  }

  case class Inner(col_grp: String, col_ord: Int)

  case class expComplex(
                         col_grp: String,
                         col_ord: Int,
                         col_str: String,
                         arr_col: Array[String],
                         struct_col: Inner,
                         map_col: Map[String, Int]
  )

  test("test_dedup2_with_other_complex_column") {

    val df = inputDataFrame
      .withColumn("arr_col", expr("array(col_grp, col_ord)"))
      .withColumn("struct_col", expr("struct(col_grp, col_ord)"))
      .withColumn("map_col", expr("map(col_grp, col_ord)"))
      .withColumn("map_col_blah", expr("map(col_grp, col_ord)"))

    val schema = df.drop("map_col_blah").schema

    val actual = df.dedupWithCombiner($"col_grp", expr("cast(concat('-',col_ord) as int)"))
      .drop("map_col_blah").orderBy("col_grp")

    val expected: DataFrame = sqlContext.createDataFrame(
      sqlContext.createDataFrame(
      Seq(
         expComplex("a",
                   1,
                   "asd1",
                   Array("a", "1"),
                   Inner("a", 1),
                   Map("a" -> 1)),
         expComplex("b",
                   1,
                   "asd4",
                   Array("b", "1"),
                   Inner("b", 1),
                   Map("b" -> 1)),
      )).rdd, schema)

    assertDataFrameEquals(expected, actual)
  }

  val dedupTopNExpectedSchema = List(
    StructField("col_grp", StringType, true),
    StructField("col_ord", IntegerType, false)
  )

  test("test_dedup_top_n") {
    val actual = inputDataFrame
      .dedupTopN(2, $"col_grp", $"col_ord".desc)
      .select($"col_grp", $"col_ord")

    val expected = sqlContext.createDataFrame(
      sc.parallelize(Seq(Row("b", 1), Row("a", 3), Row("a", 2))),
      StructType(dedupTopNExpectedSchema))

    assertDataFrameNoOrderEquals(expected, actual)
  }

  val schema2 = List(
    StructField("start", IntegerType, false),
    StructField("end", IntegerType, false),
    StructField("desc", StringType, true)
  )

  val expectedSchemaRangedJoin = List(
    StructField("col_grp", StringType, true),
    StructField("col_ord", IntegerType, false),
    StructField("col_str", StringType, true),
    StructField("start", IntegerType, true),
    StructField("end", IntegerType, true),
    StructField("desc", StringType, true)
  )

  test("join_with_range") {
    val joinWithRangeDataFrame =
      sqlContext.createDataFrame(sc.parallelize(
                                   Seq(Row(1, 2, "asd1"),
                                       Row(1, 4, "asd2"),
                                       Row(3, 5, "asd3"),
                                       Row(3, 10, "asd4"))),
                                 StructType(schema2))

    val expected = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("b", 1, "asd4", 1, 2, "asd1"),
          Row("a", 2, "asd2", 1, 2, "asd1"),
          Row("a", 1, "asd1", 1, 2, "asd1"),
          Row("b", 1, "asd4", 1, 4, "asd2"),
          Row("a", 3, "asd3", 1, 4, "asd2"),
          Row("a", 2, "asd2", 1, 4, "asd2"),
          Row("a", 1, "asd1", 1, 4, "asd2"),
          Row("a", 3, "asd3", 3, 5, "asd3"),
          Row("a", 3, "asd3", 3, 10, "asd4")
        )),
      StructType(expectedSchemaRangedJoin)
    )

    val actual = inputDataFrame.joinWithRange("col_ord",
                                              joinWithRangeDataFrame,
                                              "start",
                                              "end")

    assertDataFrameEquals(expected, actual)
  }

  val expectedSchemaRangedJoinWithDedup = List(
    StructField("col_grp", StringType, true),
    StructField("col_ord", IntegerType, false),
    StructField("col_str", StringType, true),
    StructField("start", IntegerType, true),
    StructField("end", IntegerType, true),
    StructField("desc", StringType, true)
  )

  test("join_with_range_and_dedup") {
    val df = sc
      .parallelize(
        List(("a", 1, "asd1"),
             ("a", 2, "asd2"),
             ("a", 3, "asd3"),
             ("b", 1, "asd4")))
      .toDF("col_grp", "col_ord", "col_str")
    val dfr = sc
      .parallelize(
        List((1, 2, "asd1"), (1, 4, "asd2"), (3, 5, "asd3"), (3, 10, "asd4")))
      .toDF("start", "end", "desc")

    val expected = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("b", 1, "asd4", 1, 2, "asd1"),
          Row("a", 3, "asd3", 3, 5, "asd3"),
          Row("a", 2, "asd2", 1, 2, "asd1")
        )),
      StructType(expectedSchemaRangedJoinWithDedup)
    )

    val actual = df.joinWithRangeAndDedup("col_ord", dfr, "start", "end")

    assertDataFrameNoOrderEquals(expected, actual)
  }

  test("randomJoinSkewedTests") {
    def makeSkew(i: Int): Int = {
      if (i < 200) 10 else 50
    }

    val skewed = sqlContext.createDataFrame((1 to 500).map(i => ((Math.random * makeSkew(i)).toInt, s"str$i")))
      .toDF("key", "val_skewed")
    val notSkewed = sqlContext
      .createDataFrame((1 to 500).map(i => ((Math.random * 50).toInt, s"str$i")))
      .toDF("key", "val")

    val expected = notSkewed.join(skewed, Seq("key")).sort($"key", $"val", $"val_skewed")
    val actual1 = notSkewed.broadcastJoinSkewed(skewed, "key", 1).sort($"key", $"val", $"val_skewed")
    assertDataFrameEquals(expected, actual1)

    val leftExpected = notSkewed.join(skewed, Seq("key"), "left").sort($"key", $"val", $"val_skewed")
    val actual2 = notSkewed.broadcastJoinSkewed(skewed, "key", 1, joinType = "left").sort($"key", $"val", $"val_skewed")
    assertDataFrameEquals(leftExpected, actual2)

    val rightExpected = notSkewed.join(skewed, Seq("key"), "right").sort($"key", $"val", $"val_skewed")
    val actual3 = notSkewed.broadcastJoinSkewed(skewed, "key", 2, joinType = "right").sort($"key", $"val", $"val_skewed")
    assertDataFrameEquals(rightExpected, actual3)
  }

  // because of nulls in expected data, an actual schema needs to be used
  case class expJoinSkewed(str1: String,
                           str2: String,
                           str3: String,
                           str4: String)

  test("joinSkewed") {
    val skewedList = List(("1", "a"),
                          ("1", "b"),
                          ("1", "c"),
                          ("1", "d"),
                          ("1", "e"),
                          ("2", "k"),
                          ("0", "k"))
    val skewed =
      sqlContext.createDataFrame(skewedList).toDF("key", "val_skewed")
    val notSkewed = sqlContext
      .createDataFrame((1 to 10).map(i => (i.toString, s"str$i")))
      .toDF("key", "val")

    val actual1 =
      skewed.as("a").joinSkewed(notSkewed.as("b"), expr("a.key = b.key"), 3)

    val expected1 = sqlContext
      .createDataFrame(
        List(
          ("1", "a", "1", "str1"),
          ("1", "b", "1", "str1"),
          ("1", "c", "1", "str1"),
          ("1", "d", "1", "str1"),
          ("1", "e", "1", "str1"),
          ("2", "k", "2", "str2")
        ))
      .toDF("key", "val_skewed", "key", "val")

    // assertDataFrameEquals cares about order but we don't
    assertDataFrameEquals(expected1, actual1.sort($"val_skewed"))

    val actual2 = skewed
      .as("a")
      .joinSkewed(notSkewed.as("b"), expr("a.key = b.key"), 3, "left_outer")

    val expected2 = sqlContext
      .createDataFrame(
        List(
          expJoinSkewed("1", "a", "1", "str1"),
          expJoinSkewed("1", "b", "1", "str1"),
          expJoinSkewed("1", "c", "1", "str1"),
          expJoinSkewed("1", "d", "1", "str1"),
          expJoinSkewed("1", "e", "1", "str1"),
          expJoinSkewed("2", "k", "2", "str2"),
          expJoinSkewed("0", "k", null, null)
        ))
      .toDF("key", "val_skewed", "key", "val")

    // assertDataFrameEquals cares about order but we don't
    assertDataFrameEquals(expected2, actual2.sort($"val_skewed"))
  }

  val changedSchema = List(
    StructField("fld1", StringType, true),
    StructField("fld2", IntegerType, false),
    StructField("fld3", StringType, true)
  )

  test("test_changeSchema") {

    val actual = inputDataFrame.changeSchema("fld1", "fld2", "fld3")

    val expected =
      sqlContext.createDataFrame(inputRDD, StructType(changedSchema))

    assertDataFrameEquals(expected, actual)
  }

  test("test_flatten") {

    val input = inputDataFrame
      .withColumn("struct_col", expr("struct(col_grp, col_ord)"))
      .select("struct_col")

    val expected: DataFrame = inputDataFrame.select("col_grp", "col_ord")

    val actual = input.flatten("struct_col")

    assertDataFrameEquals(expected, actual)
  }

  test("test_explode_array") {

    val input = spark.createDataFrame(Seq(
      (0.0, Seq("Hi", "I heard", "about", "Spark")),
      (0.0, Seq("I wish", "Java", "could use", "case", "classes")),
      (1.0, Seq("Logistic", "regression", "models", "are neat")),
      (0.0, Seq()),
      (1.0, null)
    )).toDF("label", "sentence_arr")

    val actual = input.explodeArray($"sentence_arr", "token")

    val expected = spark.createDataFrame(Seq(
      (0.0, Seq("Hi", "I heard", "about", "Spark"),"Hi", "I heard", "about", "Spark",null),
      (0.0, Seq("I wish", "Java", "could use", "case", "classes"),"I wish", "Java", "could use", "case", "classes"),
      (1.0, Seq("Logistic", "regression", "models", "are neat"),"Logistic", "regression", "models", "are neat",null),
      (0.0, Seq(),null,null,null,null,null),
      (1.0, null,null,null,null,null,null)
    )).toDF("label", "sentence_arr","token0","token1","token2","token3","token4")

    assertDataFrameEquals(expected, actual)
  }
  
  test("test_dedup_by-all_except") {

    val df = spark.createDataFrame(
      Seq(("a", "a", 1, 2, "aa12", "a"),
        ("a", "a", 1, 1, "aa11", "a"),
        ("a", "a", 2, 1, "aa21", "a"),
        ("a", "b", 3, 2, "ab32", "a"),
        ("b", "a", 1, 1, "ba11", "a"))
    ).toDF("col_grp1", "col_grp2", "col_grp3", "col_grp4", "col_grp5", "col_grp6")

    val noChange = df.dedupByAllExcept("col_grp1")

    assertDataFrameNoOrderEquals(df, noChange)

    val df2 = spark.createDataFrame(
      Seq(("a", "a"),
        ("b", "a"),
        ("c", "c"),
        ("d", "d"),
        ("e", "e"))
    ).toDF("col_grp1", "col_grp2")

    val loseRow = df2.dedupByAllExcept("col_grp1")

    val expected = spark.createDataFrame(
      Seq(("b", "a"),
        ("c", "c"),
        ("d", "d"),
        ("e", "e"))
    ).toDF("col_grp1", "col_grp2")

   assertDataFrameNoOrderEquals(expected, loseRow)
  }
}
