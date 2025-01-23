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
import org.apache.logging.log4j.LogManager
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.datafu.types.SparkOverwriteUDAFs
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types._
import java.io.File
import java.nio.file.{Path, Paths, Files, SimpleFileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes

@RunWith(classOf[JUnitRunner])
class UdafTests extends FunSuite with DataFrameSuiteBase {

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

  def deleteLeftoverFiles(table : String) : Unit = {
   
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

  test("minKeyValue") {
    assertDataFrameNoOrderEquals(
      sqlContext.createDataFrame(List(("b", "asd4"), ("a", "asd1"))),
      df.groupBy($"col_grp".as("_1"))
        .agg(SparkOverwriteUDAFs.minValueByKey($"col_ord", $"col_str").as("_2"))
    )
  }

  case class Exp4(colGrp: String, colOrd: Int, colStr: String, asd: String)

  val minKeyValueWindowExpectedSchema = List(
    StructField("col_grp", StringType, true),
    StructField("col_ord", IntegerType, false),
    StructField("col_str", StringType, true),
    StructField("asd", StringType, true)
  )

  test("minKeyValue window") {
    assertDataFrameNoOrderEquals(
      sqlContext.createDataFrame(
        sc.parallelize(
          Seq(
            Row("b", 1, "asd4", "asd4"),
            Row("a", 1, "asd1", "asd1"),
            Row("a", 2, "asd2", "asd1"),
            Row("a", 3, "asd3", "asd1")
          )),
        StructType(minKeyValueWindowExpectedSchema)
      ),
      df.withColumn("asd",
                    SparkOverwriteUDAFs
                      .minValueByKey($"col_ord", $"col_str")
                      .over(Window.partitionBy("col_grp")))
    )
  }

  test("test_limited_collect_list") {

    val maxSize = 10

    val rows = (1 to 30).flatMap(x => (1 to x).map(n => (x, n, "some-string " + n))).toDF("num1", "num2", "str")

    rows.show(10, false)

    import org.apache.spark.sql.functions._

    val result = rows.groupBy("num1").agg(SparkOverwriteUDAFs.collectLimitedList(expr("struct(*)"), maxSize).as("list"))
      .withColumn("list_size", expr("size(list)"))

    result.show(10, false)

    SparkDFUtils.dedupRandomN(rows,$"num1",10).show(10,false)

    val rows_different = result.filter(s"case when num1 > $maxSize then $maxSize else num1 end != list_size")

    Assert.assertEquals(0, rows_different.count())

  }

  test("test_collectNumberOrderedElements") {
    val nums = Seq(1,3,2,5,4,7,6,9,8)
    val rows = nums.flatMap(x => (1 to x).map(n => (x, "str" + n))).toDF("num", "str")

    import org.apache.spark.sql.functions._
  
    val result = rows.groupBy("num").agg(SparkOverwriteUDAFs.collectNumberOrderedElements(col("str"), 4, ascending = false).as("list"))

    val schema = StructType(List(
      StructField("num", IntegerType, nullable = false),
      StructField("list", ArrayType(StringType, containsNull = false)
    )))

    val expected = sqlContext.createDataFrame(
      sc.parallelize(Seq(
        Row(1, Seq("str1")),
        Row(2, Seq("str2", "str1")),
        Row(3, Seq("str3", "str2", "str1")),
        Row(4, Seq("str4", "str3", "str2", "str1")),
        Row(5, Seq("str5", "str4", "str3", "str2")),
        Row(6, Seq("str6", "str5", "str4", "str3")),
        Row(7, Seq("str7", "str6", "str5", "str4")),
        Row(8, Seq("str8", "str7", "str6", "str5")),
        Row(9, Seq("str9", "str8", "str7", "str6")),
      )), schema)

    assertDataFrameNoOrderEquals(expected, result)
  }
}
