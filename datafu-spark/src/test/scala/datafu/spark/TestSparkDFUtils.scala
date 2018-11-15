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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import org.scalatest.FunSuite

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scala.collection.mutable.WrappedArray

import org.apache.spark.sql.types._

@RunWith(classOf[JUnitRunner])
class DataFrameOpsTests extends FunSuite with DataFrameSuiteBase {
  
  import DataFrameOps._
  
	import spark.implicits._

 	case class exp4(col2: String, col_grp:String, col_ord:Option[Int], col_str:String)

	case class exp3(col_grp:String, col_ord:Option[Int], col_str:String)

	case class exp2(col_grp:String, col_ord:Option[Int])
	
	case class exp2n(col_grp:String, col_ord:Int)
	
  //var df : DataFrame = spark.read.format("csv").option("header", "true").load("src/test/resources/dedup.csv")
 
  lazy val inputRDD = sc.parallelize(List(("a", 1, "asd1"), ("a", 2, "asd2"), ("a", 3, "asd3"), ("b", 1, "asd4")))
  
  lazy val inputDataFrame = inputRDD.toDF("col_grp", "col_ord", "col_str").cache
  
  test("dedup") {
    val expected : DataFrame = sc.parallelize(List(("b",1),("a",3))).toDF("col_grp", "col_ord")

    assertDataFrameEquals(expected, inputDataFrame.dedup($"col_grp", $"col_ord".desc).select($"col_grp", $"col_ord"))
  }

  test("dedup2_by_int") {

   	val expectedByIntDf : DataFrame = sqlContext.createDataFrame(List(exp4("asd4","b",Option(1),"asd4"),exp4("asd1","a",Option(3),"asd3")))

    val actual = inputDataFrame.dedup2($"col_grp", $"col_ord", moreAggFunctions = Seq(min($"col_str")))

    assertDataFrameEquals(expectedByIntDf, actual)
  }

    test("dedup2_by_string_asc") {

      val actual = inputDataFrame.dedup2($"col_grp", $"col_str", desc = false)
    
      val expectedByStringDf : DataFrame = sqlContext.createDataFrame(List(exp3("b",Option(1),"asd4"),exp3("a",Option(1),"asd1")))

      assertDataFrameEquals(expectedByStringDf, actual)
    }

    test("test_dedup2_by_complex_column") {

      val actual = inputDataFrame.dedup2($"col_grp", expr("cast(concat('-',col_ord) as int)"), desc = false)
    
      val expectedComplex : DataFrame = sqlContext.createDataFrame(List(exp3("b",Option(1),"asd4"),exp3("a",Option(3),"asd3")))

      assertDataFrameEquals(expectedComplex, actual)
    }

    case class inner(col_grp:String, col_ord:Int)
    
    case class expComplex(
        col_grp: String,
        col_ord: Option[Int],
        col_str: String,
        arr_col: Array[String],
        struct_col: inner,
        map_col: Map[String, Int]
    )
    
    test("test_dedup2_with_other_complex_column") {

      val actual = inputDataFrame.withColumn("arr_col", expr("array(col_grp, col_ord)"))
                  .withColumn("struct_col", expr("struct(col_grp, col_ord)"))
                  .withColumn("map_col", expr("map(col_grp, col_ord)"))
                  .withColumn("map_col_blah", expr("map(col_grp, col_ord)"))
                  .dedup2($"col_grp", expr("cast(concat('-',col_ord) as int)"))
                  .drop("map_col_blah")

      val expected : DataFrame = sqlContext.createDataFrame(List(
          expComplex("b", Option(1), "asd4", Array("b","1"), inner("b",1), Map("b" -> 1)),
          expComplex("a", Option(1), "asd1", Array("a","1"), inner("a",1), Map("a" -> 1))
      ))
                  
//      compare(expected, actual)
      
      assertDataFrameEquals(expected, actual)
    }

    test("test_dedup_top_n") {
      val actual = inputDataFrame.dedupTopN(2, $"col_grp", $"col_ord".desc).select($"col_grp", $"col_ord")
      
      val expected = sqlContext.createDataFrame(List(exp2n("b",1), exp2n("a",3), exp2n("a",2)))
      
       assertDataFrameEquals(expected, actual)
    }

   	case class expRangeJoin(col_grp:String, col_ord:Option[Int], col_str:String, start:Option[Int], end:Option[Int], desc:String)
  
    test("join_with_range") {
      val df = sc.parallelize(List(("a", 1, "asd1"), ("a", 2, "asd2"), ("a", 3, "asd3"), ("b", 1, "asd4"))).toDF("col_grp", "col_ord", "col_str")
   	  val dfr = sc.parallelize(List((1, 2,"asd1"), (1, 4, "asd2"), (3, 5,"asd3"), (3, 10,"asd4"))).toDF("start", "end", "desc")
  
      val expected = sqlContext.createDataFrame(List(
          expRangeJoin("b",Option(1),"asd4",Option(1),Option(2),"asd1"),
          expRangeJoin("a",Option(3),"asd3",Option(3),Option(5),"asd3"),
          expRangeJoin("a",Option(2),"asd2",Option(1),Option(2),"asd1")

      ))
      
      val actual = df.joinWithRange("col_ord", dfr, "start", "end")
      
      assertDataFrameEquals(expected, actual)
  }
  
    test("broadcastJoinSkewed") {
      val skewedList = List(("1", "a"), ("1", "b"), ("1", "c"), ("1", "d"), ("1", "e"),("2", "k"),("0", "k"))
      val skewed = sqlContext.createDataFrame(skewedList).toDF("key", "val_skewed")
      val notSkewed = sqlContext.createDataFrame((1 to 10).map(i => (i.toString, s"str$i"))).toDF("key", "val")

      val expected = sqlContext.createDataFrame(List(
          ("2","str2", "k"),
          ("1","str1", "e"),
          ("1","str1", "d"),
          ("1","str1", "c"),
          ("1","str1", "b"),
          ("1","str1", "a")
      )).toDF("key","val","val_skewed")

      val actual1 = notSkewed.broadcastJoinSkewed(skewed,"key", 1)

      assertDataFrameEquals(expected, actual1)
      
      val actual2 = notSkewed.broadcastJoinSkewed(skewed,"key", 2)

      assertDataFrameEquals(expected, actual2)
    }
  
    // because of nulls in expected data, an actual schema needs to be used
    case class expJoinSkewed(str1:String, str2:String, str3:String, str4:String )
  
    test("joinSkewed") {
      val skewedList = List(("1", "a"), ("1", "b"), ("1", "c"), ("1", "d"), ("1", "e"),("2", "k"),("0", "k"))
      val skewed = sqlContext.createDataFrame(skewedList).toDF("key", "val_skewed")
      val notSkewed = sqlContext.createDataFrame((1 to 10).map(i => (i.toString, s"str$i"))).toDF("key", "val")
  
      val actual1 = skewed.as("a").joinSkewed(notSkewed.as("b"),expr("a.key = b.key"), 3)
  
      val expected1 = sqlContext.createDataFrame(List(
          ("1","a","1","str1"),
          ("1","b","1","str1"),
          ("1","c","1","str1"),
          ("1","d","1","str1"),
          ("1","e","1","str1"),
          ("2","k","2","str2")
      )).toDF("key","val_skewed","key","val")
  
      assertDataFrameEquals(expected1, actual1)
      
      val actual2 = skewed.as("a").joinSkewed(notSkewed.as("b"),expr("a.key = b.key"), 3, "left_outer")
  
      val expected2 = sqlContext.createDataFrame(List(
          expJoinSkewed("1","a","1","str1"),
          expJoinSkewed("1","b","1","str1"),
          expJoinSkewed("1","c","1","str1"),
          expJoinSkewed("1","d","1","str1"),
          expJoinSkewed("1","e","1","str1"),
          expJoinSkewed("2","k","2","str2"),
          expJoinSkewed("0","k",null,null)
      )).toDF("key","val_skewed","key","val")
  
      assertDataFrameEquals(expected2, actual2)
    }

    test("test_changeSchema") {

      val actual = inputDataFrame.changeSchema("fld1","fld2", "fld3")
    
      val expected : DataFrame = inputRDD.toDF("fld1","fld2", "fld3")

      actual.show
      
      assertDataFrameEquals(expected, actual)
    }

    test("test_flatten") {

      val input = inputDataFrame.withColumn("struct_col", expr("struct(col_grp, col_ord)")).select("struct_col")
    
      val expected : DataFrame = inputDataFrame.select("col_grp", "col_ord")
      
      val actual = input.flatten("struct_col")
      
      assertDataFrameEquals(expected, actual)
    }

    def compare(expected: DataFrame, actual: DataFrame) = {
      println ("expected: " + expected.schema)
      
      expected.show
      
      println ("actual: " + actual.schema)
      
      actual.show
      
      assert("Schemas not equal!!!", expected.schema, actual.schema)
      
      assertDataFrameEquals(expected, actual)
    }
}