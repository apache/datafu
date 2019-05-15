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

import java.io.File

import scala.util.Try

import com.holdenkarau.spark.testing.Utils
import org.junit._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestScalaPythonBridge {

  val logger = LoggerFactory.getLogger(this.getClass)

  def getNewRunner(): ScalaPythonBridgeRunner = {
    val runner = ScalaPythonBridgeRunner()
    runner.runPythonFile("pyspark_utils/init_spark_context.py")
    runner
  }

  def getNewSparkSession(): SparkSession = {

    val tempDir = Utils.createTempDir()
    val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
    val localWarehousePath = new File(tempDir, "wharehouse").getCanonicalPath
    val pythonPath =
      PythonPathsManager.getAbsolutePaths().mkString(File.pathSeparator)
    logger.info("Creating SparkConf with PYTHONPATH: " + pythonPath)
    val sparkConf = new SparkConf()
      .setMaster("local[1]")
      .set("spark.sql.warehouse.dir", localWarehousePath)
      .set("javax.jdo.option.ConnectionURL",
           s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
      .setExecutorEnv(Seq(("PYTHONPATH", pythonPath)))
      .setAppName("Spark Unit Test")

    val builder = SparkSession.builder().config(sparkConf).enableHiveSupport()
    val spark = builder.getOrCreate()

    spark
  }
}

@RunWith(classOf[JUnitRunner])
class TestScalaPythonBridge extends FunSuite {

  private val spark = TestScalaPythonBridge.getNewSparkSession
  private lazy val runner = TestScalaPythonBridge.getNewRunner()

  def assertTable(tableName: String, expected: String): Unit =
    Assert.assertEquals(
      expected,
      spark.table(tableName).collect().sortBy(_.toString).mkString(", "))

  test("pyfromscala.py") {

    import spark.implicits._

    val dfin = spark.sparkContext.parallelize(1 to 10).toDF("num")
    dfin.createOrReplaceTempView("dfin")

    runner.runPythonFile("python_tests/pyfromscala.py")

    // try to invoke python udf from scala code
    assert(
      spark
        .sql("select magic('python_udf')")
        .collect()
        .mkString(",") == "[python_udf magic]")

    assertTable("dfout",
                "[10], [12], [14], [16], [18], [20], [2], [4], [6], [8]")
    assertTable("dfout2",
                "[16], [24], [32], [40], [48], [56], [64], [72], [80], [8]")
    assertTable("stats", "[a,0.1], [b,2.0]")
  }

  test("pyfromscala_with_error.py") {
    val t = Try(runner.runPythonFile("python_tests/pyfromscala_with_error.py"))
    assert(t.isFailure)
    assert(t.failed.get.isInstanceOf[RuntimeException])
  }

  test("SparkDFUtilsBridge") {
    runner.runPythonFile("python_tests/df_utils_tests.py")
    assertTable("dedup", "[a,Alice,34], [b,Bob,36], [c,Zoey,36]")
    assertTable(
      "dedupTopN",
      "[a,Alice,34], [a,Sara,33], [b,Bob,36], [b,Charlie,30], [c,Fanny,36], [c,Zoey,36]")
    assertTable("dedup2", "[a,34], [b,36], [c,36]")
    assertTable(
      "changeSchema",
      "[a,Alice,34], [a,Sara,33], [b,Bob,36], [b,Charlie,30], [c,David,29], [c,Esther,32], " +
        "[c,Fanny,36], [c,Zoey,36]")
    assertTable("joinSkewed", "[a,Laura,34,a,1], [a,Stephani,33,a,1]")
    assertTable("broadcastJoinSkewed", "[a,Laura,34,1], [a,Stephani,33,1]")
    assertTable("joinWithRange",
                "[a,Laura,34,a,34,36], [b,Margaret,36,a,34,36]")
    assertTable("joinWithRangeAndDedup",
                "[a,Laura,34,a,34,36], [b,Margaret,36,a,34,36]")
  }

}

class ExampleFiles extends PythonResource("python_tests")
