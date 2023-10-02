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

import org.apache.logging.log4j.LogManager

import java.io._
import java.net.URL
import java.nio.file.Files
import java.util.UUID
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.datafu.deploy.SparkPythonRunner
import org.apache.spark.sql.SparkSession


/**
 * this class let's the user invoke PySpark code from scala
 * example usage:
 *
 * val runner = ScalaPythonBridgeRunner()
 * runner.runPythonFile("my_package/my_pyspark_logic.py")
 *
 */
case class ScalaPythonBridgeRunner(extraPath: String = "") {

  val logger = LogManager.getLogger(this.getClass)
  // for the bridge we take the full resolved location,
  // since this runs on the driver where the files are local:
  logger.info("constructing PYTHONPATH")

  // we include multiple options for py4j because on any given cluster only one should be found
  val pythonPath = (PythonPathsManager.getAbsolutePaths() ++
    Array("pyspark.zip",
          "py4j-0.10.9-src.zip",
          "py4j-0.10.9.7-src.zip") ++
    Option(extraPath).getOrElse("").split(",")).distinct

  logger.info("Bridge PYTHONPATH: " + pythonPath.mkString(":"))

  val runner = SparkPythonRunner(pythonPath.mkString(","))

  def runPythonFile(filename: String): String = {
    val pyScript = resolveRunnableScript(filename)
    logger.info(s"Running python file $pyScript")
    runner.runPyFile(pyScript)
  }

  def runPythonString(str: String): String = {
    val tmpFile = writeToTempFile(str, "pyspark-tmp-file-", ".py")
    logger.info(
      "Running tmp PySpark file: " + tmpFile.getAbsolutePath + " with content:\n" + str)
    runner.runPyFile(tmpFile.getAbsolutePath)
  }

  private def resolveRunnableScript(path: String): String = {
    logger.info("Resolving python script location for: " + path)

    val res
      : String = Option(this.getClass.getClassLoader.getResource(path)) match {
      case None =>
        logger.info("Didn't find script via classLoader, using as is: " + path)
        path
      case Some(resource) =>
        resource.toURI.getScheme match {
          case "jar" =>
            // if inside jar, extract it and return cloned file:
            logger.info("Script found inside jar, extracting...")
            val outputFile = ResourceCloning.cloneResource(resource, path)
            logger.info("Extracted file path: " + outputFile.getPath)
            outputFile.getPath
          case _ =>
            logger.info("Using script original path: " + resource.getPath)
            resource.getPath
        }
    }
    res
  }

  private def writeToTempFile(contents: String,
                              prefix: String,
                              suffix: String): File = {
    val tempFi = File.createTempFile(prefix, suffix)
    tempFi.deleteOnExit()
    val bw = new BufferedWriter(new FileWriter(tempFi))
    bw.write(contents)
    bw.close()
    tempFi
  }

}

/**
 * Do not instantiate this class! Use the companion object instead.
 * This class should only be used by python
 */
object ScalaPythonBridge { // need empty ctor for py4j gateway

  /**
    * members used to allow python script share context with main Scala program calling it.
    * Python script calls :
    * sc, sqlContext, spark = utils.get_contexts()
    * our Python util function get_contexts
    * uses the following to create Python wrappers around Java SparkContext and SQLContext.
    */
  // Called by python util get_contexts()
  def pyGetSparkSession(): SparkSession = SparkSession.builder().getOrCreate()
  def pyGetJSparkContext(sparkSession: SparkSession): JavaSparkContext =
    new JavaSparkContext(sparkSession.sparkContext)
  def pyGetSparkConf(jsc: JavaSparkContext): SparkConf = jsc.getConf

}

/**
 * Utility for extracting resource from a jar and copy it to a temporary location
 */
object ResourceCloning {

  private val logger = LogManager.getLogger(this.getClass)

  val uuid = UUID.randomUUID().toString.substring(6)
  val outputTempDir = new File(System.getProperty("java.io.tmpdir"),
                               s"risk_tmp/$uuid/cloned_resources/")
  forceMkdirs(outputTempDir)

  def cloneResource(resource: URL, outputFileName: String): File = {
    val outputTmpFile = new File(outputTempDir, outputFileName)
    if (outputTmpFile.exists()) {
      logger.info(s"resource $outputFileName already exists, skipping..")
      outputTmpFile
    } else {
      logger.info("cloning resource: " + resource)
      if (!outputTmpFile.exists()) {
        // it is possible that the file was already extracted in the session
        forceMkdirs(outputTmpFile.getParentFile)
        val inputStream = resource.openStream()
        streamToFile(outputTmpFile, inputStream)
      }
      outputTmpFile
    }
  }

  private def forceMkdirs(dir: File) =
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Failed to create " + dir.getPath)
    }

  private def streamToFile(outputFile: File, inputStream: InputStream) = {
    try {
      Files.copy(inputStream, outputFile.toPath)
    } finally {
      inputStream.close()
      assert(outputFile.exists())
    }
  }
}
