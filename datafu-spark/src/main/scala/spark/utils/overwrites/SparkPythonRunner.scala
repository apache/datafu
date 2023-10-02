/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.datafu.deploy

import java.io._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import datafu.spark.ScalaPythonBridge
import org.apache.commons.codec.binary.Base64
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.api.python.PythonUtils
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.util.Utils
import py4j.GatewayServer

import java.security.SecureRandom
import scala.util.Random

/**
 * Internal class - should not be used by user
 *
 * background:
 * We had to "override" Spark's PythonRunner because we failed on premature python process closing.
 * In PythonRunner the python process exits immediately when finished to read the file,
 * this caused us to Accumulators Exceptions when the driver tries to get accumulation data
 * from the python gateway.
 * Instead, like in Zeppelin, we create an "interactive" python process, feed it the python
 * script and not closing the gateway.
 */
case class SparkPythonRunner(pyPaths: String,
                             otherArgs: Array[String] = Array()) {

  val logger: Logger = LogManager.getLogger(getClass)
  val (reader, writer, process) = initPythonEnv()

  def runPyFile(pythonFile: String): String = {

    val formattedPythonFile = PythonRunner.formatPath(pythonFile)
    execFile(formattedPythonFile, writer, reader)

  }

  private def initPythonEnv(): (BufferedReader, BufferedWriter, Process) = {

    val pythonExec =
      sys.env.getOrElse("PYSPARK_DRIVER_PYTHON",
                        sys.env.getOrElse("PYSPARK_PYTHON", "python3"))

    // Format python filename paths before adding them to the PYTHONPATH
    val formattedPyFiles = PythonRunner.formatPaths(pyPaths)

    // Launch a Py4J gateway server for the process to connect to; this will let it see our
    // Java system properties and such
    val auth_token = createSecret(256)
    val gatewayServer = new GatewayServer.GatewayServerBuilder()
      .entryPoint(ScalaPythonBridge)
      .javaPort(0)
      .authToken(auth_token)
      .build()
    val thread = new Thread(new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions {
        gatewayServer.start()
      }
    })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()

    // Wait until the gateway server has started, so that we know which port is it bound to.
    // `gatewayServer.start()` will start a new thread and run the server code there, after
    // initializing the socket, so the thread started above will end as soon as the server is
    // ready to serve connections.
    thread.join()

    // Build up a PYTHONPATH that includes the Spark assembly JAR (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyPaths argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements: _*)
    logger.info(
      s"Running python with PYTHONPATH:\n\t${formattedPyFiles.mkString(",")}")

    // Launch Python process
    val builder = new ProcessBuilder(
      (Seq(pythonExec, "-iu") ++ otherArgs).asJava)
    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    env.put("PYSPARK_GATEWAY_SECRET", auth_token)
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    val process = builder.start()
    val writer = new BufferedWriter(
      new OutputStreamWriter(process.getOutputStream))
    val reader = new BufferedReader(
      new InputStreamReader(process.getInputStream))

    (reader, writer, process)
  }

  private def execFile(filename: String,
                       writer: BufferedWriter,
                       reader: BufferedReader): String = {
    writer.write("import traceback\n")
    writer.write("import sys\n")
    writer.write("try:\n")
    writer.write("    if sys.version_info < (3, 0):\n")
    writer.write(s"      execfile('$filename')\n")
    writer.write("    else:\n")
    writer.write(s"      exec(open('$filename').read())\n")
    writer.write("    print (\"*!?flush reader!?*\")\n")
    writer.write("except Exception as e:\n")
    writer.write("    traceback.print_exc()\n")
    writer.write("    print (\"*!?flush error reader!?*\")\n\n")
    writer.flush()
    var output = ""
    var line: String = reader.readLine
    while (!line.contains("*!?flush reader!?*") && !line.contains(
             "*!?flush error reader!?*")) {
      logger.info(line)
      if (line == "...") {
        output += "Syntax error ! "
      }
      output += "\r" + line + "\n"
      line = reader.readLine
    }

    if (line.contains("*!?flush error reader!?*")) {
      throw new RuntimeException("python bridge error: " + output)
    }

    output
  }

  def createSecret(secretBitLength: Int): String = {
    val rnd = new SecureRandom
    val secretBytes = new Array[Byte](secretBitLength / java.lang.Byte.SIZE)
    rnd.nextBytes(secretBytes)
    Base64.encodeBase64String(secretBytes)
  }

}
