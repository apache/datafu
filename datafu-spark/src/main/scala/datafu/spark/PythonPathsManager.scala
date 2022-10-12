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

import java.io.{File, IOException}
import java.net.JarURLConnection
import java.nio.file.Paths
import java.util
import java.util.{MissingResourceException, ServiceLoader}
import scala.collection.JavaConverters._
import org.apache.logging.log4j.{LogManager, Logger}


/**
 * Represents a resource that needs to be added to PYTHONPATH used by ScalaPythonBridge.
 *
 * To ensure your python resources (modules, files, etc.) are properly added to the bridge,
 * do the following:
 * 1)  Put all the resource under some root directory with a unique name x, and make sure path/to/x
 * is visible to the class loader (usually just use src/main/resources/x).
 * 2)  Extend this class like this:
 * class MyResource extends PythonResource("x")
 * This assumes x is under src/main/resources/x
 * 3)  (since we use ServiceLoader) Add a file to your jar/project:
 * META-INF/services/spark.utils.PythonResource
 * with a single line containing the full name (including package) of MyResource.
 *
 * This process involves scanning the entire jar and copying files from the jar to some temporary
 * location, so if your jar is really big consider putting the resources in a smaller jar.
 *
 * @param resourcePath   Path to the resource, will be loaded via
 *                       getClass.getClassLoader.getResource()
 * @param isAbsolutePath Set to true if the resource is in some absolute path rather than in jar
 *                       (try to avoid that).
 */
abstract class PythonResource(val resourcePath: String,
                              val isAbsolutePath: Boolean = false)

/**
 * There are two phases of resolving python files path:
 *
 * 1) When launching spark:
 *   the files need to be added to spark.executorEnv.PYTHONPATH
 *
 * 2) When executing python file via bridge:
 *   the files need to be added to the process PYTHONPATH.
 *   This is different than the previous phase because
 *   this python process is spawned by datafu-spark, not by spark, and always on the driver.
 */
object PythonPathsManager {

  case class ResolvedResource(resource: PythonResource,
                              resolvedLocation: String)

  private val logger: Logger = LogManager.getLogger(getClass)

  val resources: Seq[ResolvedResource] =
    ServiceLoader
      .load(classOf[PythonResource])
      .asScala
      .map(p => ResolvedResource(p, resolveDependencyLocation(p)))
      .toSeq

  logResolved

  def getAbsolutePaths(): Seq[String] = resources.map(_.resolvedLocation).distinct
  def getAbsolutePathsForJava(): util.List[String] =
    resources.map(_.resolvedLocation).distinct.asJava

  def getPYTHONPATH(): String =
    resources
      .map(_.resolvedLocation)
      .map(p => new File(p))
      .map(_.getName) // get just the name of the file
      .mkString(":")

  private def resolveDependencyLocation(resource: PythonResource): String =
    if (resource.isAbsolutePath) {
      if (!new File(resource.resourcePath).exists()) {
        throw new IOException(
          "Could not find resource in absolute path: " + resource.resourcePath)
      } else {
        logger.info("Using file absolute path: " + resource.resourcePath)
        resource.resourcePath
      }
    } else {
      Option(getClass.getClassLoader.getResource(resource.resourcePath)) match {
        case None =>
          logger.error(
            "Didn't find resource in classpath! resource path: " + resource.resourcePath)
          throw new MissingResourceException(
            "Didn't find resource in classpath!",
            resource.getClass.getName,
            resource.resourcePath)
        case Some(p) =>
          p.toURI.getScheme match {
            case "jar" =>
              // if dependency is inside jar file, use jar file path:
              val jarPath = new File(
                p.openConnection()
                  .asInstanceOf[JarURLConnection]
                  .getJarFileURL
                  .toURI).getPath
              logger.info(
                s"Dependency ${resource.resourcePath} found inside jar: " + jarPath)
              jarPath
            case "file" =>
              val file = new File(p.getFile)
              if (!file.exists()) {
                logger.warn("Dependency not found, skipping: " + file.getPath)
                null
              } else {
                if (file.isDirectory) {
                  val t_path =
                    if (System
                          .getProperty("os.name")
                          .toLowerCase()
                          .contains("win") && p.getPath().startsWith("/")) {
                      val path = p.getPath.substring(1)
                      logger.warn(
                        s"Fixing path for windows operating system! " +
                          s"converted ${p.getPath} to $path")
                      path
                    } else {
                      p.getPath
                    }
                  val path = Paths.get(t_path)
                  logger.info(
                    s"Dependency found as directory: ${t_path}\n\tusing " +
                      s"parent path: ${path.getParent}")
                  path.getParent.toString
                } else {
                  logger.info("Dependency found as a file: " + p.getPath)
                  p.getPath
                }
              }
          }
      }
    }

  private def logResolved = {
    logger.info(s"Discovered ${resources.size} python paths:\n" +
      resources
        .map(p =>
          s"className: ${p.resource.getClass.getName}\n\tresource: " +
            s"${p.resource.resourcePath}\n\tlocation: ${p.resolvedLocation}")
        .mkString("\n")) + "\n\n"
  }
}

/**
 * Contains all python files needed by the bridge itself
 */
class CoreBridgeDirectory extends PythonResource("pyspark_utils")
