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

class PysparkResource extends PythonResource(PathsResolver.pyspark, true)

class Py4JResource extends PythonResource(PathsResolver.py4j, true)

object PathsResolver {
  
  val sparkSystemVersion = System.getProperty("datafu.spark.version")
  
  val py4js = Map(
      "3.0.0" -> "0.10.9.7",
      "3.0.1" -> "0.10.9.7",
      "3.1.1" -> "0.10.9.7",
      "3.1.3" -> "0.10.9.7"
  )

  val sparkVersion = if (sparkSystemVersion == null) "3.0.0" else sparkSystemVersion
  
  val py4jVersion = py4js.getOrElse(sparkVersion, "0.10.9.7") // our default
  
  val pyspark = ResourceCloning.cloneResource(new File("data/pysparks/pyspark-" + sparkVersion + ".zip").toURI().toURL(),
    "pyspark_cloned.zip").getPath
  val py4j = ResourceCloning.cloneResource(new File("data/py4js/py4j-" + py4jVersion + "-src.zip").toURI().toURL(),
    "py4j_cloned.zip").getPath
}
