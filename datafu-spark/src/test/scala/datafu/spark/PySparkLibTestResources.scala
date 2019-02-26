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

class PysparkResource extends PythonResource(PathsResolver.pyspark, true)

class Py4JResource extends PythonResource(PathsResolver.py4j, true)

object PathsResolver {
  val pyspark = ResourceCloning.cloneResource(getClass.getResource("/built_in_pyspark_lib/pyspark.zip"), "pyspark_cloned.zip").getPath
  val py4j = ResourceCloning.cloneResource(getClass.getResource("/built_in_pyspark_lib/py4j-0.10.6-src.zip"), "py4j_cloned.zip").getPath
}