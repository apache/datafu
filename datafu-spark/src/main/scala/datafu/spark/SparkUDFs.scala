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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType

import scala.collection.mutable
import scala.reflect.runtime.universe

object SparkUDFs {

  /**
    * given an elem and array of values, return false if element is empty or if the array contains element
    */
  private def isEmptyWithDefaultFunc(emptyVals: Seq[Any] = Seq.empty, elem: String)= {
    StringUtils.isEmpty(elem) || emptyVals.contains(elem)
  }

  val isEmptyWithDefault: UserDefinedFunction = udf((emptyVals: Seq[Any], elem: String)=>{
    isEmptyWithDefaultFunc(emptyVals, elem)
  })

  val isNoneBlank: UserDefinedFunction = udf((elem: String)=>{
    StringUtils.isNoneBlank(elem)
  })

  val isBlank: UserDefinedFunction = udf((elem: String)=>{
    StringUtils.isBlank(elem)
  })

  /**
    * like null-coalesce but returning the first not empty value:
    * coalesceVal(null, "a") -> "a"
    * coalesceVal("", "a") -> "a"
    */
  @SparkUDF
  def coalesceVal(s1: String, s2: String)  = {
    Array(s1, s2).foldLeft[String](null)((b, s) => Option(b).getOrElse("") match {
      case "" => s
      case _ => b
    })
  }

  /**
    * like null-coalesce but returning the first not empty value:
    * coalesceVal(null, "a") -> "a"
    * coalesceVal("", "a") -> "a"
    */
  def coalesceValUDF: UserDefinedFunction = udf((s1: String, s2: String) => {

    SparkUDFs.coalesceVal(s1,s2)

  })
}

object UDFRegister {
  def register(sqlContext: SQLContext) = {
    new UDFRegister().register(sqlContext)
  }

  def register(clazz: Class[_], sqlContext: SQLContext) = {
    new UDFRegister().registerScalaClass(isScalaObject = false, clazz.getName, sqlContext)
  }

  def registerObject(clazz: String, sqlContext: SQLContext) = {
    new UDFRegister().registerScalaClass(isScalaObject = true, clazz, sqlContext)
  }

  def registerObject(clazz: Class[_], sqlContext: SQLContext) = {
    new UDFRegister().registerScalaClass(isScalaObject = true, clazz.getName, sqlContext)
  }
}

class UDFRegister() extends Serializable {

  /**
    * This method is called from pyspark/zeppelin
    * import pyspark_utils.utils
    * utils.UDFRegister(sqlContext)  # passing the sqlContext provided by Zeppelin
    * # UDFRegister calls this function
    *
    * @param sqlContext
    */
  def register(sqlContext: SQLContext) = {
    /**
      * Add lines here for the objects/classes that contain Scala UDFs
      * For an Object use registerScalaObject(MyObject.getClass, sqlContext)
      * For a class use registerScalaClass(typeOf[MyClass], sqlContext)
      */
    registerScalaClass(isScalaObject = true, SparkUDFs.getClass.getName, sqlContext)
  }

  def registerScalaClass(isScalaObject: Boolean, cname: String, sqlContext: SQLContext): Unit = {
    // Note - we have looked int Scala reflection, Universe, Tree, Toolbox etc.
    //  and we decide to use low level Java-like interface as the Toolbox
    // package requires a dependency on the scala-compiler.jar

    val loader = getClass.getClassLoader
    val z = loader.loadClass(cname)
    val methods = z.getMethods
    val annotatedMethods = methods.withFilter { m =>
      m.getAnnotation(classOf[SparkUDF]) != null
    }

    annotatedMethods.foreach(am => {
      val name = am.getName
      val retClz: Class[_] = am.getReturnType
      val np: Int = am.getParameterTypes.length
      try {
        val func: AnyRef = np match {
          case 0 => () => invokeByName(isScalaObject, cname, name, Array[Any](name))
          case 1 => a: Any => invokeByName(isScalaObject, cname, name, Array[Any](a))
          case 2 => (a: Any, b: Any) => invokeByName(isScalaObject, cname, name, Array[Any](a, b))
          case 3 => (a: Any, b: Any, c: Any) => invokeByName(isScalaObject, cname, name, Array[Any](a, b, c))
          case 4 => (a: Any, b: Any, c: Any, d: Any) => invokeByName(isScalaObject, cname, name, Array[Any](a, b, c, d))
          case 5 => (a: Any, b: Any, c: Any, d: Any, e: Any) => invokeByName(isScalaObject, cname, name, Array[Any](a, b, c, d, e))
          case 6 => (a: Any, b: Any, c: Any, d: Any, e: Any, f: Any) => invokeByName(isScalaObject, cname, name, Array[Any](a, b, c, d, e, f))
        }
        // convert from scala method to spark UDF
        val builder = (e: Seq[Expression]) => {
          val loader = getClass.getClassLoader
          val mirror = scala.reflect.runtime.universe.runtimeMirror(loader)
          val dataType: DataType = ScalaReflection.schemaFor(getType(retClz, mirror)).dataType
          val inputsNullSafe: Seq[Boolean] = am.getParameterTypes.map(clz => false).toSeq
          val inputTypes: Seq[DataType] = am.getParameterTypes.map(clz => ScalaReflection.schemaFor(getType(clz, mirror)).dataType).toSeq
          ScalaUDF(func, dataType, e, inputsNullSafe, inputTypes)
 //         org.apache.spark.sql.catalyst.expressions.ScalaUDF(function = func, dataType = dataType, children = e, inputTypes = inputTypes)


        }
        registerFunctionBuilder(sqlContext, name, builder)
      } catch {
        case e: Exception => println(e)
      }
    })
  }

  def invokeByNameObject(isScalaObject: Boolean, className: String, methodName: String, params: Array[Object]): Any = {
    // because of serialization problem - we cannot have java.lang.reflect.Method
    // sent as part of the clojure.
    // So we send the name of the class and method and use the class loader on each executor to find them.
    val clazz = Thread.currentThread().getContextClassLoader.loadClass(className)
    val instance = if (isScalaObject) {
      // if we register from Scala we have this MODULE$ field. but if we register from python we don't.
      val fields = clazz.getFields.filter(f => f.getName == "MODULE$")
      if (fields.length == 0)
        null
      else fields.head.get(null)
    } else clazz.newInstance()

    val methods: Array[java.lang.reflect.Method] = clazz.getDeclaredMethods.filter(m => m.getName == methodName)
    if (methods.length == 1) {
      methods(0).invoke(instance, params: _*)
    } else {
      throw new RuntimeException("SPARK UDF - must have only a single function defined with name " + methodName)
    }
  }

  def invokeByName(isScalaObject: Boolean, className: String, methodName: String, params: Array[Any]): Any = {
    invokeByNameObject(isScalaObject, className, methodName, params.map(_.asInstanceOf[Object]))
  }

  def getType(clazz: Class[_], runtimeMirror: scala.reflect.runtime.universe.Mirror): universe.Type =
    runtimeMirror.classSymbol(clazz).toType

  def registerFunctionBuilder(sqlContext : SQLContext, name : String, builder: FunctionBuilder): Unit = {
    sqlContext.sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(name, builder)
  }
}
