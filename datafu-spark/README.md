# datafu-spark

datafu-spark contains a number of spark API's and a "Scala-Python bridge" that makes calling Scala code from Python, and vice-versa, easier.

Here are some examples of things you can do with it:

* ["Dedup" a table](https://github.com/apache/datafu/blob/spark-tmp/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L139) - remove duplicates based on a key and ordering (typically a date updated field, to get only the mostly recently updated record).

* [Join a table with a numeric field with a table with a range](https://github.com/apache/datafu/blob/spark-tmp/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L361)

* [Do a skewed join between tables](https://github.com/apache/datafu/blob/spark-tmp/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L274) (where the small table is still too big to fit in memory)

* [Count distinct up to](https://github.com/apache/datafu/blob/spark-tmp/datafu-spark/src/main/scala/datafu/spark/SparkUDAFs.scala#L224) - an efficient implementation when you just want to verify that a certain minimum of distinct rows appear in a table

It has been tested on Spark releases from 2.2.0 to 2.4.3, using Scala 2.11 and 2.12. You can check if your Spark/Scala version combination has been tested by looking [here.](https://github.com/apache/datafu/blob/master/datafu-spark/build_and_test_spark.sh#L20)

-----------

In order to call the datafu-spark API's from Pyspark, you can do the following (tested on a Hortonworks vm)

First, call pyspark with the following parameters

```bash
export PYTHONPATH=datafu-spark_2.11_2.3.0-1.7.0.jar

pyspark --jars datafu-spark_2.11_2.3.0-1.7.0.jar --conf spark.executorEnv.PYTHONPATH=datafu-spark_2.11_2.3.0-1.7.0.jar
```

The following is an example of calling the Spark version of the datafu _dedup_ method

```python
from pyspark_utils.df_utils import PySparkDFUtils

df_utils = PySparkDFUtils()

df_people = sqlContext.createDataFrame([
     ("a", "Alice", 34),
     ("a", "Sara", 33),
     ("b", "Bob", 36),
     ("b", "Charlie", 30),
     ("c", "David", 29),
     ("c", "Esther", 32),
     ("c", "Fanny", 36),
     ("c", "Zoey", 36)],
     ["id", "name", "age"])

func_dedup_res = df_utils.dedup_with_order(dataFrame=df_people, groupCol=df_people.id,
                                orderCols=[df_people.age.desc(), df_people.name.desc()])

func_dedup_res.registerTempTable("dedup")

func_dedup_res.show()
```

This should produce the following output

<pre>
+---+-----+---+
| id| name|age|
+---+-----+---+
|  c| Zoey| 36|
|  b|  Bob| 36|
|  a|Alice| 34|
+---+-----+---+
</pre>

-----------

# Development

Building and testing datafu-spark can be done as described in the [the main DataFu README.](https://github.com/apache/datafu/blob/master/README.md#developers)

If you wish to build for a specific Scala/Spark version, there are two options. One is to change the *scalaVersion* and *sparkVersion* in [the main gradle.properties file.](https://github.com/apache/datafu/blob/spark-tmp/gradle.properties#L22)

The other is to pass these parameters in the command line. For example, to build and test for Scala 2.12 and Spark 2.4.3, you would use

```bash
./gradlew :datafu-spark:test -PscalaVersion=2.12 -PsparkVersion=2.4.3
```

There is a [script](https://github.com/apache/datafu/tree/spark-tmp/datafu-spark/build_and_test_spark.sh) for building and testing datafu-spark across the multiple Scala/Spark combinations.

To see the available options run it like this:

```bash
./build_and_test_spark.sh -h
```


