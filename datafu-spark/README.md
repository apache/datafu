# datafu-spark

datafu-spark contains a number of spark API's and a "Scala-Python bridge" that makes calling Scala code from Python, and vice-versa, easier.

## Compatibility Matrix

This matrix represents versions of Spark that DataFu has been compiled and tested on. Some/many methods work on unsupported versions as well.

| DataFu | Spark|
|-------|------|
| 1.7.0 | 2.2.0 to 2.2.2, 2.3.0 to 2.3.2 and 2.4.0 to 2.4.3|
| 1.8.0 | 2.2.3, 2.3.3, and 2.4.4 to 2.4.5|
| 2.0.0 | 3.0.x - 3.1.x |
| 2.1.0 (not released yet) | 3.0.x - 3.2.x |

# Examples

Here are some examples of things you can do with it:

* ["Dedup" a table](https://github.com/apache/datafu/blob/main/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L139) - remove duplicates based on a key and ordering (typically a date updated field, to get only the mostly recently updated record).

* [Join a table with a numeric field with a table with a range](https://github.com/apache/datafu/blob/main/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L361)

* [Do a skewed join between tables](https://github.com/apache/datafu/blob/main/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L274) (where the small table is still too big to fit in memory)

* [Count distinct up to](https://github.com/apache/datafu/blob/main/datafu-spark/src/main/scala/datafu/spark/Aggregators.scala#L187) - an efficient implementation when you just want to verify that a certain minimum of distinct rows appear in a table

It has been tested on Spark releases from 3.0.0 to 3.1.3 using Scala 2.12. You can check if your Spark/Scala version combination has been tested by looking [here.](https://github.com/apache/datafu/blob/main/datafu-spark/build_and_test_spark.sh#L20)

-----------

In order to call the datafu-spark API's from Pyspark, you can do the following (tested on a Hortonworks vm)

First, call pyspark with the following parameters

```bash
export PYTHONPATH=datafu-spark_2.12-2.0.0.jar

pyspark --jars datafu-spark_2.12-2.0.0.jar --conf spark.executorEnv.PYTHONPATH=datafu-spark_2.12-2.0.0.jar
```

The following is an example of calling the Spark version of the datafu _dedup_ method

```python
from pyspark_utils import df_utils

df_people = spark.createDataFrame([
     ("a", "Alice", 34),
     ("a", "Sara", 33),
     ("b", "Bob", 36),
     ("b", "Charlie", 30),
     ("c", "David", 29),
     ("c", "Esther", 32),
     ("c", "Fanny", 36),
     ("c", "Zoey", 36)],
     ["id", "name", "age"])

df_dedup = df_utils.dedup_with_order(df=df_people, group_col=df_people.id,
                                     order_cols=[df_people.age.desc(), df_people.name.desc()])
df_dedup.show()
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

Building and testing datafu-spark can be done as described in the [the main DataFu README.](https://github.com/apache/datafu/blob/main/README.md#developers)

If you wish to build for a specific Scala/Spark version, there are two options. One is to change the *scalaVersion* and *sparkVersion* in [the main gradle.properties file.](https://github.com/apache/datafu/blob/main/gradle.properties#L22)

The other is to pass these parameters in the command line. For example, to build and test for Scala 2.12 and Spark 3.1.3, you would use

```bash
./gradlew :datafu-spark:test -PscalaVersion=2.12 -PsparkVersion=3.1.3
```

There is a [script](https://github.com/apache/datafu/tree/main/datafu-spark/build_and_test_spark.sh) for building and testing datafu-spark across the multiple Scala/Spark combinations.

To see the available options run it like this:

```bash
./build_and_test_spark.sh -h
```


