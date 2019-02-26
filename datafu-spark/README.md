# datafu-spark

datafu-spark contains a number of spark API's and a "Scala-Python bridge" that makes calling Scala code from Python, and vice-versa, easier.

-----------

In order to call the spark-datafu API's from Pyspark, you can do the following (tested on a Hortonworks vm)

First, call pyspark with the following parameters

```bash
export PYTHONPATH=datafu-spark_2.11_2.3.0-1.5.0-SNAPSHOT.jar

pyspark  --jars datafu-spark_2.11_2.3.0-1.5.0-SNAPSHOT.jar --conf spark.executorEnv.PYTHONPATH=datafu-spark_2.11_2.3.0-1.5.0-SNAPSHOT.jar
```

The following is an example of calling the Spark version of the datafu _dedup_ method

```python
from pyspark_utils.df_utils import PySparkDFUtils

df_utils = PySparkDFUtils()

df_people = sqlContext.createDataFrame([
...     ("a", "Alice", 34),
...     ("a", "Sara", 33),
...     ("b", "Bob", 36),
...     ("b", "Charlie", 30),
...     ("c", "David", 29),
...     ("c", "Esther", 32),
...     ("c", "Fanny", 36),
...     ("c", "Zoey", 36)],
...     ["id", "name", "age"])

func_dedup_res = df_utils.dedup(dataFrame=df_people, groupCol=df_people.id,
...                              orderCols=[df_people.age.desc(), df_people.name.desc()])

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


