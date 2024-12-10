---
title: "Introducing DataFu-Spark"
author: Eyal Allweil
license: >
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
---

![](https://miro.medium.com/max/1400/0*koSzBO7KqbmIpiPl)

Photo by [National Cancer Institute](https://unsplash.com/@nci?utm_source=medium&utm_medium=referral) on [Unsplash](https://unsplash.com?utm_source=medium&utm_medium=referral)

**_As with many Apache projects with robust communities and growing ecosystems,_** [**_Apache DataFu_**](http://datafu.apache.org/) **_has contributions from individual code committers employed by various organizations. Users of Apache projects who contribute code back to the project benefits everyone. This is part two of PayPal's story (_**[**_part one is here_**](https://datafu.apache.org/blog/2019/01/29/a-look-at-paypals-contributions-to-datafu.html)**_)._**

For more than five years, _datafu-pig_ has been an important collection of generic Apache Pig UDFs, a collection to which PayPal has often contributed. In this post we will discuss _datafu-spark_, a new library containing utilities and UDFs for working with Apache Spark (currently versions 2.x). This library is based on an internal PayPal project, was open sourced in 2019, and has been used by production workflows since 2017. All of the code is unit tested to ensure quality. The following sections will describe how you can use this library.

---
<br>

**1\. Finding the most recent update of a given record — the _dedup_ (de-duplication) methods**

A common scenario in data sent to the HDFS — the Hadoop Distributed File System — is multiple rows representing updates for the same logical data. For example, in a table representing accounts, a record might be written every time customer data is updated, with each update receiving a newer timestamp. Let’s consider the following simplified example.

Raw customers’ data, with more than one row per customer

We can see that though most of the customers only appear once, _julia_ and _quentin_ have 2 and 3 rows, respectively. How can we get just the most recent record for each customer? In datafu-pig, we would have used the _dedup_ (de-duplicate) macro. In Spark, we can use the [_dedupWithOrder_](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L150) method.

```
import datafu.spark.DataFrameOps._  
  
val customers = spark.read.format("csv").load("customers.csv")  
  
csv.dedupWithOrder($"id", $"date_updated".desc).orderBy("id").show
```

Our result will be as expected — each customer only appears once, as you can see below:

```
+---+-------+---------+------------+  
| id|   name|purchases|date_updated|  
+---+-------+---------+------------+  
|  1|quentin|       50|  2018-03-02|  
|  2|  julia|       19|  2017-09-03|  
|  3|  eliot|       12|  2016-05-03|  
|  4|  alice|       11|  2017-02-18|  
|  5|   josh|       20|  2017-03-19|  
|  6|  janet|       19|  2016-06-23|  
|  7|  penny|        4|  2017-04-29|  
+---+-------+---------+------------+
```

There are two additional variants of _dedupWithOrder_ in datafu-spark. The [_dedupWithCombiner_](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L190) method has similar functionality to _dedupWithOrder_, but uses a UDAF to utilize map side aggregation (use this version when you expect many duplicate rows to be removed)_._ The  [_dedupTopN_](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L164) method allows retaining more than one record for each key.

---
<br>

**2\. Doing skewed joins — the** [**_joinSkewed_**](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L290) **and** [**_broadcastJoinSkewed_**](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L323) **methods**

Skewed joins are joins in which the data is not evenly distributed across the join key(s), which inhibits Spark’s ability to run in parallel efficiently. This is particularly problematic at PayPal; some accounts have only a few transactions, while others can have millions! Without specialized code that takes this into account, a naive Spark join will fail for such data. DataFu-Spark contains two methods for doing such skewed joins.

The _joinSkewed_ method should be used when the left dataframe has data skew and the right data frame is relatively small but still too big to fit in memory (for a normal Spark map side broadcast join).

Let’s take a join between the “deduped” customers table above and a table of transactions (in real life, PayPal's transactions tables are extremely large, and the customers’ tables are smaller, but still too large to fit in memory). Our sample transactions table is below:

A simple join between these two tables might be written like this:

```
txns.join(customers, customers("name") === txns("cust_name").show
```

It produces the following output:

```
+---------+--------------+---+-------+---------+------------+  
|cust_name|transaction_id| id|   name|purchases|date_updated|  
+---------+--------------+---+-------+---------+------------+  
|  quentin|             4|  1|quentin|       50|  2018-03-02|  
|  quentin|             3|  1|quentin|       50|  2018-03-02|  
|  quentin|             2|  1|quentin|       50|  2018-03-02|  
|  quentin|             1|  1|quentin|       50|  2018-03-02|  
|    julia|             5|  2|  julia|       19|  2017-09-03|  
|    julia|             5|  2|  julia|       19|  2017-09-03|  
|    alice|             8|  4|  alice|       11|  2017-02-18|  
|    alice|             7|  4|  alice|       11|  2017-02-18|  
+---------+--------------+---+-------+---------+------------+
```

The _joinSkewed_ method “salts” the right dataframe and then joins them. The syntax is similar to that of a regular Spark join.

```
txns.joinSkewed(custsDeduped, txns("cust_name") === custsDeduped("name"), 3, "inner").show
```

This will produce the same output as our original join, but the second table is “salted” so that there are 3 copies of each row. This will result in the data being partitioned more evenly. Instead of one partition taking much longer to finish (if a “normal” Spark join had been used), all the partitions should finish in about the same amount of time.

_datafu-spark_ contains an additional method for skewed joins, _broadcastJoinSkewed_. This is useful when one dataframe is skewed and the other isn’t. It splits the dataframes into two, and does a map-join to the keys that are skewed above a certain user-defined threshold, and a regular join between the remaining parts.

Spark 3.0 [introduced optimizations for skewed joins](https://www.waitingforcode.com/apache-spark-sql/whats-new-apache-spark-3-join-skew-optimization/read), but for projects using the Spark 2.x line, these DataFu methods provide a ready-to-use solution without upgrading.

---
<br>

**3\. Joining a table with a numeric column with a table with a range — the** [**_joinWithRange_**](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/SparkDFUtils.scala#L407) **method**

An interesting type of join that DataFu allows you to do is between points and ranges. For our example, let’s imagine two data frames, one of graded papers and one representing a system for scoring.

The dataframe for grades might look like this:

The scoring system might look like this:

The DataFu _joinWithRange_ method can be used to match each score with the appropriate grade. You can join these two tables with the following code:

```
students.joinWithRange("grade", grades, "min", "max", 10).show
```

That will result in the following output:

```
+-----+-------+-----+---+---+  
|grade|student|grade|min|max|  
+-----+-------+-----+---+---+  
|   95|  sansa|    A| 90|100|  
|   81|  jaime|    B| 80| 90|  
|   88|   arya|    B| 80| 90|  
|   83| renley|    B| 80| 90|  
|   79| cersei|    C| 70| 80|  
|   72|   robb|    C| 70| 80|  
|   64|    ned|    D| 60| 70|  
|   37| tyrion|    F|  0| 60|  
|   64|    ned| pass| 60|100|  
|   79| cersei| pass| 60|100|  
|   72|   robb| pass| 60|100|  
|   81|  jaime| pass| 60|100|  
|   88|   arya| pass| 60|100|  
|   83| renley| pass| 60|100|  
|   95|  sansa| pass| 60|100|  
|   37| tyrion| fail|  0| 60|  
+-----+-------+-----+---+---+
```

In order to join the tables, the range fields are exploded into the numbers that comprise it. However, this may result in a prohibitively large table. The argument 10, passed above, is the decrease factor. This is used to reduce the number of rows generated by the explode in order to make the join efficient.

---
<br>

**4\. Counting distinct records, but only up to a limited amount — the** [**_CountDistinctUpTo_**](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/SparkUDAFs.scala#L228) **UDAF**

Sometimes PayPal's analytical logic required filtering out accounts that don’t have enough data. For example, looking only at customers with a certain small minimum number of transactions. This is not difficult to do in Spark; you can group by the customer’s id, count the number of distinct transactions, and filter out the customers that don’t have enough.

We’ll use the transactions table from above as an example. You can use the following “pure” Spark code to get the number of distinct transactions per name:

```
val csv = spark.read.format("csv").load("txns.csv")csv.groupBy("cust_name").agg(countDistinct($"transaction_id").as("distinct_count")).show
```

This will produce the following output:

```
+---------+--------------+  
|cust_name|distinct_count|  
+---------+--------------+  
|    alice|             2|  
|    julia|             1|  
|  quentin|             4|  
+---------+--------------+
```

Note that Julia has a count of 1, because although she has 2 rows, they have the same transaction id.

However, as we have written above, accounts in PayPal can differ wildly in their scope. A transactions table might have only a few purchases for an individual, but millions for a large company. This is an example of data skew, and the procedure described above would not work efficiently in such cases.

In order to get the same count with much better performance, you can use the Spark version of Datafu’s _CountDistinctUpTo_ UDF. Let’s look at the following code, which counts distinct transactions up to 3 and 5:

```
import datafu.spark.SparkUDAFs.CountDistinctUpTo
val countDistinctUpTo3 = new CountDistinctUpTo(3)  
val countDistinctUpTo5 = new CountDistinctUpTo(5)

val csv = spark.read.format("csv").load("txns.csv")

csv.groupBy("cust_name").agg(countDistinctUpTo3($"transaction_id").as("cnt3"),countDistinctUpTo5($"transaction_id").as("cnt5")).show
```

This results in the following output:

```
+---------+----+----+  
|cust_name|cnt3|cnt5|  
+---------+----+----+  
|    alice|   2|   2|  
|    julia|   1|   1|  
|  quentin|   3|   4|  
+---------+----+----+
```

Notice that when we ask _CountDistinctUpTo_ to stop at 3, _quentin_ gets a count of 3, even though he has 4 transactions. When we use 5 as a parameter to _CountDistinctUpTo_, he gets the actual count of 4.

In our example, there’s no real reason to use the _CountDistinctUpTo_ UDF. But in our “real” use case, stopping the count at a small number instead of counting millions saves resources and time. The improvement is because the UDF doesn’t need to keep all the records in memory in order to return the desired result.

---
<br>

**5\. Calling Python code from Scala, and Scala code from Python — the ScalaPythonBridge**

datafu-spark contains a “Scala-Python Bridge” which allows [calling the dataFu Scala API’s](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/resources/pyspark_utils/df_utils.py), arbitrary Scala code from Python, and [Python code from Scala](https://github.com/apache/datafu/blob/1.6.0/datafu-spark/src/main/scala/datafu/spark/ScalaPythonBridge.scala#L41).

For example, in order to call the spark-datafu API’s from Pyspark, you can do the following (tested on a Hortonworks vm)

First, call pyspark with the following parameters:

```
export PYTHONPATH=datafu-spark_2.11-1.6.0.jarpyspark  --jars datafu-spark_2.11-1.6.0.jar --conf spark.executorEnv.PYTHONPATH=datafu-spark_2.11-1.6.0.jar
```

The following is an example of calling the Spark version of the datafu _dedupWithOrder_ method (taken from the _datafu-spark_ [tests](https://github.com/apache/datafu/blob/master/datafu-spark/src/test/resources/python_tests/df_utils_tests.py))

```
from pyspark_utils import df_utils

df_people = spark.createDataFrame([  
...     ("a", "Alice", 34),  
...     ("a", "Sara", 33),  
...     ("b", "Bob", 36),  
...     ("b", "Charlie", 30),  
...     ("c", "David", 29),  
...     ("c", "Esther", 32),  
...     ("c", "Fanny", 36),  
...     ("c", "Zoey", 36)],  
...     ["id", "name", "age"])
...
...     df_dedup = df_utils.dedup_with_order(df=df_people, group_col=df_people.id, order_cols=[df_people.age.desc(), df_people.name.desc()])
...     df_dedup.show()
```

This should produce the following output:

```
+---+-----+---+  
| id| name|age|  
+---+-----+---+  
|  c| Zoey| 36|  
|  b|  Bob| 36|  
|  a|Alice| 34|  
+---+-----+---+
```

---
<br>

I hope that I’ve managed to explain how to use our new Spark contributions to DataFu. At PayPal, these methods were extremely useful, and we hope that developers outside of PayPal will enjoy them as well now that they are part of the Apache ecosystem. You can find all of the files used in this post by clicking the GitHub gists.

---
<br>

A version of this post has appeared in the [Technology At PayPal Blog.](https://medium.com/paypal-tech/introducing-datafu-spark-ba67faf1933a)
