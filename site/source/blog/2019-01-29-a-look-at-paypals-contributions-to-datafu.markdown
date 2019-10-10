---
title: "A Look into PayPal’s Contributions to Apache DataFu"
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

![](https://cdn-images-1.medium.com/max/1600/1*RZRPFvbZ7_IdJxY-6TeaxQ.jpeg)

Photo by [Louis Reed](https://unsplash.com/photos/pwcKF7L4-no?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText) on [Unsplash](https://unsplash.com/search/photos/test-tubes?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText)

**_As with many Apache projects with robust communities and growing ecosystems,_** [**_Apache DataFu_**](http://datafu.apache.org/) **_has contributions from individual code committers employed by various organizations. Users of Apache projects who contribute code back to the project benefits everyone. This is PayPal's story._**

At PayPal, we often work on large datasets in a Hadoop environment — crunching up to petabytes of data and using a variety of sophisticated tools in order to fight fraud. One of the tools we use to do so is [Apache Pig](https://pig.apache.org/). Pig is a simple, high-level programming language that consists of just a few dozen operators, but it allows you to write powerful queries and transformations over Hadoop.

It also allows you to extend Pig’s capabilities by writing macros and UDF’s (user defined functions). At PayPal, we’ve written a variety of both, and contributed many of them to the [Apache DataFu](http://datafu.apache.org/) project. In this blog post we’d like to explain what we’ve contributed and present a guide to how we use them.

---
<br>

**1\. Finding the most recent update of a given record — the _dedup_ (de-duplication) macro**

A common scenario in data sent to the HDFS — the Hadoop Distributed File System — is multiple rows representing updates for the same logical data. For example, in a table representing accounts, a record might be written every time customer data is updated, with each update receiving a newer timestamp. Let’s consider the following simplified example.

<br>
<script src="https://gist.github.com/eyala/65b6750b2539db5895738a49be3d8c98.js"></script>
<center>Raw customers’ data, with more than one row per customer</center>
<br>

We can see that though most of the customers only appear once, _julia_ and _quentin_ have 2 and 3 rows, respectively. How can we get just the most recent record for each customer? For this we can use the _dedup_ macro, as below:

```pig
REGISTER datafu-pig-1.5.0.jar;

IMPORT 'datafu/dedup.pig';

data = LOAD 'customers.csv' AS (id: int, name: chararray, purchases: int, date_updated: chararray);

dedup_data = dedup(data, 'id', 'date_updated');

STORE dedup_data INTO 'dedup_out';
```

Our result will be as expected — each customer only appears once, as you can see below:

<br>
<script src="https://gist.github.com/eyala/1dddebc39e9a3fe4501638a95f577752.js"></script>
<center>“Deduplicated” data, with only the most recent record for each customer</center>
<br>

One nice thing about this macro is that you can use more than one field to dedup the data. For example, if we wanted to use both the _id_ and _name_ fields, we would change this line:

```pig
dedup_data = dedup(data, 'id', 'date_updated');
```

to this:

```pig
dedup_data = dedup(data, '(id, name)', 'date_updated');
```

---
<br>

**2\. Preparing a sample of records based on a list of keys — the sample\_by\_keys macro.**

Another common use case we’ve encountered is the need to prepare a sample based on a small subset of records. DataFu already includes a number of UDF’s for sampling purposes, but they are all based on random selection. Sometimes, at PayPal, we needed to be able to create a table representing a manually-chosen sample of customers, but with exactly the same fields as the original table. For that we use the _sample\_by\_keys_ macro. For example, let’s say we want customers 2, 4 and 6 from _customers.csv_. If we have this list stored on the HDFS as _sample.csv_, we could use the following Pig script:

```pig
REGISTER datafu-pig-1.5.0.jar;

IMPORT 'datafu/sample_by_keys.pig';

data = LOAD 'customers.csv' USING PigStorage(',') AS (id: int, name: chararray, purchases: int, updated: chararray);

customers = LOAD 'sample.csv' AS (cust_id: int);

sampled = sample_by_keys(data, customers, id, cust_id);

STORE sampled INTO 'sample_out';
```

The result will be all the records from our original table for customers 2, 4 and 6. Notice that the original row structure is preserved, and that customer 2 —_ julia_ — has two rows, as was the case in our original data. This is important for making sure that the code that will run on this sample will behave exactly as it would on the original data.

<br>
<script src="https://gist.github.com/eyala/28985cc0e3f338d044cc5ebb779f6454.js"></script>
<center>Only customers 2, 4, and 6 appear in our new sample</center>
<br>

---
<br>

**3\. Comparing expected and actual results for regression tests — the _diff\_macro_**

After making changes in an application’s logic, we are often interested in the effect they have on our output. One common use case is when we refactor — we don’t expect our output to change. Another is a surgical change which should only affect a very small subset of records. For easily performing such regression tests on actual data, we use the _diff\_macro_, which is based on DataFu’s _TupleDiff_ UDF.

Let’s look at a table which is exactly like _dedup\_out_, but with four changes.

1.  We will remove record 1, _quentin_
2.  We will change _date\_updated_ for record 2, _julia_
3.  We will change _purchases_ and _date\_updated_ for record 4, _alice_
4.  We will add a new row, record 8, _amanda_

<br>
<script src="https://gist.github.com/eyala/699942d65471f3c305b0dcda09944a95.js"></script>
<br>

We’ll run the following Pig script, using DataFu’s _diff\_macro_:

```pig
REGISTER datafu-pig-1.5.0.jar;

IMPORT 'datafu/diff_macros.pig';

data = LOAD 'dedup_out.csv' USING PigStorage(',') AS (id: int, name: chararray, purchases: int, date_updated: chararray);

changed = LOAD 'dedup_out_changed.csv' USING PigStorage(',') AS (id: int, name: chararray, purchases: int, date_updated: chararray);

diffs = diff_macro(data,changed,id,'');

DUMP diffs;
```

The results look like this:

<br>
<script src="https://gist.github.com/eyala/3d36775faf081daad37a102f25add2a4.js"></script>
<br>

Let’s take a moment to look at these results. They have the same general structure. Rows that start with _missing_ indicate records that were in the first relation, but aren’t in the new one. Conversely, rows that start with _added_ indicate records that are in the new relation, but not in the old one. Each of these rows is followed by the relevant tuple from the relations.

The rows that start with _changed_ are more interesting. The word _changed_ is followed by a list of the fields which have changed values in the new table. For the row with _id_ 2, this is the _date\_updated_ field. For the row with _id_ 4, this is the _purchases_ and _date\_updated_ fields.

Obviously, one thing we might want to ignore is the _date\_updated_ field. If the only difference in the fields is when it was last updated, we might just want to skip these records for a more concise diff. For this, we need to change the following row in our original Pig script, from this:

```pig
diffs = diff_macro(data,changed,id,'');
```

to become this:

```pig
diffs = diff_macro(data,changed,id,'date_updated');
```

If we run our changed Pig script, we’ll get the following result.

<br>
<script src="https://gist.github.com/eyala/d9b0d5c60ad4d8bbccc79c3527f99aca.js"></script>
<br>

The row for _julia_ is missing from our diff, because only _date\_updated_ has changed, but the row for _alice_ still appears, because the _purchases_ field has also changed.

There’s one implementation detail that’s important to know — the macro uses a replicated join in order to be able to run quickly on very large tables, so the sample table needs to be able to fit in memory.

---
<br>

**4\. Counting distinct records, but only up to a limited amount — the _CountDistinctUpTo_ UDF**

Sometimes our analytical logic requires us to filter out accounts that don’t have enough data. For example, we might want to look only at customers with a certain small minimum number of transactions. This is not difficult to do in Pig; you can group by the customer’s id, count the number of distinct transactions, and filter out the customers that don’t have enough.

Let’s use following table as an example:

<br>
<script src="https://gist.github.com/eyala/73dc69d0b5f513c53c4dac72c71daf7c.js"></script>
<br>

You can use the following “pure” Pig script to get the number of distinct transactions per name:

```pig
data = LOAD 'transactions.csv' USING PigStorage(',') AS (name: chararray, transaction_id:int);

grouped = GROUP data BY name;

counts = FOREACH grouped {
 distincts = DISTINCT data.transaction_id;
 GENERATE group, COUNT(distincts) AS distinct_count;
 };

DUMP counts;
```

This will produce the following output:

<br>
<script src="https://gist.github.com/eyala/a9cd0ffb99039758f63b9d08c40b1124.js"></script>
<br>

Note that Julia has a count of 1, because although she has 2 rows, they have the same transaction id.

However, accounts in PayPal can differ wildly in their scope. For example, a transactions table might have only a few purchases for an individual, but millions for a large company. This is an example of data skew, and the procedure I described above would not work effectively in such cases. This has to do with how Pig translates the nested foreach statement — it will keep all the distinct records in memory while counting.

In order to get the same count with much better performance, you can use the _CountDistinctUpTo_ UDF. Let’s look at the following Pig script, which counts distinct transactions up to 3 and 5:

```pig
REGISTER datafu-pig-1.5.0.jar;

DEFINE CountDistinctUpTo3 datafu.pig.bags.CountDistinctUpTo('3');
DEFINE CountDistinctUpTo5 datafu.pig.bags.CountDistinctUpTo('5');

data = LOAD 'transactions.csv' USING PigStorage(',') AS (name: chararray, transaction_id:int);

grouped = GROUP data BY name;

counts = FOREACH grouped GENERATE group,CountDistinctUpTo3($1) as cnt3, CountDistinctUpTo5($1) AS cnt5;

DUMP counts;
```

This results in the following output:

<br>
<script src="https://gist.github.com/eyala/19e22fb251fe2222b3ccea6f78e37a85.js"></script>
<br>

Notice that when we ask _CountDistinctUpTo_ to stop at 3, _quentin_ gets a count of 3, even though he has 4 transactions. When we use 5 as a parameter to _CountDistinctUpTo_, he gets the actual count of 4.

In our example, there’s no real reason to use the _CountDistinctUpTo_ UDF. But in our “real” use case, stopping the count at a small number instead of counting millions saves resources and time. The improvement is because the UDF doesn’t need to keep all the records in memory in order to return the desired result.

---
<br>

I hope that I’ve managed to explain how to use our new contributions to DataFu. You can find all of the files used in this post by clicking the GitHub gists.

---

A version of this post has appeared in the [PayPal Engineering Blog.](https://medium.com/paypal-engineering/a-guide-to-paypals-contributions-to-apache-datafu-b30cc25e0312)
