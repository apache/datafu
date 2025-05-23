---
title: Apache DataFu-Spark 2.1.0 Released
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

I'd like to announce the release of Apache DataFu-Spark 2.1.0.

In this release, Spark versions 3.0.0 to 3.4.2 are supported.

<br>

**Additions**

* Add dedupByAllExcept method (DATAFU-167). This is a new method for reducing rows when there is one column whose value is not important, but you don't want to lose any actual data from the other rows. For example if a server creates events with an autogenerated event id, and sometimes events are duplicated. You don't want double rows just for the event ids, but if any of the other fields are distinct you want to keep the rows (with their event ids)

* Add collectNumberOrderedElements (DATAFU-176). This is a new UDAF for aggregating and collecting data with a possibility of skew. For example if you want to create a list of top customers for a company. Using a window function would require sending all the data for a given company to the same executor. This method will filter rows out in the combiner stage.

**Improvements**

* Spark 3.0.0 - 3.4.x supported (DATAFU-175, DATAFU-179)
* Expose dedupRandomN in Python (DATAFU-180)
  
**Breaking changes**

* The four deprecated classes in SparkUDAFs - MultiSet, MultiArraySet, MapMerge and CountDistinctUpTo have been removed. Instead of them, there are new versions which use the Spark Aggregator API.

<br>

The source release can be obtained from:

http://www.apache.org/dyn/closer.cgi/datafu/apache-datafu-2.1.0/apache-datafu-sources-2.1.0.tgz

Artifacts for DataFu are published in Apache's Maven Repository:

https://repository.apache.org/content/groups/public/org/apache/datafu/

Please visit the [Download](/docs/download.html) page for instructions on building from source or retrieving the artifacts in your build system.
