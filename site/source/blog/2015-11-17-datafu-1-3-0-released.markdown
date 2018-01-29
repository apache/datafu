---
title: Apache DataFu (incubating) 1.3.0 Released
author: Matthew Hayes
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

I'd like to announce the release of Apache DataFu (incubating) 1.3.0.  This is the first release since entering the [Apache Incubator](http://incubator.apache.org/).  Thanks to all who contributed!

[Apache DataFu](/) is a collection of libraries for working with large-scale data in Hadoop. The project was inspired by the need for stable, well-tested libraries for data mining and statistics.  It consists of two libraries: [Apache DataFu Pig](/docs/datafu/getting-started.html), a collection of user-defined functions for [Apache Pig](https://pig.apache.org/), and [Apache DataFu Hourglass](/docs/hourglass/getting-started.html), an incremental processing framework for [Apache Hadoop](https://hadoop.apache.org/) in MapReduce.

You can obtain the source release from:

https://archive.apache.org/dist/incubator/datafu/apache-datafu-incubating-1.3.0/

Please follow the [Download](/docs/download.html) page for instructions on building.  A summary of [changes](https://github.com/apache/incubator-datafu/blob/1.3.0/changes.md) for 1.3.0 appears below.

Additions:

* New UDFs for entropy and weighted sampling algorithms (DATAFU-2, DATAFU-26)
* Updated SimpleRandomSample to be consistent with SimpleRandomSampleWithReplacement (DATAFU-5)
* Created OpenNLP UDF wrappers (DATAFU-8)
* Created RandomUUID UDF (DATAFU-18)
* Added LSH implementation (DATAFU-37)
* Added Base64Encode/Decode (DATAFU-52)
* URLInfo UDF (DATAFU-62)
* Created SelectFieldByName UDF (DATAFU-69)
* Added generic BagJoin that supports inner, left, and full outer joins (DATAFU-70)
* Added ZipBags UDF which can zip and arbitrary number of bags into one (DATAFU-79)
* Hadoop 2.0 compatibility (DATAFU-58)
* Created TupleFromBag.java file (DATAFU-92)

Improvements:

* Simplified BagGroup output (DATAFU-42)

Changes:

* StagedOutputJob no longer writes counters by default (DATAFU-35)

Fixes:

* ReservoirSample does not behave as expected when grouping by a key other than ALL (DATAFU-11)
* DistinctBy does not work correctly on strings containing minuses (DATAFU-31)
* Hourglass does not honor "fail on missing" in all cases (DATAFU-35)
* Hash UDFs return zero-padded strings of uniform length even when leading bits are zero (DATAFU 46)
* UDF examples work again (DATAFU-49)
* SampleByKey can throw NullPointerException (DATAFU-68)

Build system:

* Removed legacy checked in jars (DATAFU-55)
* Updated to use Pig 0.12.1 (DATAFU-10)
* Switched from Ant to Gradle 1.12 (DATAFU-27, DATAFU-44, DATAFU-43, DATAFU-66)
* Removed checked in jars, download where necessary (DATAFU-55)
* Fixed test.sh to use gradlew (DATAFU-77)

Release related:

* NOTICE updated with dependencies used or shipped with DataFu.
* Apache license headers added to all necessary files (DATAFU-4, DATAFU-75)
* Added doap file (DATAFU-36)
* Source tarball generation, gradle bootstrapping, and release instructions (DATAFU-57, DATAFU-78, DATAFU-72)
* Removed author tags (DATAFU-74)
* Resolved issues with build-plugin directory (DATAFU-76)
* Used Apache RAT to verify correct file headers (DATAFU-73, DATAFU-84)

Documentation related:

* New website (DATAFU-20, etc.)
* StreamingQuantile PDF link is broken (DATAFU-29)
* README file updated

