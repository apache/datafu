---
title: Apache DataFu-Spark 2.0.0 Released
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

I'd like to announce the release of Apache DataFu-Spark 2.0.0.

This version is the first to support Spark 3.x. In this release, Spark versions 3.0.0 to 3.1.3 are supported.

The four classes in SparkUDAFs - MultiSet, MultiArraySet, MapMerge and CountDistinctUpTo are deprecated. Instead of them, there are new versions which use the Spark Aggregator API. The deprecated versions will be removed in DataFu 2.1.0.

<br>

**Improvements**

* Spark 3.0.0 - 3.1.3 supported (DATAFU-169)
* New Aggregators replace deprecated UserDefinedAggregateFunction (DATAFU-173)
  
**Breaking changes**

* Spark 2.x no longer supported

<br>

The source release can be obtained from:

http://www.apache.org/dyn/closer.cgi/datafu/apache-datafu-2.0.0/apache-datafu-sources-2.0.0.tgz

Artifacts for DataFu are published in Apache's Maven Repository:

https://repository.apache.org/content/groups/public/org/apache/datafu/

Please visit the [Download](/docs/download.html) page for instructions on building from source or retrieving the artifacts in your build system.
