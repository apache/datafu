---
title: Apache DataFu-Spark 1.8.0 Released
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

I'd like to announce the release of Apache DataFu-Spark 1.8.0.

Many thanks to Arpit Bhardwaj and Shaked Aharon, who worked on this version. 

<br>

**Improvements**

* dedupWithCombiner method now supports a list of columns in the order / group by params  (DATAFU-171)
* Scala Python bridge now uses secure gateway (DATAFU-167)
 
**Breaking changes**

* Spark 2.2.0, 2.2.1, and 2.3.0. no longer supported

<br>

The source release can be obtained from:

http://www.apache.org/dyn/closer.cgi/datafu/apache-datafu-1.8.0/apache-datafu-sources-1.8.0.tgz

Artifacts for DataFu are published in Apache's Maven Repository:

https://repository.apache.org/content/groups/public/org/apache/datafu/

Please visit the [Download](/docs/download.html) page for instructions on building from source or retrieving the artifacts in your build system.
