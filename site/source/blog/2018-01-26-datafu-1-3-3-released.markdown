---
title: Apache DataFu (incubating) 1.3.3 Released
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

I'd like to announce the release of Apache DataFu (incubating) 1.3.3.

Additions:

* UDF for hash functions such as murmur3 and others. (DATAFU-47)
* UDF for diffing tuples. (DATAFU-119)
* Support for macros in DataFu.  Macros `count_all_non_distinct` and `count_distinct_keys` were added. (DATAFU-123)
* Macro for TFIDF. (DATAFU-61)

Improvements:

* Added lifecylce hooks to ContextualEvalFunc. (DATAFU-50)
* SessionCount and Sessionize now support millisecond precision. (DATAFU-124)
* Upgraded to Guava 20.0. (DATAFU-48)
* Updated Gradle to 3.5.1. (DATAFU-125)
* Rat tasks automatically run during assemble. (DATAFU-118)
* Building now works on Windows. (DATAFU-99)

The source release can be obtained from:

http://www.apache.org/dyn/closer.cgi/incubator/datafu/apache-datafu-incubating-1.3.3/

Artifacts for DataFu are published in Apache's Maven Repository:

https://repository.apache.org/content/groups/public/org/apache/datafu/

Please visit the [Download](/docs/download.html) page for instructions on building from source or retrieving the artifacts in your build system.