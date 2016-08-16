---
title: Apache DataFu (incubating) 1.3.1 Released
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

I'd like to announce the release of Apache DataFu (incubating) 1.3.1.

Additions:

* New UDF CountDistinctUpTo that counts tuples within a bag to a preset limit (DATAFU-117)

Improvements:

* TupleFromBag and FirstTupleFromBag now implement Accumulator interface as well (DATAFU-114, DATAFU-115)

Build System:

* IntelliJ Idea support added to build file (DATAFU-103)
* JDK version now validated when building (DATAFU-95)

The source release can be obtained from:

http://www.apache.org/dyn/closer.cgi/incubator/datafu/apache-datafu-incubating-1.3.1/

Artifacts for DataFu are published in Apache's Maven Repository:

https://repository.apache.org/content/groups/public/org/apache/datafu/

Please follow the [Quick Start](/docs/quick-start.html) for instructions on building from source or retrieving the artifacts in your build system.