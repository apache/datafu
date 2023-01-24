---
title: Apache DataFu-Spark 1.7.0 Released
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

I'd like to announce the release of Apache DataFu-Spark 1.7.0.

Many thanks to new contributors Arpit Bhardwaj, Ben Rahamim and Shaked Aharon! 

---

**Additions**

* Add collectLimitedList and dedupRandomN methods (DATAFU-165)
* Improve broadcastJoinSkewed function performance and allow all join types (DATAFU-170)

**Improvements**

* Upgrade Log4j version (DATAFU-162)
* Added count filtering option to broadcastJoinSkewed
 
**Fixes**

* explodeArray method not exposed in Python (DATAFU-163)

**Breaking changes**

* Spark 2.1.x no longer supported

---

The source release can be obtained from:

http://www.apache.org/dyn/closer.cgi/datafu/apache-datafu-1.7.0/

Artifacts for DataFu are published in Apache's Maven Repository:

https://repository.apache.org/content/groups/public/org/apache/datafu/

Please visit the [Download](/docs/download.html) page for instructions on building from source or retrieving the artifacts in your build system.
