---
title: Contributing - Community
section_name: Community
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

# Contributing

We welcome contributions to the Apache DataFu.  If you're interested, please read the following guide:

https://cwiki.apache.org/confluence/display/DATAFU/Contributing+to+Apache+DataFu

## Working in the Code Base

Common tasks for working in the DataFu code can be found below.  For information on how to contribute patches, please
follow the wiki link above.

### Get the Code

If you haven't done so already:

    git clone https://git-wip-us.apache.org/repos/asf/incubator-datafu.git
    cd incubator-datafu

### Generate Eclipse Files

The following command generates the necessary files to load the project in Eclipse:

    ./gradlew eclipse

To clean up the eclipse files:

    ./gradlew cleanEclipse

Note that you may run out of heap when executing tests in Eclipse.  To fix this adjust your heap settings for the TestNG plugin.  Go to Eclipse->Preferences.  Select TestNG->Run/Debug.  Add "-Xmx1G" to the JVM args.

### Building

All the JARs for the project can be built with the following command:

    ./gradlew assemble

This builds SNAPSHOT versions of the JARs for both DataFu Pig and Hourglass.  The built JARs can be found under `datafu-pig/build/libs` and `datafu-hourglass/build/libs`, respectively.

The Apache DataFu Pig library can be built by running the command below.

    ./gradlew :datafu-pig:assemble
    ./gradlew :datafu-hourglass:assemble

### Running Tests

Tests can be run with the following command:

    ./gradlew test

All the tests can also be run from within eclipse.

To run the DataFu Pig or Hourglass tests specifically:

    ./gradlew :datafu-pig:test
    ./gradlew :datafu-hourglass:test

To run a specific set of tests from the command line, you can define the `test.single` system property with a value matching the test class you want to run.  For example, to run all tests defined in the `QuantileTests` test class for DataFu Pig:

    ./gradlew :datafu-pig:test -Dtest.single=QuantileTests

You can similarly run a specific Hourglass test like so:

    ./gradlew :datafu-hourglass:test -Dtest.single=PartitionCollapsingTests
