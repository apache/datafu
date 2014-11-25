---
title: Contributing - Apache DataFu Pig
section_name: Apache DataFu Pig
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

We welcome contributions to the Apache DataFu Pig library!  Please read the following guide on how to contribute to DataFu.  

https://cwiki.apache.org/confluence/display/DATAFU/Contributing+to+Apache+DataFu

## Common Tasks

Common tasks for working with the DataFu Pig code can be found below.  For information on how to contribute patches, please
follow the wiki link above.

### Get the Code

To clone the repository run the following command:

    git clone git://git.apache.org/incubator-datafu.git

### Generate Eclipse Files

The following command generates the necessary files to load the project in Eclipse:

    ./gradlew eclipse

To clean up the eclipse files:

    ./gradlew cleanEclipse

Note that you may run out of heap when executing tests in Eclipse.  To fix this adjust your heap settings for the TestNG plugin.  Go to Eclipse->Preferences.  Select TestNG->Run/Debug.  Add "-Xmx1G" to the JVM args.

### Build the JAR

The Apache DataFu Pig library can be built by running the command below. 

    ./gradlew assemble

The built JAR can be found under `datafu-pig/build/libs` by the name `datafu-pig-x.y.z.jar`, where x.y.z is the version.
    
### Running Tests

All the tests can be run from within eclipse.  However they can also be run from the command line.  To run all the tests:

    ./gradlew test

To run a specific set of tests from the command line, you can define the `test.single` system property.  For example, to run all tests defined in `QuantileTests`:

    ./gradlew :datafu-pig:test -Dtest.single=QuantileTests


