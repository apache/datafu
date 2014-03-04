---
title: Contributing - Apache DataFu Pig
section_name: Apache DataFu Pig
---

# Contributing

We welcome contributions to the Apache DataFu Pig library!  If you are interested please follow the guide below.

## Get the Code

To clone the repository run the following command:

    git clone git://git.apache.org/incubator-datafu.git

## Eclipse

The following command generates the necessary files to load the project in Eclipse:

    ./gradlew eclipse

To clean up the eclipse files:

    ./gradlew cleanEclipse

Note that you may run out of heap when executing tests in Eclipse.  To fix this adjust your heap settings for the TestNG plugin.  Go to Eclipse->Preferences.  Select TestNG->Run/Debug.  Add "-Xmx1G" to the JVM args.

## Build the JAR

The Apache DataFu Pig library can be built by running the command below. 

    ./gradlew assemble

The built JAR can be found under `datafu-pig/build/libs` by the name `datafu-pig-x.y.z.jar`, where x.y.z is the version.
    
## Running Tests

All the tests can be run from within eclipse.  However they can also be run from the command line.  To run all the tests:

    ./gradlew test

To run a specific set of tests from the command line, you can define the `test.single` system property.  For example, to run all tests defined in `QuantileTests`:

    ./gradlew :datafu-pig:test -Dtest.single=QuantileTests


