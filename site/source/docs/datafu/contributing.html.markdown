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

    ant eclipse

Note that you may run out of heap when executing tests in Eclipse.  To fix this adjust your heap settings for the TestNG plugin.  Go to Eclipse->Preferences.  Select TestNG->Run/Debug.  Add "-Xmx1G" to the JVM args.

## Build the JAR

    ant jar
    
## Running Tests

All the tests can be run from within eclipse.  However they can also be run from the command line.  To run all the tests:

    ant test

To run specific tests from the command line, you have a few options.  One option is to run all tests within a given class.  To do this, run the same command but override `testclasses.pattern`, which defaults to `**/*.class`.  For example, to run all tests defined in `QuantileTests`:

    ant test -Dtestclasses.pattern=**/QuantileTests.class

The other option is to run a specific test within a class:

    ant test -Dtest.methods=datafu.test.pig.stats.QuantileTests.quantileTest

You may also provide a command separated list of tests:

    ant test -Dtest.methods=datafu.test.pig.stats.QuantileTests.quantileTest,datafu.test.pig.stats.QuantileTests.quantile2Test

## Compute code coverage

We use [Cobertura](http://cobertura.github.io/cobertura/) to compute code coverage, which can be run with:

    ant coverage

