# Apache DataFu

[Apache DataFu](http://datafu.incubator.apache.org) is a collection of libraries for working with large-scale data in Hadoop.
The project was inspired by the need for stable, well-tested libraries for data mining and statistics.

It consists of two libraries:

* **Apache DataFu Pig**: a collection of user-defined functions for [Apache Pig](http://pig.apache.org/)
* **Apache DataFu Hourglass**: an incremental processing framework for [Apache Hadoop](http://hadoop.apache.org/) in MapReduce

For more information please visit the website:

* [http://datafu.incubator.apache.org/](http://datafu.incubator.apache.org/)

If you'd like to jump in and get started, check out the corresponding guides for each library:

* [Apache DataFu Pig - Getting Started](http://datafu.incubator.apache.org/docs/datafu/getting-started.html)
* [Apache DataFu Hourglass - Getting Started](http://datafu.incubator.apache.org/docs/hourglass/getting-started.html)

## Blog Posts

* [Introducing DataFu](http://datafu.incubator.apache.org/blog/2012/01/10/introducing-datafu.html)
* [DataFu: The WD-40 of Big Data](http://datafu.incubator.apache.org/blog/2013/01/24/datafu-the-wd-40-of-big-data.html)
* [DataFu 1.0](http://datafu.incubator.apache.org/blog/2013/09/04/datafu-1-0.html)
* [DataFu's Hourglass: Incremental Data Processing in Hadoop](http://datafu.incubator.apache.org/blog/2013/10/03/datafus-hourglass-incremental-data-processing-in-hadoop.html)

## Presentations 

* [A Brief Tour of DataFu](http://www.slideshare.net/matthewterencehayes/datafu)
* [Building Data Products at LinkedIn with DataFu](http://www.slideshare.net/matthewterencehayes/building-data-products-at-linkedin-with-datafu)
* [Hourglass: a Library for Incremental Processing on Hadoop (IEEE BigData 2013)](http://www.slideshare.net/matthewterencehayes/hourglass-a-library-for-incremental-processing-on-hadoop)

## Papers

* [Hourglass: a Library for Incremental Processing on Hadoop (IEEE BigData 2013)](http://www.slideshare.net/matthewterencehayes/hourglass-27038297)

## Getting Help

Bugs and feature requests can be filed [here](https://issues.apache.org/jira/browse/DATAFU).  For other help please see the [discussion group](http://groups.google.com/group/datafu).

## Developers

### DataFu Pig

#### Building the Code

The Apache DataFu Pig library can be built by running the command below.  More information about working with the source
code can be found in the [DataFu Pig Contributing Guide](http://datafu.incubator.apache.org/docs/datafu/contributing.html).

```
./gradlew assemble
```

The built JAR can be found under `datafu-pig/build/libs` by the name `datafu-pig-x.y.z.jar`, where x.y.z is the version.

#### Generating Eclipse Files

This command generates the eclipse project and classpath files:

```
./gradlew eclipse
```

To clean up the eclipse files:

```
./gradlew cleanEclipse
```

#### Running the Tests

To run all the tests:

```
./gradlew test
```

To run tests for a single class, use the `test.single` property.  For example, to run only the QuantileTests:

```
./gradlew :datafu-pig:test -Dtest.single=QuantileTests
```

The tests can also be run from within eclipse.  Note that you may run out of heap when executing tests in Eclipse. To fix this adjust your heap settings for the TestNG plugin. Go to Eclipse->Preferences. Select TestNG->Run/Debug. Add "-Xmx1G" to the JVM args.

### DataFu Hourglass

#### Building the Code

The Apache DataFu Pig library can be built by running the commands below.  More information about working with the source
code can be found in the [DataFu Hourglass Contributing Guide](http://datafu.incubator.apache.org/docs/hourglass/contributing.html).

```
cd contrib/hourglass
ant jar
```
