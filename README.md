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

Bugs and feature requests can be filed [here](https://issues.apache.org/jira/browse/DATAFU).  For other help please see the [website](http://datafu.incubator.apache.org/).

## Developers

### Source release

If you are starting from a source release, then you'll want to verify the release is valid and bootstrap the build environment.

To verify that the archive has the correct MD5 checksum, the following two commands can be run.  These should produce the same output.

  openssl md5 < apache-datafu-sources-x.y.z-incubating.tgz
  cat apache-datafu-sources-x.y.z-incubating.tgz.MD5

To verify the archive against its signature, you can run:

  gpg2 --verify apache-datafu-sources-x.y.z-incubating.tgz.asc

The command above will assume you are verifying `apache-datafu-sources-x.y.z-incubating.tgz` and produce "Good signature" if the archive is valid.

To build DataFu from a source release, it is first necessary to download a gradle wrapper script.  This bootstrapping process requires Gradle to be installed on the source machine.  Gradle is available through most package managers or directly from [its website](http://www.gradle.org/).  Once you have installed Gradle and have ensured that the `gradle` is available in your path, you can bootstrap the wrapper with:

    gradle -b bootstrap.gradle

After the bootstrap script has completed, you should find a `gradlew` script in the root of the project.  The regular gradlew instructions below should then be available.

When building from a source release, the version for all generated artifacts will be of the form `x.y.z`.  If you were to clone the git repo and build you would find `-SNAPSHOT` appended to the version.  This helps to distinguish official releases from those generated from the code repository for testing purposes.

### Building the Code

To build DataFu from a git checkout or binary release, run:

    ./gradlew clean assemble

The datafu-pig JAR can be found under `datafu-pig/build/libs`.  The artifact name will be of the form `datafu-pig-incubating-x.y.z.jar` if this is a source release and `datafu-pig-incubating-x.y.z-SNAPSHOT.jar` if this is being built from the code repository.

The datafu-hourglass can be found in the `datafu-hourglass/build/libs` directory.

### Generating Eclipse Files

This command generates the eclipse project and classpath files:

    ./gradlew eclipse

To clean up the eclipse files:

    ./gradlew cleanEclipse

### Running the Tests

To run all the tests:

    ./gradlew test

To run only the DataFu Pig tests:

    ./gradlew :datafu-pig:test

To run only the DataFu Hourglass tests:

    ./gradlew :datafu-hourglass:test

To run tests for a single class, use the `test.single` property.  For example, to run only the QuantileTests:

    ./gradlew :datafu-pig:test -Dtest.single=QuantileTests

The tests can also be run from within eclipse.  You'll need to install the TestNG plugin for Eclipse.  See: http://testng.org/doc/download.html.

Potential issues and workaround:
* You may run out of heap when executing tests in Eclipse. To fix this adjust your heap settings for the TestNG plugin. Go to Eclipse->Preferences. Select TestNG->Run/Debug. Add "-Xmx1G" to the JVM args.
* You may get a "broken pipe" error when running tests.  If so right click on the project, open the TestNG settings, and uncheck "Use project TestNG jar".

