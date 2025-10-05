# Apache DataFu

Apache DataFu is a collection of libraries for working with large-scale data in Hadoop.  The project includes libraries for data analysis and data mining.

## Getting Started

### Prerequisites

* Java 8 or higher
* Hadoop 2.x or 3.x
* Gradle 9.x (for building from source)

### Installation

Download the latest release from the [releases page](https://github.com/apache/datafu/releases).

### Building from Source

Clone the repository:

    git clone https://github.com/apache/datafu.git
    cd datafu

Build the project:

    ./gradlew clean assemble

### Running Tests

To run all tests:

    ./gradlew test

To run tests for a specific module:

    ./gradlew :datafu-pig:test

## Modules

### DataFu Pig

DataFu Pig provides a collection of useful user-defined functions (UDFs) for Apache Pig.

### DataFu Hourglass

DataFu Hourglass is a library for incremental data processing in Hadoop.

### DataFu Spark

DataFu Spark provides utilities for Apache Spark.

## Documentation

* [DataFu Pig Documentation](http://datafu.apache.org/docs/datafu/)
* [DataFu Hourglass Documentation](http://datafu.apache.org/docs/hourglass/)
* [DataFu Spark Documentation](http://datafu.apache.org/docs/spark/)

## Contributing

We welcome contributions! Please see our [contributing guide](http://datafu.apache.org/community/contributing.html) for details.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Support

* [Mailing Lists](http://datafu.apache.org/community/mailing-lists.html)
* [Issue Tracker](https://issues.apache.org/jira/browse/DATAFU)
* [Website](http://datafu.apache.org/)

## Release Information

### Building from Source Release

To build DataFu from a source release, first verify the signature:

    gpg2 --verify apache-datafu-sources-x.y.z.tgz.asc

The command above will assume you are verifying `apache-datafu-sources-x.y.z.tgz` and produce "Good signature" if the archive is valid.

To build DataFu from a source release, it is first necessary to download a gradle wrapper script.  This bootstrapping process requires Gradle to be installed on the source machine.  Gradle is available through most package managers or directly from [its website](http://www.gradle.org/).  Once you have installed Gradle and have ensured that the `gradle` is available in your path, you can bootstrap the wrapper with:

    gradle -p . bootstrap.gradle

After the bootstrap script has completed, you should find a `gradlew` script in the root of the project.  The regular gradlew instructions below should then be available.

When building from a source release, the version for all generated artifacts will be of the form `x.y.z`.  If you were to clone the git repo and build you would find `-SNAPSHOT` appended to the version.  This helps to distinguish official releases from those generated from the code repository for testing purposes.

### Building the Code

To build DataFu from a git checkout or binary release, run:

    ./gradlew clean assemble

Each project's jars can be found under the corresponding sub directory. For example, the datafu-pig JAR can be found under `datafu-pig/build/libs`.  The artifact name will be of the form `datafu-pig-x.y.z.jar` if this is a source release and `datafu-pig-x.y.z-SNAPSHOT.jar` if this is being built from the code repository.

### Generating Eclipse Files

This command generates the eclipse project and classpath files:

    ./gradlew eclipse

To load the projects in Eclipse:

  * Select "File -> Import", then "Existing Projects into Workspace"
  * Choose the root of the repository as the root directory
  * Check "Search for nested projects"
  * Click Finish

To clean up the eclipse files:

    ./gradlew cleanEclipse

### Using Intellij

If you would like to use Intellij, please import the base datafu directory as a Gradle project. You may run into the following issues:

* Source/Tests directories not marked - please mark them manually.
* Tests not identified - add junit manually to the module using Open Module Settings->Libraries
* When running tests you get _Error scala: Output path ... is shared between_ - change your output path as described [here](https://stackoverflow.com/questions/18920334/output-path-is-shared-between-the-same-module-error)

### Running the Tests

To run all the tests:

    ./gradlew test

To run only one module's tests - for example, only the DataFu Pig tests:

    ./gradlew :datafu-pig:test

To run tests for a single class, use the `tests` property.  For example, to run only the QuantileTests:

    ./gradlew :datafu-pig:test --tests QuantileTests

The tests can also be run from within Eclipse.  You'll need to install the TestNG plugin for Eclipse for DataFu Pig and Hourglass.  See: http://testng.org/doc/download.html.

Potential issues and workaround:
* You may run out of heap when executing tests in Eclipse. To fix this adjust your heap settings for the TestNG plugin. Go to Eclipse->Preferences. Select TestNG->Run/Debug. Add "-Xmx1G" to the JVM args.