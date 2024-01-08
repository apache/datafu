# 2.0.0

Improvements

* Spark 3.0.0 - 3.1.3 supported (DATAFU-169)
* New Aggregators replace deprecated UserDefinedAggregateFunction for  (DATAFU-173)
 
Breaking changes

* Spark 2.x no longer supported


# 1.8.0

Improvements

* dedupWithCombiner method now supports a list of columns in the order / group by params  (DATAFU-171)
* Scala Python bridge now uses secure gateway (DATAFU-167)
 
Breaking changes

* Spark 2.2.0, 2.2.1, and 2.3.0. no longer supported


# 1.7.0

Additions

* Add collectLimitedList and dedupRandomN methods (DATAFU-165)
* Improve broadcastJoinSkewed function performance and allow all join types (DATAFU-170)

Improvements

* Upgrade Log4j version (DATAFU-162)
* Added count filtering option to broadcastJoinSkewed
 
Fixes

* explodeArray method not exposed in Python (DATAFU-163)

Breaking changes

* Spark 2.1.x no longer supported

# 1.6.1

Additions

* Explode Array method (DATAFU-154)

Improvements

* Add support for newer versions of Gradle (DATAFU-157)
* Document Explode Array usage recommendation (DATAFU-158)

Fixes

* Gradle build fails (DATAFU-156)

# 1.6.0

Additions:

* datafu-spark library (DATAFU-148)

Improvements:

* Remove log suppression in unit tests (DATAFU-82)

Fixes:

* Failure to assemble due to jcenter HTTP usage (DATAFU-152)

# 1.5.0

Additions:

* dedup macro (DATAFU-129)
* sample_by_keys macro (DATAFU-127)

Improvements:

* Update Ruby gem for site generation (DATAFU-147)
* Make DataFu compile with Java 8 (DATAFU-132)

Changes:

* Upgrade to Gradle v4.8.1 (DATAFU-146)

# 1.4.0

Changes:

* Removed MD5 hash for source release artifact.

# 1.3.3

Additions:

* UDF for hash functions such as murmur3 and others. (DATAFU-47)
* UDF for diffing tuples. (DATAFU-119)
* Support for macros in DataFu.  Macros count_all_non_distinct and count_distinct_keys were added. (DATAFU-123)
* Macro for TFIDF. (DATAFU-61)

Improvements:

* Added lifecylce hooks to ContextualEvalFunc. (DATAFU-50)
* SessionCount and Sessionize now support millisecond precision. (DATAFU-124)
* Upgraded to Guava 20.0. (DATAFU-48)
* Updated Gradle to 3.5.1. (DATAFU-125)
* Rat tasks automatically run during assemble. (DATAFU-118)
* Building now works on Windows. (DATAFU-99)

# 1.3.2

Improvements:

* LICENSE, NOTICE, and DISCLAIMER now included in META-INF of JARs.
* Test files now generated to build/test-files within projects.
* AliasableEvalFunc now uses getInputSchema.

# 1.3.1

Additions:

* New UDF CountDistinctUpTo that counts tuples within a bag to a preset limit (DATAFU-117)

Improvements:

* TupleFromBag and FirstTupleFromBag now implement Accumulator interface as well (DATAFU-114, DATAFU-115)

Build System:

* IntelliJ Idea support added to build file (DATAFU-103)
* JDK version now validated when building (DATAFU-95)

# 1.3.0

Additions:

* New UDFs for entropy and weighted sampling algorithms (DATAFU-2, DATAFU-26)
* Updated SimpleRandomSample to be consistent with SimpleRandomSampleWithReplacement (DATAFU-5)
* Created OpenNLP UDF wrappers (DATAFU-8)
* Created RandomUUID UDF (DATAFU-18)
* Added LSH implementation (DATAFU-37)
* Added Base64Encode/Decode (DATAFU-52)
* URLInfo UDF (DATAFU-62)
* Created SelectFieldByName UDF (DATAFU-69)
* Added generic BagJoin that supports inner, left, and full outer joins (DATAFU-70)
* Added ZipBags UDF which can zip and arbitrary number of bags into one (DATAFU-79)
* Hadoop 2.0 compatibility (DATAFU-58)
* Created TupleFromBag.java file (DATAFU-92)

Improvements:

* Simplified BagGroup output (DATAFU-42)

Changes:

* StagedOutputJob no longer writes counters by default (DATAFU-35)

Fixes:

* ReservoirSample does not behave as expected when grouping by a key other than ALL (DATAFU-11)
* DistinctBy does not work correctly on strings containing minuses (DATAFU-31)
* Hourglass does not honor "fail on missing" in all cases (DATAFU-35)
* Hash UDFs return zero-padded strings of uniform length even when leading bits are zero (DATAFU 46)
* UDF examples work again (DATAFU-49)
* SampleByKey can throw NullPointerException (DATAFU-68)

Build system:

* Removed legacy checked in jars (DATAFU-55)
* Updated to use Pig 0.12.1 (DATAFU-10)
* Switched from Ant to Gradle 1.12 (DATAFU-27, DATAFU-44, DATAFU-43, DATAFU-66)
* Removed checked in jars, download where necessary (DATAFU-55, DATAFU-55)
* Fixed test.sh to use gradlew (DATAFU-77)

Release related:

* NOTICE updated with dependencies used or shipped with DataFu.
* Apache license headers added to all necessary files (DATAFU-4, DATAFU-75)
* Added doap file (DATAFU-36)
* Source tarball generation, gradle bootstrapping, and release instructions (DATAFU-57, DATAFU-78, DATAFU-72)
* Removed author tags (DATAFU-74)
* Resolved issues with build-plugin directory (DATAFU-76)
* Used Apache RAT to verify correct file headers (DATAFU-73, DATAFU-84)

Documentation related:

* New website (DATAFU-20, etc.)
* StreamingQuantile PDF link is broken (DATAFU-29)
* README file updated

# 1.2.0

Additions:

* Pair of UDFs for simple random sampling with replacement.
* More dependencies now packaged in DataFu so fewer JAR dependencies required.
* SetDifference UDF for computing set difference A-B or A-B-C.
* HyperLogLogPlusPlus UDF for efficient cardinality estimation.

# 1.1.0

This release adds compatibility with Pig 0.12 (courtesy of jarcec).

Additions:

* Added SHA hash UDF.
* InUDF and AssertUDF added for Pig 0.12 compatibility.  These are the same as In and Assert.
* SimpleRandomSample, which implements a scalable simple random sampling algorithm.

Fixes:

* Fixed the schema declarations of several UDFs for compatibility with Pig 0.12, which is now stricter with schemas.

# 1.0.0

**This is not a backwards compatible release.**

Additions:

* Added SampleByKey, which provides a way to sample tuples based on certain fields.
* Added Coalesce, which returns the first non-null value from a list of arguments like SQL's COALESCE.
* Added BagGroup, which performs an in-memory group operation on a bag.
* Added ReservoirSample
* Added In filter func, which behaves like SQL's IN
* Added EmptyBagToNullFields, which enables multi-relation left joins using COGROUP
* Sessionize now supports long values for timestamp, in addition to string representation of time.
* BagConcat can now operate on a bag of bags, in addition to a tuple of bags
* Created TransposeTupleToBag, which creates a bag of key-value pairs from a tuple
* SessionCount now implements Accumulator interface
* DistinctBy now implements Accumulator interface
* Using PigUnit from Maven for testing, instead of checked-in JAR
* Added many more test cases to improve coverage
* Improved documentation

Changes:

* Moved WeightedSample to datafu.pig.sampling
* Using Pig 0.11.1 for testing.
* Renamed package datafu.pig.numbers to datafu.pig.random
* Renamed package datafu.pig.bag.sets to datafu.pig.sets
* Renamed TimeCount to SessionCount, moved to datafu.pig.sessions
* ASSERT renamed to Assert
* MD5Base64 merged into MD5 implementation, constructor arg picks which method, default being hex

Removals:

* Removed ApplyQuantiles
* Removed AliasBagFields, since can now achieve with nested foreach

Fixes:

* Quantile now outputs schemas consistent with StreamingQuantile
* Necessary fastutil classes now packaged in datafu JAR, so fastutil JAR not needed as dependency
* Non-deterministic UDFs now marked as so

# 0.0.10

Additions:

* CountEach now implements Accumulator
* Added AliasableEvalFunc, a base class to enable UDFs to access fields in tuple by name instead of position
* Added BagLeftOuterJoin, which can perform left join on two or more reasonably sized bags without a reduce

Fixes:

* StreamingQuantile schema fix

# 0.0.9

Additions:

* WeightedSample can now take a seed

Changes:

* Test against Pig 0.11.0

Fixes:

* Null pointer fix for Enumerate's Accumulator implementation
