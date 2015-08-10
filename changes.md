# 1.3.0 (upcoming release)

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