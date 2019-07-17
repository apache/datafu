# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#!/bin/bash

export SPARK_VERSIONS_FOR_SCALA_210="2.1.0 2.1.1 2.1.2 2.1.3 2.2.0 2.2.1 2.2.2"
export SPARK_VERSIONS_FOR_SCALA_211="2.1.0 2.1.1 2.1.2 2.1.3 2.2.0 2.2.1 2.2.2 2.3.0 2.3.1 2.3.2 2.4.0 2.4.1 2.4.2 2.4.3"
export SPARK_VERSIONS_FOR_SCALA_212="2.4.0 2.4.1 2.4.2 2.4.3"

export LATEST_SPARK_VERSIONS_FOR_SCALA_210="2.1.3 2.2.2"
export LATEST_SPARK_VERSIONS_FOR_SCALA_211="2.1.3 2.2.2 2.3.2 2.4.3"
export LATEST_SPARK_VERSIONS_FOR_SCALA_212="2.4.3"

STARTTIME=$(date +%s)

function log {
  echo $1
  if [[ $LOG_FILE != "NONE" ]]; then
    echo $1 >> $LOG_FILE
  fi
}

function build {
  echo "----- Building versions for Scala $scala, Spark $spark ----"
  if ./gradlew :datafu-spark:clean; then
    echo "----- Clean for Scala $scala, spark $spark succeeded"
    if ./gradlew :datafu-spark:assemble -PscalaVersion=$scala -PsparkVersion=$spark; then
      echo "----- Build for Scala $scala, spark $spark succeeded"
      if ./gradlew :datafu-spark:test -PscalaVersion=$scala -PsparkVersion=$spark $TEST_PARAMS; then
        log "Testing for Scala $scala, spark $spark succeeded"
        if [[ $JARS_DIR != "NONE" ]]; then
          cp datafu-spark/build/libs/*.jar $JARS_DIR/
        fi
      else
        log "Testing for Scala $scala, spark $spark failed (build succeeded)"
      fi
    else
      log "Build for Scala $scala, spark $spark failed"
    fi
  else
    log "Clean for Scala $scala, Spark $spark failed"
  fi
}

# -------------------------------------

export JARS_DIR=NONE
export LOG_FILE=NONE

while getopts "l:j:t:hq" arg; do
        case $arg in
                l)
                        LOG_FILE=$OPTARG
                        ;;
                j)
                        JARS_DIR=$OPTARG
                        ;;
                t)
                        TEST_PARAMS=$OPTARG
                        ;;
                q)
                        SPARK_VERSIONS_FOR_SCALA_210=$LATEST_SPARK_VERSIONS_FOR_SCALA_210
                        SPARK_VERSIONS_FOR_SCALA_211=$LATEST_SPARK_VERSIONS_FOR_SCALA_211
                        SPARK_VERSIONS_FOR_SCALA_212=$LATEST_SPARK_VERSIONS_FOR_SCALA_212
                        ;;
                h)
                        echo "Builds and tests datafu-spark in multiple Scala/Spark combinations"
                        echo "Usage: build_ and_test_spark <options>"
                        echo "  -t    Optional. Name of param for passing to Gradle testing - for example, to only run the test 'FakeTest' pass '-Dtest.single=FakeTest'"
                        echo "  -j    Optional. Dir for putting artifacts that have compiled and tested successfully"
                        echo "  -l    Optional. Name of file for writing build summary log"
                        echo "  -q    Optional. Quick - only build and test the latest minor version of each major Spark release"
                        echo "  -h    Optional. Prints this help"
                        exit 0
                        ;;
        esac
done

if [[ $LOG_FILE != "NONE" ]]; then
  echo "Building datafu-spark: $TEST_PARAMS" > $LOG_FILE
fi

if [[ $JARS_DIR != "NONE" ]]; then
  echo "Copying successfully built and tested jars to $JARS_DIR" > $LOG_FILE
  mkdir $JARS_DIR
fi

export scala=2.10
for spark in $SPARK_VERSIONS_FOR_SCALA_210; do
  build
done

export scala=2.11
for spark in $SPARK_VERSIONS_FOR_SCALA_211; do
  build
done

export scala=2.12
for spark in $SPARK_VERSIONS_FOR_SCALA_212; do
  build
done

export ENDTIME=$(date +%s)

log "Build took $(((($ENDTIME - $STARTTIME))/60)) minutes, $((($ENDTIME - $STARTTIME)%60)) seconds"

