#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

# Build the published-version artifact once with the release Spark default from gradle.properties.
./gradlew :datafu-spark:clean :datafu-spark:assemble -Prelease=true
release_version="$(awk -F= '$1 == "version" { print $2; exit }' gradle.properties)"
if [[ -z "$release_version" ]]; then
  echo "Could not read the release version from gradle.properties" >&2
  exit 1
fi
released_jar="datafu-spark/build/libs/datafu-spark_2.12-${release_version}.jar"
if [[ ! -f "$released_jar" ]]; then
  echo "Expected release artifact was not built: $released_jar" >&2
  exit 1
fi
released_jar="$(cd "$(dirname "$released_jar")" && pwd)/$(basename "$released_jar")"

# Latest patch releases for each supported Spark minor line; keep this list explicit and current.
for spark_version in 3.0.3 3.1.3 3.2.4 3.3.4 3.4.2 3.5.9; do
  echo "Testing the same released DataFu JAR against Spark $spark_version"
  ./gradlew :datafu-spark:test \
    -PscalaVersion=2.12 \
    -PsparkVersion="$spark_version" \
    -PreleasedDatafuJar="$released_jar"
done
