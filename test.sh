#!/usr/bin/env bash

echo $$ > test.pid
./gradlew clean test
rm test.pid
