#!/usr/bin/env bash

echo $$ > test.pid
gradle test
rm test.pid
