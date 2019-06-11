#!/usr/bin/env bash

spark-submit \
--class com.sokeran.interview.App \
--master local[*] \
./target/scala-2.12/test-task-assembly-0.1.jar \
--input ./all.csv --output-folder output
