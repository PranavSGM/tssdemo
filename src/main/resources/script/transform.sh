#!/usr/bin/env bash

echo --Running the Spark-submit job .....

 /usr/lib/spark/bin/spark-submit \
  --class com.sc.eni.transform \
  --master local \
  ~/workspace/tssdemo/target/tss.demo-1.0-SNAPSHOT.jar

echo --Checking the transformed data files at L2 .....

hdfs dfs -ls -R /tssdemo2/