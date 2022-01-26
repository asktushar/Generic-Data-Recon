#!/bin/bash

set -x

  taskType=$1
  master=$2
  complaintsSourcePath=$3
  categorySourcePath=$4
  serviceSourcePath=$5
  complaintsReportPath=$6

run() {
  ./opt/spark/bin/spark-submit
  --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/complaintReport/log4j.properties
  --conf spark.executor.cores=4
  --conf spark.executor.memory=64G
  --conf spark.driver.memory=16G
  --conf spark.jars.packages=com.asktushar.playground:MyScalaPlayground-0.0.1-SNAPSHOT
  --conf spark.kubernetes.executor.deleteOnTermination=true
  --conf spark.kubernetes.executor.limit.cores=4
  --conf spark.kubernetes.executor.request.cores=4
  --conf spark.network.timeout=30s
  --conf spark.eventLog.dir=hdfs:///user/complaintReport/spark-logs/
  --keytab /var/run/secrets/asktushar.com/complaintReport.keytab
  --principal complaintReport@asktushar.COM
  --class com.asktushar.playground.Main
  MyScalaPlayground-0.0.1-SNAPSHOT.jar taskType master complaintsSourcePath categorySourcePath serviceSourcePath complaintsReportPath
}

if [ -n "$taskType" ] && [ -n "$master" ] && [ -n "$complaintsSourcePath" ] && [ -n "$categorySourcePath" ] && [ -n "$serviceSourcePath" ] && [ -n "$complaintsReportPath" ]; then
  echo "All required variables are provided"
  run
else
  echo "All required variables are not provided. Please try again!"
  exit(1)
fi