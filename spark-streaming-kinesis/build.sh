#!/bin/bash
S3_BUCKET=$1
if [ -z "$S3_BUCKET" ] 
then
  S3_BUCKET=aksh-test-versioning
fi

sbt clean package
status=$?
if [ $status == 0 ] 
then
    aws s3 cp target/scala-2.11/spark-structured-streaming-kinesis-hudi_2.11-1.0.jar s3://$S3_BUCKET/
else

    echo "Compilation Failed"

fi