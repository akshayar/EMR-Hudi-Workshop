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
    sbt assembly
    assembly_status=$?
    if [ $assembly_status == 0 ] 
    then
        
        aws s3 cp target/scala-2.11/Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar s3://$S3_BUCKET/
        
    else
    
        echo "Assembly Failed"
    
    fi
else

    echo "Compilation Failed"

fi