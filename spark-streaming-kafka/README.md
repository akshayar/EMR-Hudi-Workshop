
# Spark Command
## Message Content pushed to the topic
The filePath here is the path to the file which got added to S3 by DMS. An S3 event gets published which is consumed by Lambda. The lambda then pushes the event below to the Spart topic which the file path of the file that got ingested. 
```
{
    "filePath": "s3://<bucket-name>/dms-full-load-path/salesdb/SALES_ORDER_DETAIL/20211118-100428844.parquet"
}
```

## Spark Submit

```aidl
spark-submit \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--conf "spark.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/spark/jars/httpclient-4.5.9.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1
--class kafka.hudi.latefile.SparkConsumer spark-structured-streaming-kafka-hudi_2.12-1.0.jar \
 <BUCKET_NAME> <KAFKA_BOOTSTRAP_SERVER> <TOPIC> <COW/MOR> <TABLE_NAME> <DB_NAME> <STARTING_POS earliest/lates>
 
 
 spark-submit \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--conf "spark.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/spark/jars/httpclient-4.5.9.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1
--class kafka.hudi.latefile.SparkConsumer spark-structured-streaming-kafka-hudi_2.12-1.0.jar \
 <BUCKET_NAME> <KAFKA_BOOTSTRAP_SERVER> <TOPIC> <COW/MOR> <TABLE_NAME> <DB_NAME> <STARTING_POS earliest/lates>

spark-submit \
--deploy-mode cluster \
--master yarn \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--conf "spark.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/spark/jars/httpclient-4.5.9.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1 \
--class kafka.hudi.latefile.SparkConsumer spark-structured-streaming-kafka-hudi_2.12-1.0.jar \
akshaya-hudi-experiments ip-10-192-11-254.ap-south-1.compute.internal:29092 data-stream-ingest COW equity_trade_records demohudi earliest
```


## Spark Shell
```aidl             
spark-shell \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--conf "spark.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/spark/jars/httpclient-4.5.9.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1

```

## Hudi DeltStreamer
1. The example below shows following use case JSON data on Kafka. The schema of data is being read from a local files on HDFS. The table is being synched with Glue Catalog 
2. The instructions below work for EMR Version 6.4.0.
There is following error with EMR Version 6.3.0
```shell
Caused by: java.lang.NoSuchMethodError: org.apache.hadoop.hive.ql.Driver.close()V
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQLs(HoodieHiveClient.java:417)
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQLUsingHiveDriver(HoodieHiveClient.java:384)
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQL(HoodieHiveClient.java:374)
at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:122)
at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:94)
```
3. Run following command for help on DeltaStreamer.
```shell
spark-submit \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
--jars `ls /usr/lib/hudi/hudi-utilities-bundle*.jar` \
--help
```   
4. Copy Jars
```shell
wget https://repo1.maven.org/maven2/org/apache/calcite/calcite-core/1.29.0/calcite-core-1.29.0.jar .
wget https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar .
sudo cp calcite-core-1.29.0.jar /usr/lib/spark/jars
sudo cp libfb303-0.9.3.jar /usr/lib/spark/jars
```
5. Copy properties file and schema file to HDFS from where the program will read it. 
```shell
hdfs dfs -copyFromLocal -f TradeData.avsc /
hdfs dfs -copyFromLocal -f hudi-deltastreamer.properties /
```
6. Run Spark Submit program
```shell

spark-submit \
--class  org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
--jars `ls /usr/lib/hudi/hudi-utilities-bundle*.jar ` \
--checkpoint s3://akshaya-hudi-experiments/demo/kafka-stream-data-checkpoint/table_delta_streamer_cow/ \
--continuous  \
--enable-hive-sync \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
--spark-master yarn \
--table-type COPY_ON_WRITE \
--target-base-path s3://akshaya-hudi-experiments/demo/hudi/table_delta_streamer_cow \
--target-table table_delta_streamer_cow \
--op UPSERT \
--source-ordering-field tradeId \
--props  hdfs:///hudi-deltastreamer.properties

```
