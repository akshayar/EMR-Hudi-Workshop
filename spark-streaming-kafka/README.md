
# Spark Command
## Message Content pushed to the topic
The filePath here is the path to the file which got added to S3 by DMS. An S3 event gets published which is consumed by Lambda. The lambda then pushes the event below to the Spart topic which the file path of the file that got ingested. 
```json
{
    "filePath": "s3://<bucket-name>/dms-full-load-path/salesdb/SALES_ORDER_DETAIL/20211118-100428844.parquet"
}
```

## Spark Submit

```shell
spark-submit \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--conf "spark.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/spark/jars/httpclient-4.5.9.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1 \
--class kafka.hudi.latefile.SparkConsumer spark-structured-streaming-kafka-hudi_2.12-1.0.jar \
 <BUCKET_NAME> <KAFKA_BOOTSTRAP_SERVER> <TOPIC> <COW/MOR> <TABLE_NAME> <DB_NAME> <STARTING_POS earliest/latest>
 


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
```shell             
spark-shell \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--conf "spark.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/spark/jars/httpclient-4.5.9.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1

```

## Hudi DeltStreamer
1. The instructions below work for EMR Version 6.5.0.
There is following error with EMR Version 6.3.0
```shell
Caused by: java.lang.NoSuchMethodError: org.apache.hadoop.hive.ql.Driver.close()V
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQLs(HoodieHiveClient.java:417)
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQLUsingHiveDriver(HoodieHiveClient.java:384)
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQL(HoodieHiveClient.java:374)
at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:122)
at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:94)
```
2. Run following command for help on DeltaStreamer.
```shell
spark-submit \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
--jars `ls /usr/lib/hudi/hudi-utilities-bundle*.jar` \
--help
```   
3. Copy Jars
```shell
wget https://repo1.maven.org/maven2/org/apache/calcite/calcite-core/1.29.0/calcite-core-1.29.0.jar .
wget https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar .
sudo cp calcite-core-1.29.0.jar /usr/lib/spark/jars
sudo cp libfb303-0.9.3.jar /usr/lib/spark/jars
```
4. Copy properties file and schema file to HDFS from where the program will read it. Copy of schema is required if you are using FilebasedSchemaProvider.
   
   <br>JSON Data , File Schema [hudi-delta-streamer/hudi-deltastreamer-schema-file-json.properties](hudi-delta-streamer/hudi-deltastreamer-schema-file-json.properties)
   <br>Schema File [hudi-delta-streamer/TradeData.avsc](hudi-delta-streamer/TradeData.avsc)   
   
   <br>AVRO Data , Schema Registry [hudi-delta-streamer/hudi-deltastreamer-schema-registry-avro.properties](hudi-delta-streamer/hudi-deltastreamer-schema-registry-avro.properties)
   

```shell
hdfs dfs -copyFromLocal -f TradeData.avsc /
hdfs dfs -copyFromLocal -f hudi-deltastreamer-schema-file-json.properties /
```
5. Run Spark Submit program. 
   JSON data on Kafka, Schema read from a local files on HDFS. The table is being synched with Glue Catalog

```shell

spark-submit \
--class  org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
--jars `ls /usr/lib/hudi/hudi-utilities-bundle*.jar ` \
--checkpoint s3://akshaya-hudi-experiments/demo/kafka-stream-data-checkpoint/table_delta_streamer_cow_2/ \
--continuous  \
--enable-hive-sync \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
--spark-master yarn \
--table-type COPY_ON_WRITE \
--target-base-path s3://akshaya-hudi-experiments/demo/hudi/table_delta_streamer_cow_2 \
--target-table table_delta_streamer_cow_2 \
--op UPSERT \
--source-ordering-field tradeId \
--props  hdfs:///hudi-deltastreamer-schema-file-json.properties

```

7. Run Spark Submit program. 
   AVRO data on Kafka, Schema read from schema registery. The table is being synched with Glue Catalog

```shell

spark-submit \
--class  org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
--jars `ls /usr/lib/hudi/hudi-utilities-bundle*.jar ` \
--checkpoint s3://akshaya-hudi-experiments/demo/kafka-stream-data-checkpoint/table_delta_streamer_cow_2_avro/ \
--continuous  \
--enable-hive-sync \
--schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
--source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
--spark-master yarn \
--table-type COPY_ON_WRITE \
--target-base-path s3://akshaya-hudi-experiments/demo/hudi/table_delta_streamer_cow_2 \
--target-table table_delta_streamer_cow_2 \
--op UPSERT \
--source-ordering-field tradeId \
--props  hdfs:///hudi-deltastreamer-schema-registry-avro.properties

```
### Hudi Delta Streamer : To Do 
1. Try with spark.executor.extraClassPath and spark.driver.extraClassPath configuration.
```shell
--conf spark.executor.extraClassPath=/home/hadoop/jars/calcite-core-1.29.0.jar:/home/hadoop/jars/libfb303-0.9.3.jar \
--conf spark.driver.extraClassPath=/home/hadoop/jars/calcite-core-1.29.0.jar:/home/hadoop/jars/libfb303-0.9.3.jar \

```
2. Try with AWS DMS. 

### Hudi Delta Streamer : Errors Faced
1. The error below was thrown when I used hoodie.datasource.hive_sync.use_jdbc=false. This went away after removing this entry. 
```shell

Caused by: NoSuchObjectException(message:Table table_delta_streamer_cow_2 not found. (Service: AWSGlue; Status Code: 400; Error Code: EntityNotFoundException; Request ID: c31d5605-cd58-4147-ae11-f1dde04a5d5a; Proxy: null))
	at com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter$1.get(BaseCatalogToHiveConverter.java:90)
	at com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter.getHiveException(BaseCatalogToHiveConverter.java:109)
	at com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter.wrapInHiveException(BaseCatalogToHiveConverter.java:100)
	at com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate.getCatalogPartitions(GlueMetastoreClientDelegate.java:1129)
	at com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate.access$200(GlueMetastoreClientDelegate.java:162)
	at com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate$3.call(GlueMetastoreClientDelegate.java:1023)
	at com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate$3.call(GlueMetastoreClientDelegate.java:1020)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)

```
2. The error below happened with EMR Version 6.3.0
```shell
Caused by: java.lang.NoSuchMethodError: org.apache.hadoop.hive.ql.Driver.close()V
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQLs(HoodieHiveClient.java:417)
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQLUsingHiveDriver(HoodieHiveClient.java:384)
at org.apache.hudi.hive.HoodieHiveClient.updateHiveSQL(HoodieHiveClient.java:374)
at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:122)
at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:94)
```