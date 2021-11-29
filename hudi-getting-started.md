## Spark SQL
```
-- Apache Hudi
spark-sql \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'


```

## Create Table
```
-- Apache Hudi
-- create a managed cow table
create table if not exists hudi_sample_table_cow (
    id bigint,    name string,    dt string,    hh string  
) using hudi
location 's3://akshaya-firehose-test/lake/compare/hudi_sample_table_cow'
options (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = '_hoodie_commit_time'
 ) 
partitioned by (dt, hh);

/// s3://akshaya-firehose-test/lake/compare/hudi_sample_table_cow/.hoodie/hoodie.properties
#Properties saved on Mon Nov 29 05:30:35 UTC 2021
#Mon Nov 29 05:30:35 UTC 2021
hoodie.table.precombine.field=_hoodie_commit_time
hoodie.table.partition.fields=dt,hh
hoodie.table.type=COPY_ON_WRITE
hoodie.archivelog.folder=archived
hoodie.timeline.layout.version=1
hoodie.table.version=2
hoodie.table.recordkey.fields=id
hoodie.table.name=hudi_sample_table_cow
hoodie.table.create.schema={"type"\:"record","name"\:"topLevelRecord","fields"\:[{"name"\:"_hoodie_commit_time","type"\:["string","null"]},{"name"\:"_hoodie_commit_seqno","type"\:["string","null"]},{"name"\:"_hoodie_record_key","type"\:["string","null"]},{"name"\:"_hoodie_partition_path","type"\:["string","null"]},{"name"\:"_hoodie_file_name","type"\:["string","null"]},{"name"\:"id","type"\:["long","null"]},{"name"\:"name","type"\:["string","null"]},{"name"\:"dt","type"\:["string","null"]},{"name"\:"hh","type"\:["string","null"]}]}




-- create an external mor table
create table if not exists hudi_sample_table_mor (
    id bigint,    name string,    dt string ,    hh string 
) using hudi
location 's3://akshaya-firehose-test/lake/compare/hudi_sample_table_mor'
options (
  type = 'mor',
  primaryKey = 'id',
  preCombineField = '_hoodie_commit_time' 
)
partitioned by (dt, hh);

/// s3://akshaya-firehose-test/lake/compare/hudi_sample_table_mor/.hoodie/hoodie.properties
#Properties saved on Mon Nov 29 05:30:57 UTC 2021
#Mon Nov 29 05:30:57 UTC 2021
hoodie.table.precombine.field=_hoodie_commit_time
hoodie.table.partition.fields=dt,hh
hoodie.table.type=MERGE_ON_READ
hoodie.archivelog.folder=archived
hoodie.compaction.payload.class=org.apache.hudi.common.model.DefaultHoodieRecordPayload
hoodie.timeline.layout.version=1
hoodie.table.version=2
hoodie.table.recordkey.fields=id
hoodie.table.name=hudi_sample_table_mor
hoodie.table.create.schema={"type"\:"record","name"\:"topLevelRecord","fields"\:[{"name"\:"_hoodie_commit_time","type"\:["string","null"]},{"name"\:"_hoodie_commit_seqno","type"\:["string","null"]},{"name"\:"_hoodie_record_key","type"\:["string","null"]},{"name"\:"_hoodie_partition_path","type"\:["string","null"]},{"name"\:"_hoodie_file_name","type"\:["string","null"]},{"name"\:"id","type"\:["long","null"]},{"name"\:"name","type"\:["string","null"]},{"name"\:"dt","type"\:["string","null"]},{"name"\:"hh","type"\:["string","null"]}]}



```
![Folder Structure](imges/hudi-folder.png)


## Insert Data
```
spark-shell \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import spark.implicits._
import org.apache.hudi.hive.MultiPartKeysValueExtractor
val tableName = "hudi_sample_table_cow"
val basePath = "s3://akshaya-firehose-test/lake/compare/hudi_sample_table_cow"



var someDF= Seq((1,"First","2021-11-28","12"),(2,"Second","2021-11-29","12")).toDF("id","name","dt","hh")

(someDF.write.format("hudi")
.options(getQuickstartWriteConfigs)
.option(PRECOMBINE_FIELD.key(), "id")
.option(RECORDKEY_FIELD.key(), "id")
.option(HIVE_PARTITION_FIELDS.key(), {"dt,hh"})
.option(HIVE_PARTITION_EXTRACTOR_CLASS.key(), classOf[MultiPartKeysValueExtractor].getName)
.option(TBL_NAME.key(), tableName)
. mode(Append).save(basePath))

val tableName2 = "hudi_sample_table_mor"
val basePath2 = "s3://akshaya-firehose-test/lake/compare/hudi_sample_table_mor"
(someDF.write.format("hudi")
.options(getQuickstartWriteConfigs)
.option(PRECOMBINE_FIELD.key(), "id")
.option(RECORDKEY_FIELD.key(), "id")
.option(HIVE_PARTITION_FIELDS.key(), {"dt,hh"})
.option(HIVE_PARTITION_EXTRACTOR_CLASS.key(), classOf[MultiPartKeysValueExtractor].getName)
.option(TBL_NAME.key(), tableName2)
. mode(Append).save(basePath2))

spark.read.format("hudi").load(basePath2)


-- Apache Hudi
insert into hudi_sample_table_cow select 1 as id, 'First', '2021-11-28' as dt, '12' as hh;
insert into hudi_sample_table_cow partition(dt = '2021-11-29') select 2, 'Second';

insert into hudi_sample_table_mor select 1 as id, 'First', '2021-11-28' as dt, '12' as hh;
insert into hudi_sample_table_mor partition(dt = '2021-11-29') select 2, 'Second';



```
```
--Content of .hoodie/20211129124353.commit for COW table
{
  "partitionToWriteStats": {
    "default": [
      {
        "fileId": "5a9f85e6-a74a-43b1-89bf-521102fcc212-0",
        "path": "default/5a9f85e6-a74a-43b1-89bf-521102fcc212-0_0-98-98_20211129124353.parquet",
        "prevCommit": "20211129123734",
        "numWrites": 1,
        "numDeletes": 0,
        "numUpdateWrites": 1,
        "numInserts": 0,
        "totalWriteBytes": 434867,
        "totalWriteErrors": 0,
        "tempPath": null,
        "partitionPath": "default",
        "totalLogRecords": 0,
        "totalLogFilesCompacted": 0,
        "totalLogSizeCompacted": 0,
        "totalUpdatedRecordsCompacted": 0,
        "totalLogBlocks": 0,
        "totalCorruptLogBlock": 0,
        "totalRollbackBlocks": 0,
        "fileSizeInBytes": 434867,
        "minEventTime": null,
        "maxEventTime": null
      }
    ]
  },
  "compacted": false,
  "extraMetadata": {
    "schema": "{\"type\":\"record\",\"name\":\"hudi_sample_table_cow_record\",\"namespace\":\"hoodie.hudi_sample_table_cow\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dt\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"hh\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  },
  "operationType": "UPSERT",
  "fileIdAndRelativePaths": {
    "5a9f85e6-a74a-43b1-89bf-521102fcc212-0": "default/5a9f85e6-a74a-43b1-89bf-521102fcc212-0_0-98-98_20211129124353.parquet"
  },
  "totalRecordsDeleted": 0,
  "totalLogRecordsCompacted": 0,
  "totalLogFilesCompacted": 0,
  "totalCompactedRecordsUpdated": 0,
  "totalLogFilesSize": 0,
  "totalScanTime": 0,
  "totalCreateTime": 0,
  "totalUpsertTime": 853,
  "minAndMaxEventTime": {
    "Optional.empty": {
      "val": null,
      "present": false
    }
  },
  "writePartitionPaths": [
    "default"
  ]
}


--- Content of   .hoodie/20211129131046.deltacommit for MOR table
{
  "partitionToWriteStats": {
    "default": [
      {
        "fileId": "3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0",
        "path": "default/.3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.15_0-476-462",
        "prevCommit": "20211129130020",
        "numWrites": 2,
        "numDeletes": 0,
        "numUpdateWrites": 2,
        "numInserts": 0,
        "totalWriteBytes": 1020,
        "totalWriteErrors": 0,
        "tempPath": null,
        "partitionPath": "default",
        "totalLogRecords": 0,
        "totalLogFilesCompacted": 0,
        "totalLogSizeCompacted": 0,
        "totalUpdatedRecordsCompacted": 0,
        "totalLogBlocks": 0,
        "totalCorruptLogBlock": 0,
        "totalRollbackBlocks": 0,
        "fileSizeInBytes": 1020,
        "minEventTime": null,
        "maxEventTime": null,
        "logVersion": 15,
        "logOffset": 0,
        "baseFile": "3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_0-20-21_20211129130020.parquet",
        "logFiles": [
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.14_0-444-431",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.13_0-412-400",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.12_0-380-369",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.11_0-348-338",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.10_0-316-307",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.9_0-287-279",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.8_0-258-251",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.7_0-229-223",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.6_0-200-195",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.5_0-171-167",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.4_0-142-139",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.3_0-113-111",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.2_0-84-83",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.1_0-52-52",
          ".3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.15_0-476-462"
        ]
      }
    ]
  },
  "compacted": false,
  "extraMetadata": {
    "schema": "{\"type\":\"record\",\"name\":\"hudi_sample_table_mor_record\",\"namespace\":\"hoodie.hudi_sample_table_mor\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dt\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"hh\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  },
  "operationType": "UPSERT",
  "totalRecordsDeleted": 0,
  "totalLogRecordsCompacted": 0,
  "totalLogFilesCompacted": 0,
  "totalCompactedRecordsUpdated": 0,
  "totalLogFilesSize": 0,
  "totalScanTime": 0,
  "totalCreateTime": 0,
  "totalUpsertTime": 438,
  "minAndMaxEventTime": {
    "Optional.empty": {
      "val": null,
      "present": false
    }
  },
  "writePartitionPaths": [
    "default"
  ],
  "fileIdAndRelativePaths": {
    "3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0": "default/.3ef578a8-7e0c-4cfc-99b0-afbe8e5b2e19-0_20211129130020.log.15_0-476-462"
  }
}
```

## Query Data
```
-- Apache Hudi
select id, name, dt, hh from  hudi_sample_table_cow where id=1 ;
select id, name, dt, hh from  hudi_sample_table_mor where id=1 ;
select * from  hudi_sample_table_cow ;
select * from  hudi_sample_table_mor ;

 
```

## Update Merge

```
-- Apache Hudi
merge into hudi_sample_table_cow as target
using (  select 3 as id, 'Third Merge' as name, '2021-11-29' as dt, '11' hh from s) source on target.id = source.id when matched then update set *
when not matched then insert * ;

merge into hudi_sample_table_cow as target
using (  select 2 as id, 'Second Update' as name, '2021-12-29' as dt, '10' hh from s) source on target.id = source.id when matched then update set *
when not matched then insert * ;

merge into hudi_sample_table_mor as target
using (  select 3 as id, 'Third Merge' as name, '2021-11-29' as dt, '11' hh from s) source on target.id = source.id when matched then update set *
when not matched then insert * ;

merge into hudi_sample_table_mor as target
using (  select 2 as id, 'Second Update' as name, '2021-12-29' as dt, '10' hh from s) source on target.id = source.id when matched then update set *
when not matched then insert * ;

select * from  hudi_sample_table_cow ;
select * from  hudi_sample_table_mor ;


```

## Update Data
```
-- Apache Hudi
update  hudi_sample_table_cow  set  name = name + 'Updated' where id=1 ;
update  hudi_sample_table_mor  set  name = name + 'Updated' where id=1 ;

select * from  hudi_sample_table_cow ;
select * from  hudi_sample_table_mor ;


 
```

## Delete data
```
-- Apache Hudi
delete from  hudi_sample_table_cow  where id=1 ;
delete from  hudi_sample_table_mor  where id=1 ;

select * from  hudi_sample_table_cow ;
select * from  hudi_sample_table_mor ;



```