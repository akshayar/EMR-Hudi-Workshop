package kinesis.hudi.latefile
// Spark Shell ---start 
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisInitialPositions
import java.util.Date
import java.text.SimpleDateFormat
// Spark Shell ---end 
/**
  The file consumes messages pushed to Kinesis. The message content look like 
  {
   "tradeId":"211124204181756",
   "symbol":"GOOGL",
   "quantity":"39",
   "price":"39",
   "timestamp":1637766663,
   "description":"Traded on Wed Nov 24 20:41:03 IST 2021",
   "traderName":"GOOGL trader",
   "traderFirm":"GOOGL firm"
 }
 The parameters expected are -
  s3_bucket  Ex. <akshaya-firehose-test>
  streamName Ex. <hudi-stream-ingest>
  region Ex. <us-west-2>
  tableType Ex. <COW/MOR>
  hudiTableNamePrefix Ex. <hudi_trade_info>

*/
object SparkKinesisConsumerHudiProcessorNoSchema {

  def epochToDate(epochMillis: String): Date = {
    new Date(epochMillis.toLong)
  } 

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
          .builder
          .appName("SparkHudi")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.default.parallelism", 9)
          .config("spark.sql.shuffle.partitions", 9)
          .enableHiveSupport()
          .getOrCreate()
    
    // Spark Shell ---start 
    import spark.implicits._
    // Spark Shell -- hardcode these parameters
    var s3_bucket="akshaya-firehose-test"//args(0)//
    var streamName="data-stream-ingest"//args(1)//
    var region="ap-south-1"//args(2)//
    var tableType="COW"//args(3)//
    var hudiTableNamePrefix = "hudi_trade_info_ns"//args(4)//
    var iteratorPos="TRIM_HORIZON"
    if(args.length > 0){
      s3_bucket=args(0)//"akshaya-firehose-test"//
      streamName=args(1)//"data-stream-ingest"//
      region=args(2)//"ap-south-1"//
      tableType=args(3)//"COW"//
      hudiTableNamePrefix = args(4)//"hudi_trade_info_ns"//
    }
    
    if(args.length > 5){
      iteratorPos=args(5)
    }
    var hudiTableName=hudiTableNamePrefix+"_cow"
    var dsWriteOptionType=DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
    
    if(tableType.equals("COW")){
       hudiTableName = hudiTableNamePrefix+"_cow"
       dsWriteOptionType=DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
    }else if (tableType.equals("MOR")){
      hudiTableName = hudiTableNamePrefix+"_mor"
      dsWriteOptionType=DataSourceWriteOptions.MOR_STORAGE_TYPE_OPT_VAL
    }

    val hudiTableRecordKey = "record_key"
    val hudiTablePartitionKey = "partition_key"
    val hudiTablePrecombineKey = "trade_datetime"
    val hudiTablePath = s"s3://$s3_bucket/hudi/" + hudiTableName
    val hudiHiveTablePartitionKey = "day,hour"
    val checkpoint_path=s"s3://$s3_bucket/kinesis-stream-data-checkpoint/"+hudiTableName+"/"
    val endpointUrl=s"https://kinesis.$region.amazonaws.com"

    println("hudiTableRecordKey:"+hudiTableRecordKey)
    println("hudiTablePartitionKey:"+hudiTablePartitionKey)
    println("hudiTablePrecombineKey:"+hudiTablePrecombineKey)
    println("hudiTablePath:"+hudiTablePath)
    println("hudiHiveTablePartitionKey:"+hudiHiveTablePartitionKey)
    println("checkpoint_path:"+checkpoint_path)
    println("endpointUrl:"+endpointUrl)
    println("streamName:"+streamName)

    //Read Schema of table
    val sql=s"select * from $hudiTableName where 1 != 1"
    println("sql:"+sql)
    val currentDF=spark.sql(sql)
    var dropDF=currentDF.drop(currentDF.columns.filter(_.startsWith("_hoodie")): _*)
    val schemeExisting=dropDF.schema
  
    val streamingInputDF = (spark
                    .readStream .format("kinesis") 
                    .option("streamName", streamName) 
                    .option("startingposition", iteratorPos)
                    .option("endpointUrl", endpointUrl)
                    .load())

    val jsonDS=(streamingInputDF.selectExpr("CAST(data AS STRING)").as[(String)])
                  
    jsonDS.printSchema()
    val query = (jsonDS.writeStream.foreachBatch{ (batchDS: Dataset[String], _: Long) => {
                 batchDS.show()
              if(!batchDS.rdd.isEmpty){
                  batchDS.show()
                  //Infer Schema from incomeing record
                  var schemaNew: StructType = spark.read.json(batchDS.select("value").as[String]).schema
                   println("New Schema"+schemaNew)
                  //Merge new schema with exising ensuring order of columns
                  var schemaNewMap=schemaNew.fields.map(f => (f.name.toLowerCase -> f)).toMap
                  var schemaExistingMap=schemeExisting.fields.map(f => (f.name.toLowerCase -> f)).toMap  
                  var additionalNames=schemaNewMap.keySet.diff(schemaExistingMap.keySet)
                  var listName=List() ++ schemaExistingMap.keys ++ additionalNames
                  var useSchemaType= new StructType
                  for (name <- listName ){ 
                      if(schemaNewMap.contains(name)){
                        useSchemaType=useSchemaType.add(schemaNewMap(name))

                      }else{
                        useSchemaType=useSchemaType.add(schemaExistingMap(name))
                      }
                  }
                 println("Old Schema"+schemeExisting)
                 println("New Schema"+useSchemaType)              
                  //Read data from the schema infered above
                  var parDF=(batchDS.withColumn("jsonData",from_json(col("value"),useSchemaType))
                              .select(col("jsonData.*")))
                  parDF=parDF.select(parDF.columns.map(x => col(x).as(x.toLowerCase)): _*)
                  parDF.show()  
                  parDF=parDF.filter(parDF.col("tradeId").isNotNull) 
                                
                  parDF = parDF.withColumn(hudiTableRecordKey, concat(col("tradeId"), lit("#"), col("timestamp")))
                 
                  parDF = parDF.withColumn("trade_datetime", from_unixtime(parDF.col("timestamp")))
                  parDF = parDF.withColumn("day",dayofmonth($"trade_datetime").cast(StringType)).withColumn("hour",hour($"trade_datetime").cast(StringType))
                  parDF = parDF.withColumn(hudiTablePartitionKey,concat(lit("day="),$"day",lit("/hour="),$"hour"))
                  parDF.show()  

                  if(!parDF.rdd.isEmpty){
                    println("Saving data ")
                    parDF.select("day","hour",hudiTableRecordKey,"quantity").show()
                    val result=parDF.write.format("org.apache.hudi")
                          .option("hoodie.datasource.write.table.type", dsWriteOptionType)
                          .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, hudiTableRecordKey)
                          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, hudiTablePartitionKey)
                          .option(HoodieWriteConfig.TABLE_NAME, hudiTableName)
                          .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
                          .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, hudiTablePrecombineKey)
                          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
                          .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY,hudiTableName)
                          .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, hudiHiveTablePartitionKey)
                          .option("hoodie.datasource.hive_sync.assume_date_partitioning", "false")
                          .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)
                          .mode("append")
                          .save(hudiTablePath);
                    println("Saved data "+hudiTablePath+":"+result)
                  }else{
                    println("Empty DF, nothing to write")
                  }

            }
    }}.option("checkpointLocation", checkpoint_path).start())
  // Spark Shell ---end 
    query.awaitTermination()
    //query.stop

  }

}
