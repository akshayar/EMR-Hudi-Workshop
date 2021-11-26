package kinesis.hudi.latefile

// Spark Shell ---start

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
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
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import java.util.concurrent.TimeUnit
// Spark Shell ---end 
/**
 * The file consumes messages pushed to Kinesis. The message content look like
 * {
 * "tradeId":"211124204181756",
 * "symbol":"GOOGL",
 * "quantity":"39",
 * "price":"39",
 * "timestamp":1637766663,
 * "description":"Traded on Wed Nov 24 20:41:03 IST 2021",
 * "traderName":"GOOGL trader",
 * "traderFirm":"GOOGL firm"
 * }
 * The parameters expected are -
 * s3_bucket  Ex. <akshaya-firehose-test>
 * streamName Ex. <hudi-stream-ingest>
 * region Ex. <us-west-2>
 * tableType Ex. <COW/MOR>
 * hudiTableNamePrefix Ex. <hudi_trade_info>
 *
 */
object SparkKinesisConsumerHudiProcessor {

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
    var s3_bucket = "akshaya-firehose-test"
    var streamName = "data-stream-ingest"
    var region = "ap-south-1"
    var tableType = "COW"
    var hudiTableNamePrefix = "hudi_trade_info"
    var hudiTableName = hudiTableNamePrefix + "_cow"
    // Spark Shell ---end 

    if (!Option(args).isEmpty) {
      s3_bucket = args(0) //"akshaya-firehose-test"//
      streamName = args(1) //"hudi-stream-ingest"//
      region = args(2) //"us-west-2"//
      tableType = args(3) //"COW"//
      hudiTableNamePrefix = args(4) //"hudi_trade_info"//
      hudiTableName = hudiTableNamePrefix + "_cow"
    }

    // Spark Shell ---start 
    var hudiDatabaseName = "hudi"
    var dsWriteOptionType = DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
    if (tableType.equals("COW")) {
      hudiTableName = hudiTableNamePrefix + "_cow"
      dsWriteOptionType = DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
    } else if (tableType.equals("MOR")) {
      hudiTableName = hudiTableNamePrefix + "_mor"
      dsWriteOptionType = DataSourceWriteOptions.MOR_STORAGE_TYPE_OPT_VAL
    }

    val hudiTableRecordKey = "record_key"
    val hudiTablePartitionKey = "partition_key"
    val hudiTablePrecombineKey = "trade_datetime"
    val hudiTablePath = s"s3://$s3_bucket/hudi/" + hudiTableName
    val hudiHiveTablePartitionKey = "day,hour"
    val checkpoint_path = s"s3://$s3_bucket/kinesis-stream-data-checkpoint/" + hudiTableName + "/"
    val endpointUrl = s"https://kinesis.$region.amazonaws.com"

    println("hudiTableRecordKey:" + hudiTableRecordKey)
    println("hudiTablePartitionKey:" + hudiTablePartitionKey)
    println("hudiTablePrecombineKey:" + hudiTablePrecombineKey)
    println("hudiTablePath:" + hudiTablePath)
    println("hudiHiveTablePartitionKey:" + hudiHiveTablePartitionKey)
    println("checkpoint_path:" + checkpoint_path)
    println("endpointUrl:" + endpointUrl)

    val streamingInputDF = (spark
      .readStream.format("kinesis")
      .option("streamName", streamName)
      .option("startingposition", "TRIM_HORIZON")
      .option("endpointUrl", endpointUrl)
      .load())

    val decimalType = DataTypes.createDecimalType(38, 10)
    val dataSchema = StructType(Array(
      StructField("tradeId", StringType, true),
      StructField("symbol", StringType, true),
      StructField("quantity", StringType, true),
      StructField("price", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("description", StringType, true),
      StructField("traderName", StringType, true),
      StructField("traderFirm", StringType, true)
    ))


    val jsonDF = (streamingInputDF.selectExpr("CAST(data AS STRING)").as[(String)]
      .withColumn("jsonData", from_json(col("data"), dataSchema))
      .select(col("jsonData.*")))


    jsonDF.printSchema()
    var parDF = jsonDF
    parDF = parDF.select(parDF.columns.map(x => col(x).as(x.toLowerCase)): _*)
    parDF = parDF.filter(parDF.col("tradeId").isNotNull)
    parDF.printSchema()
    parDF = parDF.withColumn(hudiTableRecordKey, concat(col("tradeId"), lit("#"), col("timestamp")))

    parDF = parDF.withColumn("trade_datetime", from_unixtime(parDF.col("timestamp")))
    parDF = parDF.withColumn("day", dayofmonth($"trade_datetime").cast(StringType)).withColumn("hour", hour($"trade_datetime").cast(StringType))
    parDF = parDF.withColumn(hudiTablePartitionKey, concat(lit("day="), $"day", lit("/hour="), $"hour"))
    parDF.printSchema()

    var query = (parDF.writeStream.format("org.apache.hudi")
      .option("hoodie.datasource.write.table.type", dsWriteOptionType)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, hudiTableRecordKey)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, hudiTablePartitionKey)
      .option(HoodieWriteConfig.TABLE_NAME, hudiTableName)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, hudiTablePrecombineKey)
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, hudiTableName)
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, hudiHiveTablePartitionKey)
      .option("hoodie.datasource.hive_sync.assume_date_partitioning", "false")
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, hudiDatabaseName)
      .option("checkpointLocation", checkpoint_path)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
      .start(hudiTablePath));

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    // Spark Shell ---end
    query.awaitTermination()

  }

}
