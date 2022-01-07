
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieBootstrapConfig
import org.apache.hudi.config.HoodieBootstrapConfig._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import java.util.concurrent.TimeUnit
import org.apache.hudi.keygen._
object Bootstrap {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
        .builder
        .appName("HudiBootstrap")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.default.parallelism", 9)
        .config("spark.sql.shuffle.partitions", 9)
        .enableHiveSupport()
        .getOrCreate()

        // Spark Shell -- hardcode these parameters
        val hudiTableName="parquet_bootstrapped_trade_info_cow"
        // Location of the existing dataset to be used with Hudi
        var sourceDataPath = "s3://akshaya-firehose-test/parquet/parquet_trade_info/"
        // Location for the Hudi table where it generates the metadata
        var hudiTablePath = s"s3://akshaya-firehose-test/hudi/$hudiTableName/"
        var tableType = "COW"
        var hudiDatabaseName="hudi"
       
        // Spark Shell ---end 

        if (!Option(args).isEmpty) {
            hudiTableName = args(0) 
            sourceDataPath = args(1) 
            hudiTablePath = args(2) /
            tableType = args(3) 
            hudiDatabaseName=args(4) 
        }

    
        var dsWriteOptionType = DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
        if (tableType.equals("COW")) {
            dsWriteOptionType = DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
        } else if (tableType.equals("MOR")) {
            dsWriteOptionType = DataSourceWriteOptions.MOR_STORAGE_TYPE_OPT_VAL
        }

        // Spark Shell ---start 
        import spark.implicits._
        

        // Create an empty DataFrame and write using Hudi Spark DataSource
       
        val bootstrapResult=(spark.emptyDataFrame.write
        .format("hudi")
        .option(TABLE_TYPE_OPT_KEY, COW_STORAGE_TYPE_OPT_VAL)
        .option(TABLE_NAME, "bootstrapped_hudi_table_cow")
        .option(RECORDKEY_FIELD_OPT_KEY, "tradeid")
        .option(KEYGENERATOR_CLASS_OPT_KEY, classOf[ComplexKeyGenerator].getName) 
        .option(OPERATION_OPT_KEY,BOOTSTRAP_OPERATION_OPT_VAL)    
        .option(BOOTSTRAP_BASE_PATH_PROP, "s3://my_bucket/parquet/source/" ) 
        .option(BOOTSTRAP_KEYGEN_CLASS, classOf[ComplexKeyGenerator].getName)
        .option(HIVE_STYLE_PARTITIONING_OPT_KEY, "true")
        .option(HIVE_SYNC_ENABLED_OPT_KEY, "true")
        .option(HIVE_TABLE_OPT_KEY, "bootstrapped_hudi_table_cow")
        .option(HIVE_PARTITION_FIELDS_OPT_KEY, "day,hour")
        .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)
        .option(HIVE_DATABASE_OPT_KEY, "hudi")
        .mode(SaveMode.Overwrite)
        .save("s3://my_bucket/hudi/bootstrapped_hudi_table_cow/"))
      
    }
}
