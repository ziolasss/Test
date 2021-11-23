import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import spark.implicits._
import spark.sql

val spark = (SparkSession.builder
    .master("local[*]")
    .enableHiveSupport()
    .appName("Load observation hours data ")
    .getOrCreate())

val schema = StructType(Array(
  StructField("year", StringType, false),
  StructField("month", StringType, false),
  StructField("day", StringType, false),
  StructField("morning_hour", StringType, false),
  StructField("noon_hour", StringType, false),
  StructField("evening_hour", StringType, false),
))

val df_raw_obs_hours = spark.createDataFrame(
  spark.sparkContext.textFile("/data/raw/bolin/obs_hours/*.txt")
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), schema)

// this could be casted to common DateType but I decided 
// to keep it as it appeared in raw data
val df_final = (df_raw_obs_hours
  .withColumn("year", col("year").cast(IntegerType))
  .withColumn("month", col("month").cast(IntegerType))
  .withColumn("day", col("day").cast(IntegerType))
  .withColumn("morning_hour", col("morning_hour").cast(DoubleType))
  .withColumn("noon_hour", col("noon_hour").cast(DoubleType))
  .withColumn("evening_hour", col("evening_hour").cast(DoubleType))
)

df_final.write.mode(SaveMode.Append).insertInto("bolin.observation_hours")