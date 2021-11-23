import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import spark.implicits._
import spark.sql

val spark = (SparkSession.builder
    .master("local[*]")
    .enableHiveSupport()
    .appName("Load temperature data")
    .getOrCreate())

// let's load the data as-is, only specifing the column names
val base_schema = StructType(Array(
  StructField("year", StringType, false),
  StructField("month", StringType, false),
  StructField("day", StringType, false),
  StructField("morning_temp", StringType, false),
  StructField("noon_temp", StringType, false),
  StructField("evening_temp", StringType, false),
))

val schema_1859_to_1960 = (base_schema
  .add(StructField("tmin", StringType, true))
  .add(StructField("tmax", StringType, true)))

val schema_after_1961 = (schema_1859_to_1960
  .add(StructField("estimated_diurnal_mean", StringType, true)))


val df_1756_1858 = spark.createDataFrame(
  spark.sparkContext.textFile("/data/raw/bolin/temperature/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), base_schema)

val df_1859_1960 = spark.createDataFrame(
  spark.sparkContext.textFile("/data/raw/bolin/temperature/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), schema_1859_to_1960
)

val df_after_1961 = spark.createDataFrame(
  spark.sparkContext.textFile("/data/raw/bolin/temperature/stockholm_daily_temp_obs*ntm.txt")
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), schema_after_1961
)

// not the fastest way to union the dataframes, but good enough for our purpose 
val df_merged = (df_after_1961
  .unionByName(df_1859_1960, allowMissingColumns = true)
  .unionByName(df_1756_1858, allowMissingColumns = true)
  )

// now that we have our data with common schema, let's cast the types
// I could merge year/month/day into single DateType, but I decided to keep 
// it the same way it appears in the raw data 
val df_final = (df_merged
  .withColumn("year", col("year").cast(IntegerType))
  .withColumn("month", col("month").cast(IntegerType))
  .withColumn("day", col("day").cast(IntegerType))
  .withColumn("morning_temp", col("morning_temp").cast(DoubleType))
  .withColumn("noon_temp", col("noon_temp").cast(DoubleType))
  .withColumn("evening_temp", col("evening_temp").cast(DoubleType))
  .withColumn("tmin", col("tmin").cast(DoubleType))
  .withColumn("tmax", col("tmin").cast(DoubleType))
  .withColumn("estimated_diurnal_mean", col("estimated_diurnal_mean").cast(DoubleType))
  )

// write the results to Hive
df_final.write.mode(SaveMode.Append).insertInto("bolin.temperature")