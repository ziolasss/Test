import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import spark.implicits._
import spark.sql

val spark = (SparkSession.builder
    .master("local[*]")
    .enableHiveSupport()
    .appName("Word count")
    .getOrCreate())

val schema_1756_1858 = StructType(Array(
  StructField("year", StringType, false),
  StructField("month", StringType, false),
  StructField("day", StringType, false),
  StructField("bar_obs_morning", StringType, false),
  StructField("temp_morning", StringType, false),
  StructField("bar_obs_noon", StringType, false),
  StructField("temp_noon", StringType, false),
  StructField("bar_obs_evening", StringType, false),
  StructField("temp_evening", StringType, false),
))

val schema_1859_1861 = StructType(Array(
  StructField("year", StringType, false),
  StructField("month", StringType, false),
  StructField("day", StringType, false),
  StructField("bar_obs_morning", StringType, false),
  StructField("temp_morning", StringType, false),
  StructField("pressure_morning", StringType, false),
  StructField("bar_obs_noon", StringType, false),
  StructField("temp_noon", StringType, false),
  StructField("pressure_noon", StringType, false),
  StructField("bar_obs_evening", StringType, false),
  StructField("temp_evening", StringType, false),
  StructField("pressure_evening", StringType, false)))

val schema_1862_to_2017 = StructType(Array(
  StructField("year", StringType, false),
  StructField("month", StringType, false),
  StructField("day", StringType, false),
  StructField("pressure_morning", StringType, false),
  StructField("pressure_noon", StringType, false),
  StructField("pressure_evening", StringType, false)))


val df_1756_1858 = spark.createDataFrame(
  spark.sparkContext.textFile("/data/raw/bolin/pressure/stockholm_barometer_1756_1858.txt")
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), schema_1756_1858)

val df_1859_1861 = spark.createDataFrame(
  spark.sparkContext.textFile("/data/raw/bolin/pressure/stockholm_barometer_1859_1861.txt")
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), schema_1859_1861
)

// even though the schema for 1938+ is the same, the units used for pressure are different
// I could either read it separately, or then use a case/when to update the fields from 1862-1937
val df_1862_1937 = spark.createDataFrame(
  spark.sparkContext.textFile("/data/raw/bolin/pressure/stockholm_barometer_1862_1937.txt")
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), schema_1862_to_2017
)

val df_after_1938 = spark.createDataFrame(
  spark.sparkContext.textFile("""
    |/data/raw/bolin/pressure/stockholm_barometer_1938_1960.txt,
    |/data/raw/bolin/pressure/stockholm_barometer_1961_2012.txt,
    |/data/raw/bolin/pressure/stockholm_barometer_2013_2017.txt,
    |/data/raw/bolin/pressure/stockholmA_barometer_2013_2017.txt""".stripMargin.replaceAll("\n", "")
    )
    .map(_.trim.replaceAll(" +", " "))
    .map(line => Row.fromSeq(line.split(" "))), schema_1862_to_2017
)

// Now, let's unify that data.
// we will try to get it all to match the schema from 1862 - 2017

// This one is quite straitghforward, it only needs to be 
// converted to hPa from mm Hg (I goggled that it needs to be multiplied x 1.33)
val df_1862_1937_unified = (df_1862_1937
  .withColumn("pressure_morning", col("pressure_morning")*1.33)
  .withColumn("pressure_noon", col("pressure_noon")*1.33)
  .withColumn("pressure_evening", col("pressure_evening")*1.33)
)

// ok now here I had to make a decision whether to keep 
// the temperature data from 1756 - 1861 or not - since it's not 
// present in the data <1862,2017>
// I decided to drop it - we already have temperature data from other source


// I noticed we have both barometer readings and normalized air pressure 
// I decided to go with the air pressure.
// Air pressure needed conversion, from swedish inches*0.1 thus alternated formula
val df_1859_1861_unified = (df_1859_1861
  .drop("temp_morning","temp_noon","temp_evening","bar_obs_morning","bar_obs_noon","bar_obs_evening")  
  .withColumn("pressure_morning", col("pressure_morning")*1.33*2.969)
  .withColumn("pressure_noon", col("pressure_noon")*1.33*2.969)
  .withColumn("pressure_evening", col("pressure_evening")*1.33*2.969)
)

// And the last one
// Air pressure needed conversion, from swedish inches
val df_1756_1858_unified = (df_1756_1858
  .withColumn("pressure_morning", col("bar_obs_morning")*1.33*29.69)
  .withColumn("pressure_noon", col("bar_obs_noon")*1.33*29.69)
  .withColumn("pressure_evening", col("bar_obs_evening")*1.33*29.69)
  .drop("temp_morning","temp_noon","temp_evening","bar_obs_morning","bar_obs_noon","bar_obs_evening")  
)

val df_merged = (df_1756_1858_unified
  .union(df_1859_1861_unified)
  .union(df_1862_1937_unified)
  .union(df_after_1938)
)

// this could be casted to common DateType but I decided 
// to keep it as it appeared in raw data
val df_final = (df_merged
  .withColumn("year", col("year").cast(IntegerType))
  .withColumn("month", col("month").cast(IntegerType))
  .withColumn("day", col("day").cast(IntegerType))
  .withColumn("pressure_morning", col("pressure_morning").cast(DoubleType))
  .withColumn("pressure_noon", col("pressure_noon").cast(DoubleType))
  .withColumn("pressure_evening", col("pressure_evening").cast(DoubleType))
 )

df_final.write.mode(SaveMode.Append).insertInto("bolin.pressure")