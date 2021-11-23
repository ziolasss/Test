-- I could have created the tables in Spark job, but I believe DDLs should 
-- be decoupled from ETL jobs
CREATE DATABASE IF NOT EXISTS bolin;

-- normally I would have partitioned the tables
-- but the data size doesn't call for that
-- if required, we could partition it by year, month 
-- or  other column frequently used for filtering

-- I decided to use Parquet as it's dedicated for analytical purposes
-- and easy to migrate if we ever need that
DROP TABLE IF EXISTS bolin.temperature;
CREATE TABLE bolin.temperature (
    year INT, 
    month INT,
    day INT,
    morning_temp DOUBLE,
    noon_temp DOUBLE,
    evening DOUBLE,
    tmin_temp DOUBLE,
    tmax DOUBLE,
    estimated_diurnal_mean DOUBLE
)
STORED AS PARQUET;

DROP TABLE IF EXISTS bolin.pressure;
CREATE TABLE bolin.pressure (
    year INT, 
    month INT,
    day INT,
    pressure_morning DOUBLE,
    pressure_noon DOUBLE,
    pressure_evening DOUBLE
)
STORED AS PARQUET;

DROP TABLE IF EXISTS bolin.observation_hours;
CREATE TABLE bolin.obs_hours (
    year INT, 
    month INT,
    day INT,
    morning_hour DOUBLE,
    noon_hour DOUBLE,
    evening_hour DOUBLE
)
STORED AS PARQUET;
