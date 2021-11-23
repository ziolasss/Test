#!/usr/bin/env bash

set -euo pipefail

# some variables to start with 
TEMPERATURE_URL="https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/"
PRESSURE_URL="https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/air_pressure/raw/"
OBS_HOURS_URL="https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/observation_hours/"
TMP_DIR="/tmp/bolin_raw_data"
HDFS_RAW="/data/raw"

# First, let's set up HDFS dirs and Hive db
hadoop fs -mkdir -p /data/raw/bolin
hive -f sql/init_hive_tables.sql

# Then get the actual raw data
mkdir -p ${TMP_DIR}
wget -r -nH --cut-dirs=7 -e robots=off --no-parent --accept txt --reject="README*" -P "${TMP_DIR}/temperature" ${TEMPERATURE_URL}
wget -r -nH --cut-dirs=6 -e robots=off --no-parent --accept txt --reject="README*" -P "${TMP_DIR}/pressure" ${PRESSURE_URL}
wget -r -nH --cut-dirs=6 -e robots=off --no-parent --accept txt --reject="README*" -P "${TMP_DIR}/obs_hours" ${OBS_HOURS_URL}

# Let's then move it to the HDFS
hadoop fs -put "${TMP_DIR}/temperature/" "${HDFS_RAW}/bolin"
hadoop fs -put "${TMP_DIR}/pressure/" "${HDFS_RAW}/bolin"
hadoop fs -put "${TMP_DIR}/obs_hours/" "${HDFS_RAW}/bolin"

# Now that the files are in HDFS let's run the actual Spark scripts
# Disclaimer - I know this should be run with spark-submit job.jar 
# this hacky approach is just for the sake of this exercise
spark-shell < scala/load_observation_hours.scala
spark-shell < scala/load_pressure_data.scala
spark-shell < scala/load_temperature_data.scala
