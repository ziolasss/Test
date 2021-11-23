## Intro
Hi All!

In this repo you will find my solution to given task
Following the instructions, I tried to deliver the solution using HDFS and Scala.
Since I was told to use HDFS, I figured Hive might be a good place to share the data with data scientists.

Please start your evaluation with `main.sh` script - it contains all of the commands and the order in which they should be run.
## Assumptions
- Script is run on the Master / Edge node of the Hadoop cluster with access to:
    - Spark CLI
    - HDFS CLI
    - Hive CLI
- User was already authorized with Kerberos or the cluster is not kerberized
- All other assumptions (why I've decided to keep column x over y, and so) are documented inside a code

## Test Setup
These scripts were tested on Dataproc 2.0.24-debian10 (Hadoop 3.2.2, Hive 3.1.2, Spark 3.1.2).

## Sidenotes
Sorry if the code is not very Scala'stic but there's a limit ib how much one can learn in an evening.