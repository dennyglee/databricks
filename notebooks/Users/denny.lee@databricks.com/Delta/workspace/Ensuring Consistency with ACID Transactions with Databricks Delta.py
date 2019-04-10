# Databricks notebook source
# MAGIC %md # Ensuring Consistency with ACID Transactions with Databricks Delta
# MAGIC 
# MAGIC This notebook showcases ACID transactions by running INSERT/UPDATE statements (in this notebook) while you can view the same table using the *View* notebook in a consistent manner.  
# MAGIC 
# MAGIC ### How to Run These Notebooks
# MAGIC 1. Run this notebook up until and including Cell 7
# MAGIC 1. View the initial set of data
# MAGIC 
# MAGIC **Once this is done, you can also watch this using structured streaming:**
# MAGIC 1. Run cells 8 to (and including) 11
# MAGIC 1. Can watch the data streamed in
# MAGIC 1. You can also view the table history
# MAGIC 1. Then after that, you can *time travel*
# MAGIC 
# MAGIC ### For more information:
# MAGIC * Databricks Delta Guide ([Azure](https://docs.azuredatabricks.net/delta/index.html) | [AWS](https://docs.databricks.com/delta/index.html#delta-guide))
# MAGIC * Upserts (MERGE INTO) ([Azure](https://docs.azuredatabricks.net/delta/delta-batch.html#upserts-merge-into) | [AWS](https://docs.databricks.com/delta/delta-batch.html#upserts-merge-into))
# MAGIC * Merge Into (Databricks Delta) ([Azure](https://docs.azuredatabricks.net/spark/latest/spark-sql/language-manual/merge-into.html#merge-into-delta) | [AWS](https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html#examples))

# COMMAND ----------

# MAGIC %md ## Source Data Preparation
# MAGIC 
# MAGIC Source Data: 
# MAGIC * [OpenFlights: Airport, airline and route data](http://openflights.org/data.html)
# MAGIC * [United States Department of Transportation: Bureau of Transportation Statistics (TranStats)](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time)
# MAGIC  * Note, the data used here was extracted from the US DOT:BTS between 1/1/2014 and 3/31/2014*
# MAGIC  
# MAGIC For more information, refer to [On-Time Flight Performance with GraphFrames for Apache Spark](https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html)

# COMMAND ----------

# Set File Paths
tripdelaysFilePath = "/databricks-datasets/flights/departuredelays.csv"

# Obtain departure Delays data
departureDelays = spark.read.options(header='true').csv(tripdelaysFilePath)
departureDelays.createOrReplaceTempView("departureDelays")
departureDelays.cache()

# COMMAND ----------

# MAGIC %md ## Databricks Delta Table Preparation
# MAGIC 
# MAGIC These next steps will allow us to create a Databricks Delta table based on the source data by
# MAGIC * Removing the folder `/tmp/departureDelays_delta` (and any of its files)
# MAGIC  * You can configure this to a different folder in the following cell
# MAGIC * Create the `departureDelays_delta` Databricks Delta Parquet files

# COMMAND ----------

# Configure temporary path
departureDelays_delta_path = '/tmp/departureDelays_delta'

# Remove files and folder
dbutils.fs.rm(departureDelays_delta_path, True)

# Create Databricks Delta Parquet files
departureDelays.write.format("delta").mode("overwrite").partitionBy("origin").save(departureDelays_delta_path)

# COMMAND ----------

# Create `departureDelays_delta` Databricks Delta table
spark.sql("DROP TABLE IF EXISTS departureDelays_delta")
spark.sql("CREATE TABLE departureDelays_delta USING DELTA LOCATION '{}'".format(departureDelays_delta_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM departureDelays_delta WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '%28' LIMIT 20

# COMMAND ----------

# MAGIC %md ## Inserts into Databricks Delta Table
# MAGIC 
# MAGIC To showcase ACID transactions, we will run two concurrent queries via while loops within our Databricks notebooks.
# MAGIC * This notebook will run an `UPDATE` every 10s against our `departureDelays_delta` table
# MAGIC * It will change a specific set of rows (`origin='SEA', destination='SFO', date like '%28'`) every 10s 
# MAGIC * It will insert new rows every 10s
# MAGIC * In the separate notebook, you can then view output of those queries in a consistent fashion

# COMMAND ----------

# Read the insertion of data
departureDelays_readStream = spark.readStream.format("delta").load("/tmp/departureDelays_delta")
departureDelays_readStream.createOrReplaceTempView("departureDelays_readStream")
display(spark.sql("SELECT * FROM departureDelays_readStream WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '%28'"))

# COMMAND ----------

display(spark.sql("SELECT COUNT(1) AS NumRows, MIN(CAST(delay AS double)) AS delay_min, MAX(CAST(delay AS double)) AS delay_max FROM departureDelays_readStream WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '%28'"))

# COMMAND ----------

# MAGIC %md Wait until the two streams are up and running before executing the code below

# COMMAND ----------

import time
i = 1
while i <= 10:
  # Execute Insert statement
  insert_sql = "INSERT INTO departureDelays_delta VALUES ('040" + str(i) + "1028'," + str(200+(20*i)) + ",590,'SEA','SFO')"
  spark.sql(insert_sql)
  print('departureDelays_delta: inserted new row with date of [040%s1028]' % i)
    
  # Loop through
  i = i + 1
  time.sleep(10)

# COMMAND ----------

# MAGIC %md ## View Table History 
# MAGIC 
# MAGIC You can view the Databricks Delta table history by viewing the table via **Data** in the left panel.  Below is an example screenshot; note the rows of `UPDATE` and `WRITE` (i.e. `INSERT`) recorded in the Delta transaction log for this table.
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/departureDelays_delta_history.png)
# MAGIC 
# MAGIC 
# MAGIC --- 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC **NOTE:** If you are using the Demo shard, you can view this table by opening the [departureDelays_delta table](https://demo.cloud.databricks.com/#table/default/departuredelays_delta)

# COMMAND ----------

# MAGIC %md ## Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### Time Travel via Timestamp
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT * FROM departuredelays_delta TIMESTAMP AS OF "2019-04-09 00:00:08.000"
# MAGIC ) a
# MAGIC WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '%28'
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT * FROM departuredelays_delta TIMESTAMP AS OF "2019-04-09T00:00:37.000+0000"
# MAGIC ) a
# MAGIC WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '%28'
# MAGIC ORDER by date

# COMMAND ----------

# MAGIC %md ### Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT * FROM departuredelays_delta VERSION AS OF 0
# MAGIC ) a
# MAGIC WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '%28'
# MAGIC ORDER by date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT * FROM departuredelays_delta VERSION AS OF 10
# MAGIC ) a
# MAGIC WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '%28'
# MAGIC ORDER by date

# COMMAND ----------

