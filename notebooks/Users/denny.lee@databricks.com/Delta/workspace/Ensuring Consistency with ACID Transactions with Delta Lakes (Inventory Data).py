# Databricks notebook source
# MAGIC %md # Ensuring Consistency with ACID Transactions with Delta Lakes
# MAGIC 
# MAGIC This notebook showcases ACID transactions by running INSERT/UPDATE statements (in this notebook) while you can view the same table using the *View* notebook in a consistent manner.  
# MAGIC 
# MAGIC 
# MAGIC ### [TODO]
# MAGIC * Update to include new Delta Lakes logo
# MAGIC * Point to new Delta Lakes documentation (per below)
# MAGIC 
# MAGIC 
# MAGIC ### For more information:
# MAGIC * Databricks Delta Guide ([Azure](https://docs.azuredatabricks.net/delta/index.html) | [AWS](https://docs.databricks.com/delta/index.html#delta-guide))
# MAGIC * Upserts (MERGE INTO) ([Azure](https://docs.azuredatabricks.net/delta/delta-batch.html#upserts-merge-into) | [AWS](https://docs.databricks.com/delta/delta-batch.html#upserts-merge-into))
# MAGIC * Merge Into (Databricks Delta) ([Azure](https://docs.azuredatabricks.net/spark/latest/spark-sql/language-manual/merge-into.html#merge-into-delta) | [AWS](https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html#examples))

# COMMAND ----------

# MAGIC %md ## Source Data Prepartion
# MAGIC Loading Inventory Data in Parquet format for our scenario.

# COMMAND ----------

# MAGIC %run ./Delta_Inv_Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the current Inventory Parquet table
# MAGIC SELECT * FROM current_inventory_pq

# COMMAND ----------

# MAGIC %md ## In-place Import
# MAGIC With Delta Lakes, you can perform and in-place import of your Parquet data; i.e. Enables converting existing Parquet tables into Delta tables, without re-writing all the data. 
# MAGIC 
# MAGIC **TODO**: Need to re-write above so we're actually doing a in-place import

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Current example is creating a new table instead of in-place import so will need to change this code
# MAGIC CREATE TABLE current_inventory_delta
# MAGIC USING delta
# MAGIC AS SELECT * FROM current_inventory;
# MAGIC 
# MAGIC -- View Delta Lakes table
# MAGIC SELECT * FROM current_inventory_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read table information
# MAGIC DESCRIBE TABLE EXTENDED current_inventory_delta

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Delta Lakes Unifies Streaming & Batch
# MAGIC 
# MAGIC To showcase **ACID transactions**, we will run streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 10s against our `current_inventory_delta` table
# MAGIC * We will run two streaming queries concurrently against this data

# COMMAND ----------

# Read the insertion of data
inventory_readStream = spark.readStream.format("delta").load("/user/hive/warehouse/delta_webinar_inventory.db/current_inventory_delta")
inventory_readStream.createOrReplaceTempView("inventory_readStream")

# COMMAND ----------

# MAGIC %md ### Review Inventory for Specific SKU by Country
# MAGIC Let's review the `WHITE FRAME - G` SKU by Country 

# COMMAND ----------

# MAGIC %sql SELECT Country, SUM(Quantity) AS Quantity FROM inventory_readStream WHERE description = 'WHITE FRAME - G' GROUP BY Country ORDER BY SUM(Quantity) DESC

# COMMAND ----------

# Create our country_codes DataFrame
country_codes = spark.createDataFrame([('AUS', 'Australia'),('AUT', 'Austria'),('BHR', 'Bahrain'),('BEL', 'Belgium'),('CYP', 'Cyprus'),('DNK', 'Denmark'),('FIN', 'Finland'),('FRA', 'France'),('DEU', 'Germany'),('ISL', 'Iceland'),('ISR', 'Israel'),('ITA','Italy'),('JPN', 'Japan'),('LTU', 'Lithuania'),('NLD', 'Netherlands'),('NOR', 'Norway'),('POL', 'Poland'),('PRT', 'Portugal'),('ESP', 'Spain'),('SWE', 'Sweden'),('CHE', 'Switzerland'),('GBR', 'United Kingdom'),('USA', 'USA')], ["CountryCode", "Country"])
country_codes.createOrReplaceTempView("country_codes")

# COMMAND ----------

# MAGIC %md ### Map Inventory for Specific SKU by Country
# MAGIC Let's map the `WHITE FRAME - G` SKU by Country 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT f.*, c.CountryCode FROM inventory_readStream f LEFT OUTER JOIN country_codes c ON c.Country = f.Country WHERE description = 'WHITE FRAME - G'

# COMMAND ----------

# MAGIC %md **Wait** until the two streams are up and running before executing the code below

# COMMAND ----------

import time
i = 1
while i <= 10:
  # Execute Insert statement
  insert_sql = "INSERT INTO current_inventory_delta VALUES ('109SGf962LD', 'WHITE FRAME - G', " + str(50*i) + ", 178.31, 'Japan')"
  spark.sql(insert_sql)
  print('current_inventory_delta: inserted new row of data, loop: [%s]' % i)
    
  # Loop through
  i = i + 1
  time.sleep(10)

# COMMAND ----------

# MAGIC %md **Note** Japan increasing in both the bar charts and map!

# COMMAND ----------

# MAGIC %md ## View Table History 
# MAGIC 
# MAGIC You can view the Databricks Delta table history by viewing the table via **Data** in the left panel.  Below is an example screenshot; note the rows of `UPDATE` and `WRITE` (i.e. `INSERT`) recorded in the Delta transaction log for the table `departureDelays_delta`.
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/departureDelays_delta_history.png" width="800" />
# MAGIC 
# MAGIC 
# MAGIC --- 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC **NOTE:** If you are using the Demo shard, you can view the `current inventory delta` table by opening the [current inventory_delta table](https://demo.cloud.databricks.com/#table/delta_webinar_inventory/current_inventory_delta)

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
# MAGIC SELECT Country, SUM(Quantity) AS Quantity
# MAGIC   FROM (
# MAGIC     SELECT * FROM current_inventory_delta TIMESTAMP AS OF "2019-04-13T02:21:07.000+0000"
# MAGIC     ) 
# MAGIC  WHERE description = 'WHITE FRAME - G'
# MAGIC  GROUP BY Country
# MAGIC  ORDER BY SUM(Quantity) DESC
# MAGIC  LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Country, SUM(Quantity) AS Quantity
# MAGIC   FROM (
# MAGIC     SELECT * FROM current_inventory_delta TIMESTAMP AS OF "2019-04-13T02:22:48.000+0000"
# MAGIC     ) 
# MAGIC  WHERE description = 'WHITE FRAME - G'
# MAGIC  GROUP BY Country
# MAGIC  ORDER BY SUM(Quantity) DESC
# MAGIC  LIMIT 5

# COMMAND ----------

# MAGIC %md ### Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.CountryCode, SUM(f.Quantity) AS Quantity
# MAGIC   FROM (
# MAGIC     SELECT * FROM current_inventory_delta VERSION AS OF 0
# MAGIC     ) f
# MAGIC     LEFT OUTER JOIN country_codes c
# MAGIC       ON c.Country = f.Country 
# MAGIC  WHERE f.description = 'WHITE FRAME - G'
# MAGIC  GROUP BY c.CountryCode
# MAGIC  ORDER BY SUM(f.Quantity) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.CountryCode, SUM(f.Quantity) AS Quantity
# MAGIC   FROM (
# MAGIC     SELECT * FROM current_inventory_delta VERSION AS OF 10
# MAGIC     ) f
# MAGIC     LEFT OUTER JOIN country_codes c
# MAGIC       ON c.Country = f.Country 
# MAGIC  WHERE f.description = 'WHITE FRAME - G'
# MAGIC  GROUP BY c.CountryCode
# MAGIC  ORDER BY SUM(f.Quantity) DESC