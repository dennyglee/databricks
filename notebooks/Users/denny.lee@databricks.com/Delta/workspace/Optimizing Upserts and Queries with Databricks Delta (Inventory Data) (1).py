# Databricks notebook source
# MAGIC %md # Optimizing Upserts and Queries with Databricks Delta - Inventory Data
# MAGIC 
# MAGIC To perform an `UPSERT` operation, traditionally you would need to perform two distinct tasks - an `UPdate` and an `inSERT` operation.  To optimize the performance, Databricks Delta includes the ability to perform an "upsert" or `MERGE` operation, simplifying your code-base as well as your operations.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## In this notebook, we will show 2 use cases:
# MAGIC 1. DML support `MERGE/DELETE/UPDATE`
# MAGIC 3. Optimize query performance with `OPTIMIZE` and `ZORDER`
# MAGIC 
# MAGIC We will be using database `delta_webinar_inventory` with table `current_inventory`, created and modified from dataset `/databricks-datasets/online_retail/data-001/data.csv`.
# MAGIC 
# MAGIC 
# MAGIC ### Attribution:
# MAGIC * Author: Maggie Chu
# MAGIC * Contributor: Denny Lee
# MAGIC 
# MAGIC For more information:
# MAGIC * Databricks Delta Guide ([Azure](https://docs.azuredatabricks.net/delta/index.html) | [AWS](https://docs.databricks.com/delta/index.html#delta-guide))
# MAGIC * Attribution: This notebook is based on the *ZOrdering (multi-dimensional clustering)* Databricks documentation ([Azure](https://docs.azuredatabricks.net/delta/optimizations.html#delta-zorder) | [AWS](https://docs.databricks.com/delta/optimizations.html#zordering-multi-dimensional-clustering))

# COMMAND ----------

# DBTITLE 1,Run setup notebook
# MAGIC %run ./Delta_Inv_Setup

# COMMAND ----------

# MAGIC %md ## INSERT or UPDATE parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)

# COMMAND ----------

# DBTITLE 1,Select parquet table
# MAGIC %sql
# MAGIC select * from current_inventory_pq

# COMMAND ----------

# DBTITLE 1,1. Rows to insert into parquet
items = [('2187709', 'RICE COOKER', 30, 50.04, 'United Kingdom'), ('2187631', 'PORCELAIN BOWL - BLACK', 10, 8.33, 'United Kingdom')]
cols = ['StockCode', 'Description', 'Quantity', 'UnitPrice', 'Country']

insert_rows = spark.createDataFrame(items, cols)

display(insert_rows)

# COMMAND ----------

# DBTITLE 1,2. Rows to update in parquet
items = [('21877', 'HOME SWEET HOME MUG', 300, 26.04, 'United Kingdom'), ('21876', 'POTTERING MUG', 1000, 48.33, 'United Kingdom')]
cols = ['StockCode', 'Description', 'Quantity', 'UnitPrice', 'Country']

update_rows = spark.createDataFrame(items, cols)

display(update_rows)

# COMMAND ----------

# DBTITLE 1,3. Unchanged rows in parquet
unchanged_rows = spark.sql("select * from current_inventory where StockCode !='21877' and StockCode != '21876'")

display(unchanged_rows)

# COMMAND ----------

# DBTITLE 1,4. Create new temp table
temp_current_inventory = insert_rows \
    .union(update_rows) \
    .union(unchanged_rows)

display(temp_current_inventory)

# COMMAND ----------

# DBTITLE 1,5. Drop current_inventory_pq
# MAGIC %sql drop table if exists current_inventory_pq

# COMMAND ----------

# DBTITLE 1,6. Create updated current_inventory_pq with temp
temp_current_inventory.write.format("parquet").saveAsTable("current_inventory_pq")

display(table("current_inventory_pq"))

# COMMAND ----------

# DBTITLE 1,7. Drop temp table
# MAGIC %sql
# MAGIC drop table if exists temp_current_inventory

# COMMAND ----------

# MAGIC %md ## INSERT or UPDATE with Databricks Delta
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`
# MAGIC 
# MAGIC Note: Additional statements available:  `MERGE`, `UPDATE`, `DELETE`

# COMMAND ----------

# DBTITLE 1,Create delta table
# MAGIC %sql 
# MAGIC create table current_inventory_delta
# MAGIC using delta
# MAGIC as select * from current_inventory;
# MAGIC 
# MAGIC select * from current_inventory_delta

# COMMAND ----------

# DBTITLE 1,1. Select rows to insert OR update delta
# MAGIC %sql select * from merge_table

# COMMAND ----------

# DBTITLE 1,2. Use MERGE to insert and update delta
# MAGIC %sql
# MAGIC MERGE INTO current_inventory_delta as d
# MAGIC USING merge_table as m
# MAGIC on d.StockCode = m.StockCode and d.Country = m.Country
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Successfully merged into delta!
# MAGIC %sql select * from current_inventory_delta where StockCode in ('2187709', '2187631', '21877', '21876') and Country = 'United Kingdom'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # DELETE
# MAGIC 
# MAGIC You can easily `DELETE` rows from delta.

# COMMAND ----------

# DBTITLE 1,Create redItem
redItem = Row({'StockCode':'33REDff', 'Description':'ADDITIONAL RED ITEM', 'Quantity': 8, 'UnitPrice': 3.53, 'Country':'United Kingdom'})
redItemDF = spark.createDataFrame(redItem)
display(redItemDF)

# COMMAND ----------

# DBTITLE 1,Add redItem to parquet and delta
redItemDF.write.format("delta").mode("append").saveAsTable("current_inventory_delta")
redItemDF.write.format("parquet").mode("append").saveAsTable("current_inventory_pq")

display(spark.sql("""select * from current_inventory_delta where StockCode = '33REDff'"""))

# COMMAND ----------

# DBTITLE 1,Error: Cannot do this in parquet
# MAGIC %sql delete from current_inventory_pq where StockCode = '33REDff';

# COMMAND ----------

# DBTITLE 1,Successfully DELETE redItem from delta
# MAGIC %sql
# MAGIC delete from current_inventory_delta where StockCode = '33REDff';
# MAGIC select * from current_inventory_delta where StockCode = '33REDff';

# COMMAND ----------

# MAGIC %md # OPTIMIZE and ZORDER
# MAGIC 
# MAGIC Improve your query performance with `OPTIMIZE` and `ZORDER` using file compaction and a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC Legend:
# MAGIC * Gray dot = data point e.g., chessboard square coordinates
# MAGIC * Gray box = data file; in this example, we aim for files of 4 points each
# MAGIC * Yellow box = data file that’s read for the given query
# MAGIC * Green dot = data point that passes the query’s filter and answers the query
# MAGIC * Red dot = data point that’s read, but doesn’t satisfy the filter; “false positive”
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/07/Screen-Shot-2018-07-30-at-2.03.55-PM.png)
# MAGIC 
# MAGIC Reference: [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

# COMMAND ----------

# DBTITLE 1,Run query on delta
# MAGIC %sql select * from current_inventory_delta where Country = 'United Kingdom' and StockCode like '21%' and UnitPrice > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE current_inventory_delta
# MAGIC ZORDER by Country, StockCode;
# MAGIC 
# MAGIC select * from current_inventory_delta

# COMMAND ----------

# DBTITLE 1,Run the same query (even faster)
# MAGIC %sql select * from current_inventory_delta where Country = 'United Kingdom' and StockCode like '21%' and UnitPrice > 10

# COMMAND ----------

# MAGIC %md
# MAGIC The query over Delta runs much faster after `OPTIMIZE` and `ZORDER` is run. How much faster the query runs can depend on the configuration of the cluster you are running on, however should be **5-10X** faster compared to the standard table. 

# COMMAND ----------

# MAGIC %md ## Review the file system
# MAGIC 
# MAGIC Let's take a quick view of the file system and how Databricks Delta optimizes the underlying files
# MAGIC * Start by reviewing the files within one of the partitions of the `current_inventory_delta` Databricks Delta table
# MAGIC * Notice the `_delta_log` folder, this contains the Databricks Delta logs
# MAGIC * As you can see, there are a lot of small files and a few large ones
# MAGIC   * The small files are the original table files created prior to the `OPTIMIZE` command
# MAGIC   * The large file is the result of the `OPTIMIZE` command
# MAGIC * Both sets of files exist to ensure transactional consistency
# MAGIC 
# MAGIC So let's clean up all the old files:
# MAGIC * We'll run the `VACUUM` statement and voila a much cleaner file system

# COMMAND ----------

# View file system of `current_inventory_delta`
path = '/user/hive/warehouse/delta_webinar_inventory.db/current_inventory_delta'
display(dbutils.fs.ls(path))

# COMMAND ----------

# MAGIC %md ## Clean up with Vacuum
# MAGIC If you no longer need the files, you can use the `VACUUM` command to remove them.  By default, the retention is 7 days

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable retention duration check
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC 
# MAGIC -- Clear out files older than 0 hour
# MAGIC -- WARNING: Be careful when doing this, default retention is 7 days
# MAGIC VACUUM current_inventory_delta RETAIN 0 HOURS;
# MAGIC 
# MAGIC -- Renable retention duration check
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = true;

# COMMAND ----------

# View file system of `current_inventory_delta`
path = '/user/hive/warehouse/delta_webinar_inventory.db/current_inventory_delta'
display(dbutils.fs.ls(path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Copyright (2019) Databricks, Inc.  This notebook and related files are the intellectual property of Databricks, Inc. and its licensors. This notebook is provided "as-is" and is meant for educational  purposes only.  This notebook may be used only in connection with your use of the Databricks Platform Services and such use is subject to the limited license and other terms in place governing such services.