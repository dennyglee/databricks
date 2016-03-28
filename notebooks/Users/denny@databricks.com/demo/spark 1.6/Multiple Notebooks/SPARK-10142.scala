// Databricks notebook source exported at Mon, 28 Mar 2016 15:54:33 UTC
// MAGIC %md ## SPARK-10142: Per-operator Metrics for SQL Execution
// MAGIC Display statistics on a peroperator basis for memory usage and spilled data size.

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(1) from people_ns where age > 0

// COMMAND ----------

// MAGIC %md 
// MAGIC To view the *per-operator metrics* 
// MAGIC * Click on ***View*** under *Spark Jobs* for your SQL statement above
// MAGIC * Click the ***SQL*** tab
// MAGIC * Click the ***take at OutputAggregator.scala:NN***
// MAGIC * Expand the cell below to take a veiw at the screenshot

// COMMAND ----------

// MAGIC %md ![SPARK-10142 Screenshot](https://sparkhub.databricks.com/wp-content/uploads/2015/11/SPARK-10142_screenshot-e1448833882479.png)