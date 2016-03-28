// Databricks notebook source exported at Mon, 28 Mar 2016 15:57:30 UTC
// MAGIC %md ## SPARK-4849: Advanced Layout of Cached Data
// MAGIC * Storing partitioning and ordering schemes in In-memory table scan
// MAGIC  * Allows for performance improvements: e.g. in Joins, an extra partition step can be saved based on this information
// MAGIC * Adding distributeBy and localSort to DF API
// MAGIC  * Similar to HiveQL’s DISTRIBUTE BY
// MAGIC  * Allows the user to control the partitioning and ordering of a data set 

// COMMAND ----------

