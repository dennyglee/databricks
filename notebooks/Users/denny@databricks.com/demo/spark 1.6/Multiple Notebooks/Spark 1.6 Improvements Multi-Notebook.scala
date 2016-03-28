// Databricks notebook source exported at Mon, 28 Mar 2016 15:51:21 UTC
// MAGIC %md # What's Coming in Spark 1.6?
// MAGIC 
// MAGIC Watch the [Apache 1.6 with Databricks cofounder Patrick Wendell](http://go.databricks.com/apache-spark-1.6-with-patrick-wendell) webinar to see this notebook in action!
// MAGIC 
// MAGIC To view this as a [single notebook](http://go.databricks.com/hubfs/notebooks/Spark_1.6_Improvements.html).
// MAGIC 
// MAGIC 
// MAGIC This *Scala* notebook contains a number of Spark 1.6 improvements including 
// MAGIC * [SPARK-10000](http://go.databricks.com/hubfs/notebooks/SPARK-10000.html): **Unified Memory Management** - Shared memory for execution and caching instead of exclusive division of the regions.
// MAGIC * [SPARK-11197](http://go.databricks.com/hubfs/notebooks/SPARK-11197.html): **SQL Queries on Files** - Concise syntax for running SQL queries over files of any supported format without registering a table. Example Usage.
// MAGIC * [SPARK-9999](http://go.databricks.com/hubfs/notebooks/SPARK-9999.html): **Datasets API** - A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten). 
// MAGIC * [Streaming] [SPARK-2629](http://go.databricks.com/hubfs/notebooks/SPARK-2629.html): **New improved state management** - trackStateByKey - a DStream transformation for stateful stream processing, supercedes updateStateByKey in functionality and performance.
// MAGIC * [Streaming] [SPARK-10885 PR#8950](http://go.databricks.com/hubfs/notebooks/SPARK-10885_PR8950.html): **Display the failed output op in Streaming**
// MAGIC * [SPARK-4849](http://go.databricks.com/hubfs/notebooks/SPARK-4849.html): **Advanced Layout of Cached Data** - storing partitioning and ordering schemes in In-memory table scan, and adding distributeBy and localSort to DF API.
// MAGIC * [MLlib] [SPARK-6725](http://go.databricks.com/hubfs/notebooks/SPARK-6725.html): **Pipeline persistence**  - Save/load for ML Pipelines, with partial coverage of spark.ml algorithms.
// MAGIC * [SPARK-9836](http://cdn2.hubspot.net/hubfs/438089/notebooks/Spark_1.6_R_Improvements.html): **R-like Statistics for GLMs** - (Partial) R-like stats for ordinary least squares via summary(model)
// MAGIC  
// MAGIC [List of various other Spark 1.6 Improvements by Category](http://go.databricks.com/hubfs/notebooks/List_of_Spark_1.6_Improvements.html)
// MAGIC * Performance
// MAGIC * Spark SQL
// MAGIC * Spark Streaming
// MAGIC * MLlib
// MAGIC * More information
// MAGIC 
// MAGIC Other Miscellaneous examples include:
// MAGIC * [SPARK-11745](http://go.databricks.com/hubfs/notebooks/SPARK-11745.html): **Reading non-standard JSON files** - Added options to read non-standard JSON files (e.g. single-quotes, unquoted attributes)
// MAGIC * [SPARK-10142](http://go.databricks.com/hubfs/notebooks/SPARK-10142.html): **Per-operator Metrics for SQL Execution** - Display statistics on a peroperator basis for memory usage and spilled data size.
// MAGIC * [SPARK-11111](http://go.databricks.com/hubfs/notebooks/SPARK-11111.html): **Fast null-safe joins** - Joins using null-safe equality (<=>) will now execute using SortMergeJoin instead of computing a cartisian product
// MAGIC * [SPARK-8518](http://go.databricks.com/hubfs/notebooks/SPARK-8518.html): **Survival analysis** - Log-linear model for survival analysis

// COMMAND ----------

