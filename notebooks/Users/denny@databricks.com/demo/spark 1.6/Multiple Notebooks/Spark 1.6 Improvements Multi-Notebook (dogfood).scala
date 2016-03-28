// Databricks notebook source exported at Mon, 28 Mar 2016 15:51:38 UTC
// MAGIC %md # What's Coming in Spark 1.6?
// MAGIC This *Scala* notebook contains a number of Spark 1.6 improvements including 
// MAGIC * [SPARK-10000](https://dogfood.staging.cloud.databricks.com/#notebook/2171514): **Unified Memory Management** - Shared memory for execution and caching instead of exclusive division of the regions.
// MAGIC * [SPARK-11197](https://dogfood.staging.cloud.databricks.com/#notebook/2171526): **SQL Queries on Files** - Concise syntax for running SQL queries over files of any supported format without registering a table. Example Usage.
// MAGIC * [SPARK-9999](https://dogfood.staging.cloud.databricks.com/#notebook/2171539): **Datasets API** - A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten). 
// MAGIC * [Streaming] [SPARK-2629](https://dogfood.staging.cloud.databricks.com/#notebook/2171561): **New improved state management** - trackStateByKey - a DStream transformation for stateful stream processing, supercedes updateStateByKey in functionality and performance.
// MAGIC * [Streaming] [SPARK-10885 PR#8950](https://dogfood.staging.cloud.databricks.com/#notebook/2171566): **Display the failed output op in Streaming**
// MAGIC * [SPARK-4849](https://dogfood.staging.cloud.databricks.com/#notebook/2171575): **Advanced Layout of Cached Data** - storing partitioning and ordering schemes in In-memory table scan, and adding distributeBy and localSort to DF API.
// MAGIC * [MLlib] [SPARK-6725](https://dogfood.staging.cloud.databricks.com/#notebook/2171579): **Pipeline persistence**  - Save/load for ML Pipelines, with partial coverage of spark.ml algorithms.
// MAGIC * [SPARK-9836](https://dogfood.staging.cloud.databricks.com/#notebook/2170925): **R-like Statistics for GLMs** - (Partial) R-like stats for ordinary least squares via summary(model)
// MAGIC  
// MAGIC [List of various other Spark 1.6 Improvements by Category](https://dogfood.staging.cloud.databricks.com/#notebook/2171584)
// MAGIC * Performance
// MAGIC * Spark SQL
// MAGIC * Spark Streaming
// MAGIC * MLlib
// MAGIC * More information
// MAGIC 
// MAGIC Other Miscellaneous examples include:
// MAGIC * [SPARK-11745](https://dogfood.staging.cloud.databricks.com/#notebook/2171596): **Reading non-standard JSON files** - Added options to read non-standard JSON files (e.g. single-quotes, unquoted attributes)
// MAGIC * [SPARK-10142](https://dogfood.staging.cloud.databricks.com/#notebook/2171604): **Per-operator Metrics for SQL Execution** - Display statistics on a peroperator basis for memory usage and spilled data size.
// MAGIC * [SPARK-11111](https://dogfood.staging.cloud.databricks.com/#notebook/2171613): **Fast null-safe joins** - Joins using null-safe equality (<=>) will now execute using SortMergeJoin instead of computing a cartisian product
// MAGIC * [SPARK-8518](https://dogfood.staging.cloud.databricks.com/#notebook/2171619): **Survival analysis** - Log-linear model for survival analysis

// COMMAND ----------

