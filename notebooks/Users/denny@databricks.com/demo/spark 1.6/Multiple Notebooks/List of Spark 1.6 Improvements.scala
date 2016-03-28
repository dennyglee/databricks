// Databricks notebook source exported at Mon, 28 Mar 2016 15:49:17 UTC
// MAGIC %md # List of Spark 1.6 Improvements by Category

// COMMAND ----------

// MAGIC %md ## Performance
// MAGIC * [SPARK-10000](https://issues.apache.org/jira/browse/SPARK-10000): Unified Memory Management - Shared memory for execution and caching instead of exclusive division of the regions.
// MAGIC * [SPARK-10917](https://issues.apache.org/jira/browse/SPARK-10917), [SPARK-11149](https://issues.apache.org/jira/browse/SPARK-11149): In-memory Columnar Cache Performance - Significant (up to 14x) speed up when caching data that contains complex types in DataFrames or SQL.
// MAGIC * [SPARK-11389](https://issues.apache.org/jira/browse/SPARK-11389):SQL Execution Using Off-Heap Memory - Support for configuring query execution to occur using off-heap memory to avoid GC overhead
// MAGIC * [SPARK-4849](https://issues.apache.org/jira/browse/SPARK-4849): Advanced Layout of Cached Data - storing partitioning and ordering schemes in In-memory table scan, and adding distributeBy and localSort to DF API
// MAGIC * [SPARK-9858](https://issues.apache.org/jira/browse/SPARK-9598): Adaptive query execution - Initial support for automatically selecting the number of reducers for joins and aggregations.
// MAGIC * [SPARK-11787](https://issues.apache.org/jira/browse/SPARK-11787): Parquet Performance - Improve Parquet scan performance when using flat schemas.

// COMMAND ----------

// MAGIC %md ## Spark SQL
// MAGIC * [SPARK-9999](https://issues.apache.org/jira/browse/SPARK-9999)  Dataset API  
// MAGIC * [SPARK-11197](https://issues.apache.org/jira/browse/SPARK-11197) SQL Queries on Files
// MAGIC * [SPARK-11745](https://issues.apache.org/jira/browse/SPARK-11745) Reading non-standard JSON files
// MAGIC * [SPARK-10412](https://issues.apache.org/jira/browse/SPARK-10412) Per-operator Metrics for SQL Execution
// MAGIC * [SPARK-11329](https://issues.apache.org/jira/browse/SPARK-11329) Star (*) expansion for StructTypes
// MAGIC * [SPARK-11111](https://issues.apache.org/jira/browse/SPARK-11111) Fast null-safe joins
// MAGIC * [SPARK-10978](https://issues.apache.org/jira/browse/SPARK-10978) Datasource API Avoid Double Filter 

// COMMAND ----------

// MAGIC %md ## Spark Streaming
// MAGIC **API Updates**
// MAGIC * [SPARK-2629](https://issues.apache.org/jira/browse/SPARK-2629)  New improved state management
// MAGIC * [SPARK-11198](https://issues.apache.org/jira/browse/SPARK-11198) Kinesis record deaggregation
// MAGIC * [SPARK-10891](https://issues.apache.org/jira/browse/SPARK-10891) Kinesis message handler function
// MAGIC * [SPARK-6328](https://issues.apache.org/jira/browse/SPARK-6328)  Python Streamng Listener API
// MAGIC 
// MAGIC **UI Improvements**
// MAGIC * Made failures visible in the streaming tab, in the timelines, batch list, and batch details page.
// MAGIC * Made output operations visible in the streaming tab as progress bars

// COMMAND ----------

// MAGIC %md ## MLlib
// MAGIC ** New algorithms / models **
// MAGIC * [SPARK-8518](https://issues.apache.org/jira/browse/SPARK-8518)  Survival analysis - Log-linear model for survival analysis
// MAGIC * [SPARK-9834](https://issues.apache.org/jira/browse/SPARK-9834)  Normal equation for least squares - Normal equation solver, providing R-like model summary statistics
// MAGIC * [SPARK-3147](https://issues.apache.org/jira/browse/SPARK-3147)  Online hypothesis testing - A/B testing in the Spark Streaming framework
// MAGIC * [SPARK-9930](https://issues.apache.org/jira/browse/SPARK-9930)  New feature transformers - ChiSqSelector, QuantileDiscretizer, SQL transformer
// MAGIC * [SPARK-6517](https://issues.apache.org/jira/browse/SPARK-6517)  Bisecting K-Means clustering - Fast top-down clustering variant of K-Means
// MAGIC 
// MAGIC ** API Improvements **
// MAGIC * ML Pipelines
// MAGIC  * [SPARK-6725](https://issues.apache.org/jira/browse/SPARK-6725)  Pipeline persistence - Save/load for ML Pipelines, with partial coverage of spark.ml algorithms
// MAGIC  * [SPARK-5565](https://issues.apache.org/jira/browse/SPARK-5565)  LDA in ML Pipelines - API for Latent Dirichlet Allocation in ML Pipelines
// MAGIC * R API
// MAGIC  * [SPARK-9836](https://issues.apache.org/jira/browse/SPARK-9836)  R-like statistics for GLMs - (Partial) R-like stats for ordinary least squares via summary(model)
// MAGIC  * [SPARK-9681](https://issues.apache.org/jira/browse/SPARK-9681)  Feature interactions in R formula - Interaction operator ":" in R formula
// MAGIC * Python API - Many improvements to Python API to approach feature parity
// MAGIC 
// MAGIC ** Miscellaneous Improvements **
// MAGIC * [SPARK-7685](https://issues.apache.org/jira/browse/SPARK-7685) , [SPARK-9642](https://issues.apache.org/jira/browse/SPARK-9642)  Instance weights for GLMs - Logistic and Linear Regression can take instance weights
// MAGIC * [SPARK-10384](https://issues.apache.org/jira/browse/SPARK-10384), [SPARK-10385](https://issues.apache.org/jira/browse/SPARK-10385) Univariate and bivariate statistics in DataFrames - Variance, stddev, correlations, etc.
// MAGIC * [SPARK-10117](https://issues.apache.org/jira/browse/SPARK-10117) LIBSVM data source - LIBSVM as a SQL data source

// COMMAND ----------

// MAGIC %md ## More Information
// MAGIC * [Apache Spark 1.6.0 Release Preview](http://bit.ly/1Ilk9da)
// MAGIC * [Spark 1.6 Preview available in Databricks](http://bit.ly/1QR0RPC)
// MAGIC * [Spark 1.6 Improvements Notebook (Exported HTML)](http://bit.ly/1lrvdLc)
// MAGIC * [Spark 1.6 R Improvements Notebook (Exported HTML)](http://bit.ly/1OBkjMM)

// COMMAND ----------

