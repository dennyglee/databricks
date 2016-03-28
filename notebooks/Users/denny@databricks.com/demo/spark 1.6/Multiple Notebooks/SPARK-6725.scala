// Databricks notebook source exported at Mon, 28 Mar 2016 15:57:49 UTC
// MAGIC %md
// MAGIC ## [MLlib] SPARK-6725: Pipeline persistence
// MAGIC Save/load for ML Pipelines, with partial coverage of spark.ml algorithms.
// MAGIC * Persist ML Pipelines to:
// MAGIC   * Save models in the spark.ml API
// MAGIC   * Re-run workflows in a reproducible manner
// MAGIC   * Export models to non-Spark apps (e.g., model server)
// MAGIC * This is more complex than ML model persistence because:
// MAGIC   * Must persist Transformers and Estimators, not just Models.
// MAGIC   * We need a standard way to persist Params.
// MAGIC   * Pipelines and other meta-algorithms can contain other Transformers and Estimators, including as Params.
// MAGIC   * We should save feature metadata with Models
// MAGIC   
// MAGIC Specifically, this jira:
// MAGIC * Adds model export/import to the spark.ml API. 
// MAGIC * Adds the internal Saveable/Loadable API and Parquet-based format
// MAGIC 
// MAGIC ![SPARK-6725 Subtasks](https://sparkhub.databricks.com/wp-content/uploads/2015/12/SPARK-6725-subtasks.png)

// COMMAND ----------

