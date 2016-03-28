// Databricks notebook source exported at Mon, 28 Mar 2016 15:55:13 UTC
// MAGIC %md ## SPARK-11111: Fast null-safe join
// MAGIC Prior to Spark 1.6, null safe joins are executed with a Cartesian product. As of Spark 1.6, joins with only null safe equality should not result in a Cartesian product.

// COMMAND ----------

sqlContext.sql("select * from people_ns a join people b on (a.age <=> b.age)").explain

// COMMAND ----------

// MAGIC %md Below is the Spark 1.5 plan
// MAGIC 
// MAGIC ![Spark 1.5 Null Cartesian Product](https://sparkhub.databricks.com/wp-content/uploads/2015/11/Spark_1.5_null_cartesian_join-e1448833412690.png)

// COMMAND ----------

