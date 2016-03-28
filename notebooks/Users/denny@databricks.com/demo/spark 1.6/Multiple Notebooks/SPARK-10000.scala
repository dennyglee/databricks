// Databricks notebook source exported at Mon, 28 Mar 2016 15:54:17 UTC
// MAGIC %md ## SPARK-10000: Unified Memory Management
// MAGIC Shared memory for execution and caching instead of exclusive division of the regions.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Memory Management in Spark: < 1.5
// MAGIC * Two separate memory managers: 
// MAGIC  * Execution memory: computation of shuffles, joins, sorts, aggregations
// MAGIC  * Storage memory: caching and propagating internal data sources across cluster
// MAGIC * Issues with this:
// MAGIC  * Manual intervention to avoid unnecessary spilling
// MAGIC  * No pre-defined defaults for all workloads
// MAGIC  * Need to partition the execution (shuffle) memory and cache memory fractions
// MAGIC * Goal: Unify these two memory regions and borrow from each other	

// COMMAND ----------

// MAGIC %md
// MAGIC ### Unified Memory Management in Spark 1.6
// MAGIC * Can cross between execution and storage memory
// MAGIC  * When execution memory exceeds its own region, it can borrow as much of the storage space as is free and vice versa
// MAGIC  * Borrowed storage memory can be evicted at any time (though not execution memory at this time)
// MAGIC * New configurations to be introduced
// MAGIC * Under memory pressure
// MAGIC  * Evict cached data
// MAGIC  * Evict storage memory
// MAGIC  * Evict execution memory
// MAGIC * Notes:
// MAGIC  * Dynamically allocated reserved storage region that exection memory cannot borrow from
// MAGIC  * Cached data evicted only if actual storage exceeds the dynamically allocated reserved storage region
// MAGIC * Result:
// MAGIC  * Allows for better memory management for multi-tenancy and applications relying heavily on caching
// MAGIC  * No cap on storage memory nor on execution memory
// MAGIC  * Dynamic allocation of reserved storage memory will not require user configuration
// MAGIC 
// MAGIC  
// MAGIC  

// COMMAND ----------

// Execute reduceByKey that will cause spill (DBC cluster with 3 nodes, 90GB RAM)
val result = sc.parallelize(0 until 200000000).map { i => (i / 2, i) }.reduceByKey(math.max).collect()

// COMMAND ----------

// MAGIC %md #### Review Stage Details for reduceByKey job
// MAGIC * Spark 1.6 completes faster than the same size Spark 1.5 cluster
// MAGIC * Note the Spark 1.5 spills to memory and disk
// MAGIC 
// MAGIC ![reduceByKey Spark 1.6 run](https://sparkhub.databricks.com/wp-content/uploads/2015/11/SPARK-10000-1.6-e1448833785393.png)
// MAGIC ![reduceByKey Spark 1.5 run](https://sparkhub.databricks.com/wp-content/uploads/2015/11/SPARK-10000-1.5-e1448833729236.png)

// COMMAND ----------

