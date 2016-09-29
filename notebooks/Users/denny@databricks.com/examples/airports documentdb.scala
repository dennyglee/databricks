// Databricks notebook source exported at Thu, 29 Sep 2016 21:40:43 UTC
// MAGIC %md ## Connecting to Azure DocumentDB
// MAGIC Databricks Community Edition Notebook (on AWS) connecting to Azure DocumentDB instance 
// MAGIC 
// MAGIC Requires: Spark 2.0+, Scala 2.11

// COMMAND ----------

/* Import Spark SQL and DataFrame Classes (needed by Azure DocumentDB) */
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

/* Import Azure DocumentDB Spark Classes */
import azure.spark.docdb._
import azure.spark.docdb.config._
import azure.spark.docdb.config.DocdbConfig._

// COMMAND ----------

/* Build connection to Azure DocumentDB Instance (via URL, master key, database, and collection) */
val builder = DocdbConfigBuilder(Map(Endpoint -> "https://doctorwho.documents.azure.com:443/", Masterkey -> "le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==", Database -> "Airports", Collection ->"Codes", SamplingRatio -> 1.0))

// COMMAND ----------

/* Configure connection to Azure DocumentDB Instance */
val readConfig = builder.build()
val docdbctx = toDocdbContext(new SQLContext(sc))
val docdbRDD = docdbctx.fromDocDB(readConfig)


// COMMAND ----------

// Create Temporary View
docdbRDD.createTempView("AirportCodes")

// COMMAND ----------

// Query DataFrame
val dataFrame = spark.sql("SELECT * FROM AirportCodes WHERE State = 'WA'")

// COMMAND ----------

display(dataFrame)

// COMMAND ----------

// MAGIC %sql SELECT State, count(1) FROM AirportCodes WHERE Country = 'USA' and State <> 'null' GROUP BY State 

// COMMAND ----------


