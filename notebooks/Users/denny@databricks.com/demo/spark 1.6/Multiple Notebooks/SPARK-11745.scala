// Databricks notebook source exported at Mon, 28 Mar 2016 15:56:00 UTC
// MAGIC %md ## SPARK-11745: Enable more JSON parsing options for parsing non-standard JSON files
// MAGIC Starting with Spark 1.6, you can read non-standard JSON files including single quotes and unquoted attributes

// COMMAND ----------

//
// Create a non-standard people.json file 
//   - Using single quotes (instead of double quotes)
//   - Include Java / C++ comments
//   - Using numerics with leading zeros
//
dbutils.fs.put("/home/pwendell/1.6/people-nonstandard.json","""
{'name':'Michael', 'desc':/* Hide Comment */ '#SparkRocks'}
{'name':'Andy', 'age':0030, 'desc':'#Plutoflyby'}
{'name':'Justin', 'age':0019}
""", true)

// COMMAND ----------

// 
// Read non-standard people.json file
//   - Specify options to allowComments and allowNumericLeadingZeros to true
//   - By default, allowSingleQuotes is true
//
val people_ns = sqlContext.read.option("allowNumericLeadingZeros", "true").option("allowComments", "true").json("/home/pwendell/1.6/people-nonstandard.json")

// Register Temp Table
people_ns.registerTempTable("people_ns")

// COMMAND ----------

// SQL Query using Scala
sqlContext.sql("SELECT name, age, desc from people_ns").collect.foreach(println)


// COMMAND ----------

// MAGIC %sql SELECT name, age, desc from people_ns

// COMMAND ----------

