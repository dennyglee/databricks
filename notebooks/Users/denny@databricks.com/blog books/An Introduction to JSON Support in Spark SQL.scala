// Databricks notebook source exported at Mon, 28 Mar 2016 17:06:14 UTC
// MAGIC %md ## An Introduction to JSON Support in Spark SQL Notebook
// MAGIC This notebook supports the blog post [An Introduction to JSON Support in Spark SQL](https://databricks.com/blog/2015/02/02/an-introduction-to-json-support-in-spark-sql.html).

// COMMAND ----------

// MAGIC %md ### Create temporary file people.json
// MAGIC Creating a temporary file within DBFS to query this data.

// COMMAND ----------

// Create people.json file 
dbutils.fs.put("/tmp/json/people.json","""
{"name":"Yin", "address":{"city":"Columbus","state":"Ohio"}}
{"name":"Michael", "address":{"city":null, "state":"California"}}
""", true)

// COMMAND ----------

// MAGIC %md ### Existing Practices
// MAGIC In a system like Hive, the JSON objects are typically stored as values of a single column. To access this data, fields in JSON objects are extracted and flattened using a UDF. In the SQL query shown below, the outer fields (name and address) are extracted and then the nested address field is further extracted.

// COMMAND ----------

// MAGIC %sql 
// MAGIC create external table tmp_people (jsonObject string) location '/tmp/json/';

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT
// MAGIC   v1.name, v2.city, v2.state 
// MAGIC FROM tmp_people
// MAGIC   LATERAL VIEW json_tuple(tmp_people.jsonObject, 'name', 'address') v1 
// MAGIC      as name, address
// MAGIC   LATERAL VIEW json_tuple(v1.address, 'city', 'state') v2
// MAGIC      as city, state;

// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table tmp_people;

// COMMAND ----------

// MAGIC %md ### JSON support in Spark SQL
// MAGIC Spark SQL provides a natural syntax for querying JSON data along with automatic inference of JSON schemas for both reading and writing data. Spark SQL understands the nested fields in JSON data and allows users to directly access these fields without any explicit transformations. 

// COMMAND ----------

// Create DataFrame (df) from this JSON
val people = sqlContext.read.json("/tmp/json/people.json")

// Register TempTable for SQL queries
people.registerTempTable("people")

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- Query the people table
// MAGIC SELECT name, address.city, address.state FROM people

// COMMAND ----------

// MAGIC %md Just like any other DataFrame, you can query (above) and review the schema (below) of your DataFrame generated from JSON.

// COMMAND ----------

// Review the schema from the people DataFrame
people.printSchema()

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- drop people table
// MAGIC drop table people

// COMMAND ----------

// MAGIC %md You can also create the `people` table from SQL

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- Create temporary table `people` via Spark SQL
// MAGIC CREATE TEMPORARY TABLE people
// MAGIC USING org.apache.spark.sql.json
// MAGIC OPTIONS (path '/tmp/json/people.json')

// COMMAND ----------

// MAGIC %sql 
// MAGIC select name, address.state, address.city from people