// Databricks notebook source exported at Mon, 28 Mar 2016 15:58:17 UTC
// MAGIC %md ## SPARK-9999: Dataset API 
// MAGIC A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten). 
// MAGIC 
// MAGIC “Encoder” converts from JVM Object into a Dataset Row
// MAGIC 
// MAGIC ![JVM Object to Dataset Row](https://sparkhub.databricks.com/wp-content/uploads/2015/12/Encoder-JVM-Object-to-Dataset-Row.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC ![DataFrame and Dataset](https://sparkhub.databricks.com/wp-content/uploads/2015/12/DataFrame_and_Dataset.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### WordCount Example
// MAGIC A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten)
// MAGIC * In the example below, we will take lines of text and split them up into words. 
// MAGIC * Next, we count the number of occurances of each work in the set using a variety of Spark API.

// COMMAND ----------

// Import the SQL Functions
import org.apache.spark.sql.functions._

// Load DataFrame
val df = sqlContext.read.text("/home/pwendell/1.6/lines")

// Interpret each line as a java.lang.String (using built-in encoder)
val ds = df.as[String]
.flatMap(line => line.split(" "))               // Split on whitespace
.filter(line => line != "")                     // Filter empty words
.groupBy(word => word)
.count()

display(ds.toDF)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md 
// MAGIC ### People JSON Example
// MAGIC A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten)
// MAGIC * In the example below, we will create a JSON file  
// MAGIC * Next, we will filter and calculate the average age of the people in the JSON file

// COMMAND ----------

// Create people.json file 
dbutils.fs.put("/home/pwendell/1.6/people.json","""
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
{"name":"Michael", "age":26}
""", true)

// COMMAND ----------

// Create Person class, dataframe and dataset
import org.apache.spark.sql.Dataset

// Establish case class
case class Person(name: String, age: Long)

// Read JSON and establish dataset
val peopledf = sqlContext.read.json("/home/pwendell/1.6/people.json")
val peopleds: Dataset[Person] = peopledf.as[Person]

// COMMAND ----------

// MAGIC %md
// MAGIC Why a *Long* datatype even though the output is *bigint*?
// MAGIC * In HiveQL, *bigint* is backed by *Long*.
// MAGIC * Which is different from a Scala *bigint* and Java *bigint*
// MAGIC * Therefore, when in **schema**, when you see *bigint*, it is actually *Long* type.
// MAGIC * More information at [SPARK-11856] SQL add type cast if the real type is different but compatible with encoder schema [#9840](https://github.com/apache/spark/pull/9840)
// MAGIC 
// MAGIC Thanks to Yin Huai for the clarification!

// COMMAND ----------

// Filtering the dataset
peopleds.filter(p => p.name.startsWith("M")).collect.foreach(println)

// COMMAND ----------

// Filter the dataset, groupBy name, and calculate average
val peopleDS_startsWithM = peopleds.filter(p => p.name.startsWith("M"))
.toDF()
.groupBy($"name")
.avg("age")

// Display output
display(peopleDS_startsWithM)

// COMMAND ----------

