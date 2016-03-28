// Databricks notebook source exported at Mon, 28 Mar 2016 15:55:30 UTC
// MAGIC %md ## SPARK-11197: Run SQL queries directly on files
// MAGIC Starting with Spark 1.6, you can run SQL queries directly on files without needing to first create a table pointing to the files.

// COMMAND ----------

// MAGIC %md ### Setup the data
// MAGIC * Create a two line file with the following text into the file system

// COMMAND ----------

dbutils.fs.put("/home/pwendell/1.6/lines","""
Hello hello is it me you are looking for
Hello how are you
""", true)

// COMMAND ----------

// MAGIC %md 
// MAGIC ![Lionel Richie Adele "Hello" meme](http://www.b365.ro/media/image/201510/w620/adele_lionel_ritchie_hello_64012700.png) 

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- View the data
// MAGIC SELECT * FROM text.`/home/pwendell/1.6/lines` WHERE value != ""

// COMMAND ----------

