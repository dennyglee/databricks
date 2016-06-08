// Databricks notebook source exported at Wed, 8 Jun 2016 21:55:07 UTC
// MAGIC %md ## Check Tweet Storage
// MAGIC Via the `Collect Tweets` notebook, we are storing political-based tweets to S3.

// COMMAND ----------

// Configure Tweets directory 
val rootDir = "/mnt/databricks-denny/twitter602/"
val tweetsDir = dbutils.fs.ls(rootDir).toDF()
tweetsDir.registerTempTable("tweetsDir")

// COMMAND ----------

// Show tweets directory
display(tweetsDir)

// COMMAND ----------

// MAGIC %sql select * from tweetsDir order by name desc limit 10

// COMMAND ----------

// Get the [latest - 10] directory (will connect DataFrame to this)
val latestDir = sqlContext.sql("select name from (select name from tweetsDir order by path desc limit 10) a order by name asc")
val latestDirStr = latestDir.map {_.toString}.first
val latestDirStrTrim = latestDirStr.substring(1, latestDirStr.length-1)
val outputDirectory = rootDir + latestDirStrTrim
display(dbutils.fs.ls(outputDirectory))

// COMMAND ----------

// Create DataFrame against the latest tweets
val tweets = sqlContext.read.json(outputDirectory)
tweets.registerTempTable("tweets")

// COMMAND ----------

tweets.printSchema()

// COMMAND ----------

// MAGIC %sql select count(1) from tweets

// COMMAND ----------

// MAGIC %sql select user.location from tweets where user.location is not null limit 10;

// COMMAND ----------

// MAGIC %sql select favoriteCount, text from tweets limit 10;

// COMMAND ----------

// MAGIC %sql select hashtagEntities, text from tweets limit 10;

// COMMAND ----------

// MAGIC %sql select text from tweets limit 10;

// COMMAND ----------

// MAGIC %sql select count(1) from tweets

// COMMAND ----------

