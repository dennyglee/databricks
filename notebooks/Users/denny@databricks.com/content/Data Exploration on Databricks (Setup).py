# Databricks notebook source exported at Mon, 28 Mar 2016 15:37:39 UTC
# MAGIC %md # Data Exploration on Databricks (Setup)

# COMMAND ----------

# MAGIC %md ### Parsing weblogs with regular expressions to create a table
# MAGIC 
# MAGIC * Original Format: %s %s %s [%s] \"%s %s HTTP/1.1\" %s %s
# MAGIC * Example Web Log Row 
# MAGIC  * 10.0.0.213 - 2185662 [14/Aug/2015:00:05:15 -0800] "GET /Hurricane+Ridge/rss.xml HTTP/1.1" 200 288

# COMMAND ----------

# MAGIC %md ## Setup Instructions
# MAGIC * Please refer to the Data Exploration on Databricks How-To Guide for the location of the source files to import for this notebook.
# MAGIC * Please refer to the Databricks Data Import How-To Guide on how to import data into S3 for use with Databricks notebooks.

# COMMAND ----------

import urllib
ACCESS_KEY = "[REPLACE_WITH_ACCESS_KEY]"
SECRET_KEY = "[REPLACE_WITH_SECRET_KEY]"
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "my-data-for-databricks"
MOUNT_NAME = "my-data"

# COMMAND ----------

# MAGIC %md ## Sample Apache Access Web Logs

# COMMAND ----------

display(dbutils.fs.ls("/mnt/my-data/apache"))

# COMMAND ----------

myApacheLogs = sc.textFile("/mnt/my-data/apache")
myApacheLogs.take(10)

# COMMAND ----------

# MAGIC %md ## Sample Web Response Codes

# COMMAND ----------

display(dbutils.fs.ls("/mnt/my-data/response"))

# COMMAND ----------

myResponseCodes = sc.textFile("/mnt/my-data/response")
myResponseCodes.take(10)

# COMMAND ----------

# MAGIC %md ## Sample Mapping between IP Address and Geography
# MAGIC * Note, this mapping is generated for the above web logs

# COMMAND ----------

display(dbutils.fs.ls("/mnt/my-data/map"))

# COMMAND ----------

myIPtoGeo = sc.textFile("/mnt/my-data/map")
myIPtoGeo.take(10)

# COMMAND ----------

