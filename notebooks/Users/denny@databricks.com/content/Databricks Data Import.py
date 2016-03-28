# Databricks notebook source exported at Mon, 28 Mar 2016 15:39:58 UTC
# MAGIC %md # Databricks Data Import
# MAGIC This is the companion notebook for the Databricks Data Import How-To Guide

# COMMAND ----------

# MAGIC %md ## Enter your AWS access keys here
# MAGIC Important: After setting the mount, remove or obfuscate this section so you are not saving access keys in clear text.

# COMMAND ----------

# MAGIC %md #### Configure your AWS key settings

# COMMAND ----------

import urllib
ACCESS_KEY = "<ACCESS_KEY>"
SECRET_KEY = "<SECRET_KEY>"
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "my-data-for-databricks"
MOUNT_NAME = "my-data"

# COMMAND ----------

# MAGIC %md #### Mount your bucket

# COMMAND ----------

dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md # Access your Data
# MAGIC Using *dbutils* you can access your files

# COMMAND ----------

display(dbutils.fs.ls("/mnt/my-data"))

# COMMAND ----------

# MAGIC %md #### Count the number of rows in all of the files in your Apache Access Web Logs folder

# COMMAND ----------

myApacheLogs = sc.textFile("/mnt/my-data/apache")
myApacheLogs.count()

# COMMAND ----------

# MAGIC %md #### Review ten rows from your Apache Access web logs

# COMMAND ----------

myApacheLogs.take(10)

# COMMAND ----------

# MAGIC %md # Setup Apache Access Log DataFrame

# COMMAND ----------

# sc is an existing SparkContext.
from pyspark.sql import SQLContext, Row

# Load the space-delimited web logs (text files)
parts = myApacheLogs.map(lambda l: l.split(" "))
apachelogs = parts.map(lambda p: Row(ipaddress=p[0], clientidentd=p[1], userid=p[2], datetime=p[3], tmz=p[4], method=p[5], endpoint=p[6], protocol=p[7], responseCode=p[8], contentSize=p[9]))

# COMMAND ----------

# Infer the schema, and register the DataFrame as a table.
schemaApacheLogs = sqlContext.createDataFrame(apachelogs)
schemaApacheLogs.registerTempTable("apachelogs")

# COMMAND ----------

# Access the table using Spark SQL within the sqlContext
sqlContext.sql("select ipaddress, endpoint from apachelogs").take(10)

# COMMAND ----------

