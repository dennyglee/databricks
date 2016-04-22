// Databricks notebook source exported at Fri, 22 Apr 2016 04:32:23 UTC
// MAGIC %md ## Simplifying and Visualizing Decision Trees with ML Pipelines and Databricks

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/asa/

// COMMAND ----------

val airlines = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/databricks-datasets/asa/small/small.csv")



// COMMAND ----------

val selectedData = df.select("year", "model")
selectedData.write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("newcars.csv")

// COMMAND ----------

# Obtain airlines dataset
#airlines <- read.df(sqlContext, path="/databricks-datasets/asa/airlines/2002.csv", source="com.databricks.spark.csv", header="true", inferSchema="true")
#airlines <- read.df(sqlContext, path="/databricks-datasets/asa/airlines/", source="com.databricks.spark.csv", header="true", inferSchema="true")

# For Databricks Community Edition, please use this smaller file
airlines <- read.df(sqlContext, path="/databricks-datasets/asa/small/small.csv", source="com.databricks.spark.csv", header="true", inferSchema="true")

# Obtain planes table
planes <- read.df(sqlContext, "/databricks-datasets/asa/planes/plane-data.csv", source="com.databricks.spark.csv", header="true", inferSchema="true")

# Rename some of the columns in the `planes` table
colnames(planes)[1] <- "tail_number"
colnames(planes)[9] <- "plane_year"

# Join airlines and planes dataset by $TailNum
joined <- join(airlines, planes, airlines$TailNum == planes$tail_number)

# Setup training dataset 
training <- dropna(joined)