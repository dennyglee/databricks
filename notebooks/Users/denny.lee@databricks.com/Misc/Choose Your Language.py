# Databricks notebook source
# MAGIC %scala
# MAGIC // Read data from DBFS using Scala
# MAGIC val departureDelays = spark.read
# MAGIC         .option("header", true)
# MAGIC         .option("inferSchema", true)
# MAGIC         .csv("/databricks-datasets/flights/departuredelays.csv")
# MAGIC 
# MAGIC // Create DataFrame via Scala
# MAGIC departureDelays.createOrReplaceTempView("departureDelays")

# COMMAND ----------

# MAGIC %python
# MAGIC # Perform a filtered count using Python 
# MAGIC flights = spark.sql("select date, origin, destination from departureDelays where origin = 'SEA'")
# MAGIC flights.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the top 10 destinations using Spark SQL
# MAGIC select destination, count(1) from departureDelays group by destination order by count(1) desc limit 10

# COMMAND ----------

# MAGIC %r
# MAGIC # Use SparkR to review summary statistics of flight data
# MAGIC library(SparkR)
# MAGIC flightsRDF <- sql("SELECT delay, distance FROM departureDelays where origin = 'SFO'")
# MAGIC display(summary(flightsRDF))

# COMMAND ----------

