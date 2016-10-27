// Databricks notebook source exported at Thu, 27 Oct 2016 00:21:28 UTC
// MAGIC %md ## Flights Graph Using Spark and Azure DocumentDB
// MAGIC Databricks Community Edition Notebook (on AWS) connecting to Azure DocumentDB instance 
// MAGIC 
// MAGIC Requires: Spark 2.0+, Scala 2.11

// COMMAND ----------

// MAGIC %md ### Flights Data
// MAGIC Obtaining flights information from Azure DocumentDB

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
val builder = DocdbConfigBuilder(Map(Endpoint -> "https://doctorwho.documents.azure.com:443/", Masterkey -> "le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==", Database -> "DepartureDelays", Collection ->"delays", SamplingRatio -> 1.0))

// COMMAND ----------

/* Configure connection to Azure DocumentDB Instance */
val readConfig = builder.build()
val docdbctx = toDocdbContext(new SQLContext(sc))
val docdbRDD = docdbctx.fromDocDB(readConfig)
docdbRDD.createOrReplaceTempView("departureDelays")

// COMMAND ----------

// MAGIC %md ### Obtain Airports Information
// MAGIC Connect to Databricks File System (DBFS) to obtain the airport codes

// COMMAND ----------

val airportsFilePath = "/databricks-datasets/flights/airport-codes-na.txt"
val airports = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\t").load(airportsFilePath)
airports.createOrReplaceTempView("airports")


// COMMAND ----------

// MAGIC %md ## Create Edges and Vertices
// MAGIC Build the edges and vertices (in DataFrames) for our graph

// COMMAND ----------

// Create the tripEdges DataFrame 
val tripEdges = spark.sql("select f.date as tripid, cast(f.delay as int) as delay, f.origin as src, f.destination as dst, a.city as city_dst, a.state as state_dst from departureDelays f join airports a on a.iata = f.destination join airports o on o.iata = f.origin")
tripEdges.cache()


// COMMAND ----------

// Display (instead of .show - cooler method ala Databricks)
display(tripEdges)

// COMMAND ----------

// Create the tripVertices DataFrame (instead of .show - cooler method ala Databricks)
val tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
tripVertices.cache()


// COMMAND ----------

// Create the tripEdges DataFrame (instead of .show - cooler method ala Databricks)
display(tripVertices)

// COMMAND ----------

// MAGIC %md ## Create Graph
// MAGIC Create the `tripGraph` using our two DataFrames

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.graphframes._
val tripGraph = GraphFrame(tripVertices, tripEdges)


// COMMAND ----------

// MAGIC %md ## Q: Determine delayed flights leaving Seattle

// COMMAND ----------

display(tripGraph.edges.filter("src = 'SEA' and delay > 0").groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")).limit(10))

// COMMAND ----------

// MAGIC %md ## Q: What is the most important airport?
// MAGIC This can be determined by calculating the number of degrees in to the airport.  i.e. determine `inDegrees`.

// COMMAND ----------

display(tripGraph.inDegrees.sort(desc("inDegree")).limit(10))


// COMMAND ----------

// MAGIC %md ## Q: What are the most popular single-hop cities?
// MAGIC This can be calculated by looking at the tripGraph and running a groupBy statement against the edges of the graph.

// COMMAND ----------

val topTrips = tripGraph.edges.groupBy("src", "dst").agg(count("delay").alias("trips"))
display(topTrips.orderBy(desc("trips")).limit(10))

// COMMAND ----------

// MAGIC %md Notice the top most important flights are {LAX, SFO} and {SFO, LAX}.  One of the reasons why Alaska Airlines purchased Virgin America was to get access to this vital hub.  [Alaska Air Sees Virgin America as Key to West Coast](http://www.nytimes.com/2016/04/05/business/dealbook/alaska-airlines-parent-to-buy-virgin-america-for-2-6-billion-in-cash.html?_r=0)

// COMMAND ----------

// MAGIC %md ## Q: How many direct flights between [SEA] and [SFO]?

// COMMAND ----------

val paths = tripGraph.bfs.fromExpr("id = 'SEA'").toExpr("id = 'SFO'").maxPathLength(1).run()

// COMMAND ----------

display(paths)

// COMMAND ----------

// MAGIC %md ## Q: How many direct flights between SFO and BUF?

// COMMAND ----------

val paths = tripGraph.bfs.fromExpr("id = 'SEA'").toExpr("id = 'BUF'").maxPathLength(1).run()
paths.count()

// COMMAND ----------

// MAGIC %md ## Q: Any one-stop flights between SFO and BUF?

// COMMAND ----------

val paths = tripGraph.bfs.fromExpr("id = 'SEA'").toExpr("id = 'BUF'").maxPathLength(2).run()

// COMMAND ----------

display(paths)

// COMMAND ----------

// Review most popular layovers
display(paths.groupBy("v1.id", "v1.City").count().orderBy(desc("count")).limit(10))


// COMMAND ----------

// MAGIC %md #### Re-write this query using SQL

// COMMAND ----------

paths.createOrReplaceTempView("paths")

// COMMAND ----------

// MAGIC %sql
// MAGIC select v1, count(1) 
// MAGIC from paths 
// MAGIC group by v1 
// MAGIC order by count(1) desc 
// MAGIC limit 10

// COMMAND ----------

val ranks = tripGraph.pageRank.resetProbability(0.15).maxIter(5).run()

// COMMAND ----------

display(ranks.vertices.orderBy(desc("pagerank")).limit(10))

// COMMAND ----------

display(ranks.vertices.orderBy(desc("pagerank")).limit(10))

// COMMAND ----------


