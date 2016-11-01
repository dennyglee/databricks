// Databricks notebook source exported at Tue, 1 Nov 2016 05:49:07 UTC
// MAGIC %md ## GraphFrames TriangleCount on HEP-TH dataset
// MAGIC Dataset source is from [SNAP High Energy Physics - Theory collaboration network](https://snap.stanford.edu/data/ca-HepTh.html).  Note, I'm using a modified version of the file which does not contain comments to simplify the code below.
// MAGIC 
// MAGIC References: [Stackoverflow: Spark GraphX : requirement failed: Invalid initial capacity](http://stackoverflow.com/questions/40337366/spark-graphx-requirement-failed-invalid-initial-capacity)

// COMMAND ----------

// FilePath for the dataset
val filepath = "/mnt/tardis6/data/snap/modified-ca-HepTh.txt"

// Read the data in using spark.csv
val df = spark.read
    .format("com.databricks.spark.csv")
    .option("delimiter", "\t")
    .option("header", "false") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(filepath)

// Rename columns
val newColumnNames = Seq("src", "dst")
val edges = df.toDF(newColumnNames: _*)

// create Temp View
hep_th.createOrReplaceTempView("edges")

// COMMAND ----------

// Create vertices
val vertices = spark.sql("select distinct src as id from (select src from edges union all select dst from edges) a order by src")


// COMMAND ----------

// Build graph
import org.graphframes._
val g = GraphFrame(vertices, edges)

// COMMAND ----------

g.vertices.count()

// COMMAND ----------

g.edges.count()

// COMMAND ----------

val results = g.triangleCount.run()
display(results)

// COMMAND ----------


