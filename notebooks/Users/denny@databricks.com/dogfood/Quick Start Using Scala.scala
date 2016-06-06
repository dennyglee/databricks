// Databricks notebook source exported at Mon, 6 Jun 2016 05:24:01 UTC
// MAGIC %md ## Quick Start Using Scala
// MAGIC * Using a Databricks notebook to showcase RDD operations using Scala
// MAGIC * Reference http://spark.apache.org/docs/latest/quick-start.html

// COMMAND ----------

// Take a look at the file system
display(dbutils.fs.ls("/databricks-datasets/samples/docs/"))

// COMMAND ----------

// Setup the textFile RDD to read the README.md file
//   Note this is lazy 
val textFile = sc.textFile("/databricks-datasets/samples/docs/README.md")

// COMMAND ----------

// MAGIC %md RDDs have ***actions***, which return values, and ***transformations***, which return pointers to new RDDs.

// COMMAND ----------

// When performing an action (like a count) this is when the textFile is read and aggregate calculated
//    Click on [View] to see the stages and executors
textFile.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scala Count (Jobs)
// MAGIC ![Scala Count Jobs](https://sparkhub.databricks.com/wp-content/uploads/2015/12/Scala-Count-Jobs-e1450067391785.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scala Count (Stages)
// MAGIC * Notice how the file is read during the *.count()* action
// MAGIC * Many Spark operations are lazy and executed upon some action
// MAGIC ![Scala Count Jobs](https://sparkhub.databricks.com/wp-content/uploads/2015/12/Scala-Count-Stages-e1450067376679.png)

// COMMAND ----------

// Output the first line from the text file
textFile.first()

// COMMAND ----------

// MAGIC %md 
// MAGIC Now we're using a filter ***transformation*** to return a new RDD with a subset of the items in the file.

// COMMAND ----------

// Filter all of the lines wihtin the RDD and output the first five rows
val linesWithSpark = textFile.filter(line => line.contains("Spark"))

// COMMAND ----------

// MAGIC %md Notice that this completes quickly because it is a transformation but lacks any action.  
// MAGIC * But when performing the actions below (e.g. count, take) then you will see the executions.

// COMMAND ----------

// Perform a count (action) 
linesWithSpark.count()

// COMMAND ----------

// Filter all of the lines wihtin the RDD and output the first five rows
linesWithSpark.collect().take(5).foreach(println)

// COMMAND ----------

