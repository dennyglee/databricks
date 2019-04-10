# Databricks notebook source
# MAGIC %md ## Quick Start Using Python
# MAGIC * Using a Databricks notebook to showcase RDD operations using Python
# MAGIC * Reference http://spark.apache.org/docs/latest/quick-start.html

# COMMAND ----------

# Take a look at the file system
display(dbutils.fs.ls("/databricks-datasets/samples/docs/"))

# COMMAND ----------

# Setup the textFile RDD to read the README.md file
#   Note this is lazy 
textFile = sc.textFile("/databricks-datasets/samples/docs/README.md")

# COMMAND ----------

# MAGIC %md RDDs have ***actions***, which return values, and ***transformations***, which return pointers to new RDDs.

# COMMAND ----------

# When performing an action (like a count) this is when the textFile is read and aggregate calculated
#    Click on [View] to see the stages and executors
textFile.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Python Count (Job)
# MAGIC ![Python Count - Job](https://sparkhub.databricks.com/wp-content/uploads/2015/12/Python-Count-Job.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Python Count (Stages)
# MAGIC * Notice how the file is read during the *.count()* action
# MAGIC * Many Spark operations are lazy and executed upon some action
# MAGIC 
# MAGIC ![Python Count - Stages](https://sparkhub.databricks.com/wp-content/uploads/2015/12/Python-Count-Stages.png)

# COMMAND ----------

# Output the first line from the text file
textFile.first()

# COMMAND ----------

# MAGIC %md 
# MAGIC Now we're using a filter ***transformation*** to return a new RDD with a subset of the items in the file.

# COMMAND ----------

# Filter all of the lines wihtin the RDD and output the first five rows
linesWithSpark = textFile.filter(lambda line: "Spark" in line)

# COMMAND ----------

# MAGIC %md Notice that this completes quickly because it is a transformation but lacks any action.  
# MAGIC * But when performing the actions below (e.g. count, take) then you will see the executions.

# COMMAND ----------

# Perform a count (action) 
linesWithSpark.count()

# COMMAND ----------

# Filter all of the lines within the RDD and output the first five rows
linesWithSpark.take(5)

# COMMAND ----------

