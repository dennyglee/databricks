// Databricks notebook source exported at Thu, 16 Jun 2016 05:19:10 UTC
// MAGIC %md ## Spark 2.0 Highlights
// MAGIC This notebook is an abridged version of the following excellent resources:
// MAGIC * Reynold Xin's [Apache Spark 2.0: Faster, Easier, and Smarter](http://go.databricks.com/apache-spark-2.0-presented-by-databricks-co-founder-reynold-xin) webinar and associated notebooks
// MAGIC * Michael Armbrust's [Structuring Spark: DataFrames, Datasets, and Streaming](https://spark-summit.org/2016/events/structuring-spark-dataframes-datasets-and-streaming/)
// MAGIC * Tathagata Das' [A Deep Dive into Spark Streaming](https://spark-summit.org/2016/events/a-deep-dive-into-structured-streaming/)
// MAGIC * Joseph Bradley's [Apache Spark MLlib 2.0 Preview: Data Science and Production](https://spark-summit.org/2016/events/apache-spark-mllib-20-preview-data-science-and-production/)
// MAGIC * Tim Hunter's [TensorFrames: Google Tensorflow on Apache Spark]()
// MAGIC 
// MAGIC As well, a good resource is [Getting Started with Apache Spark with Databricks Guide]() and [Apache Spark Key Terms, Explained](http://www.kdnuggets.com/2016/06/spark-key-terms-explained.html)

// COMMAND ----------

// MAGIC %md 
// MAGIC ![](https://databricks.com/wp-content/uploads/2016/06/Major-Features-in-Spark-2.0.png)

// COMMAND ----------

// MAGIC %md ### Dataset and Spark Context
// MAGIC 
// MAGIC ![](https://databricks.com/wp-content/uploads/2016/06/gsasg-apache-spark-2-dataset-api-diagram.png)
// MAGIC 
// MAGIC * The Dataset API provides a type-safe, object-oriented programming interface. 
// MAGIC * In Spark 2.0 DataFrame and Datasets are unified where DataFrame is an alias for an untyped Dataset [Row]. 
// MAGIC * Like DataFrames, Datasets take advantage of:
// MAGIC  * Spark?s Catalyst optimizer by exposing expressions and data fields to a query planner
// MAGIC  * Tungsten's fast in-memory encoding
// MAGIC * Compile-time type safety: meaning production applications can be checked for errors *before* they are run
// MAGIC * Dataset API offers a high-level domain specific language operations like sum(), avg(), join(), select(), groupBy(), making the code a lot easier to express, read, and write.

// COMMAND ----------

// Import org.apache.sql.Dataset
import org.apache.spark.sql.Dataset
implicit class DatasetDisplay(ds: Dataset[_]) {
  def display(): Unit = {
    com.databricks.backend.daemon.driver.EnhancedRDDFunctions.display(ds)
  }
}

// Configure file location
val filepath = "/databricks-datasets/samples/people/people.json"

// COMMAND ----------

// Reading the file (text)
dbutils.fs.head(filepath)

// COMMAND ----------

// Create DataFrame `df` and
// view the data using `display()` command in Databricks
val df = spark.read.json(filepath)  
display(df)

// COMMAND ----------

// MAGIC %md 
// MAGIC ####|| SparkSession
// MAGIC 
// MAGIC Notice that we're no longer using `sqlContext.read...` but instead `spark.read...`
// MAGIC * Entry point for reading data
// MAGIC * Working with metadata
// MAGIC * Configuration
// MAGIC * Cluster resource management

// COMMAND ----------

// Define case class
case class Address(city: String, state: String)
case class People(name: String, address: Address)

// COMMAND ----------

// Declare and view dataset
val ds = spark.read.json(filepath).as[People]
display(ds)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### || Why convert from DataFrame to Dataset?
// MAGIC There are a couple of reasons why you want to convert a DataFrame into a type-specific JVM objects:
// MAGIC 1. Compile-type safety: e.g. using filter operation using wrong data type will detect mismatch instead of runtime error
// MAGIC 2. Dataset API easier to read and develop

// COMMAND ----------

// Use Dataset functions
ds.map(_.name).display()

// COMMAND ----------

// How about if we use the wrong datatype
// With Datasets this query will stop
ds.filter(_.address.city > 140).display()

// COMMAND ----------

// Okay, let's use the right datatype
ds.filter(_.address.city == "Columbus").display()

// COMMAND ----------

// With DataFrames, the query will complete but you would not be aware 
// that you're usign the wrong data type
df.filter(df("name") > 140).display()

// COMMAND ----------

// MAGIC %md ### Performance
// MAGIC Improved performance due to push to `whole-stage code generation`
// MAGIC * No virtual function dispatches: this reduces multiple CPU calls which can have a profound impact on performance when dispatching billions of times.
// MAGIC * Intermediate data in memory vs CPU registers: Tungsten Phase 2 places intermediate data into CPU registers.  This is an order of magnitudes reduction in the number of cycles to obtain data from the CPU registers instead of from memory
// MAGIC * Loop unrolling and SIMD: Optimize Apache Spark?s execution engine to take advantage of modern compilers and CPUs? ability to efficiently compile and execute simple for loops (as opposed to complex function call graphs).
// MAGIC 
// MAGIC Reference the blog: [Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)

// COMMAND ----------

// Define a simple benchmark function for measuring time taken
def benchmark(name: String)(f: => Unit) {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}

// COMMAND ----------

// This config turns off whole stage code generation, effectively changing the execution path to be similar to Spark 1.6.
spark.conf.set("spark.sql.codegen.wholeStage", false)

benchmark("Spark 1.6") {
  spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").count()
}

// COMMAND ----------

// This config turns on whole stage code generation, which is on by default in Spark 2.0.
spark.conf.set("spark.sql.codegen.wholeStage", true)

benchmark("Spark 2.0") {
  spark.range(1000L * 1000 * 1005).join(spark.range(1040L).toDF(), "id").count()
}

// COMMAND ----------

// MAGIC %md ## Structured Streaming
// MAGIC For a great notebook to play with Spark 2.0 and Spark Streaming, check out Michael Armbrust's [2016 Election Tweets](https://docs.cloud.databricks.com/docs/latest/featured_notebooks/2016%20Election%20Tweets.html) notebook.
// MAGIC 
// MAGIC ![](https://databricks.com/wp-content/uploads/2016/06/Shared-Optimization-and-Execution.png)
// MAGIC 
// MAGIC 
// MAGIC * High-level streaming API built on Apache Spark SQL engine
// MAGIC  * Runs the same queries on DataFrames
// MAGIC  * Event time, windowing, sessions, sources & sinks
// MAGIC 
// MAGIC * Unifies streaming, interactive and batch queries
// MAGIC  * Aggregate data in a stream, then serve using JDBC
// MAGIC  * Change queries at runtime
// MAGIC  * Build and apply ML models

// COMMAND ----------

if (org.apache.spark.BuildInfo.sparkBranch < "3.0") sys.error("Just skipping the next set of cells")

// COMMAND ----------

// Batch Aggregation [Example Code]
logs = spark.read.format("json").open("s3://logs")

logs.groupBy(logs.user_id).agg(sum(logs.time))
    .write.format("jdbc")
    .save("jdbc:mysql//...")

// COMMAND ----------

// MAGIC %md To run Continous Aggregations, switch from `.save` to `.stream`.

// COMMAND ----------

// Continous Aggregation [Example Code]
logs = spark.read.format("json").stream("s3://logs")

logs.groupBy(logs.user_id).agg(sum(logs.time))
    .write.format("jdbc")
    .stream("jdbc:mysql//...")

// COMMAND ----------

// MAGIC %md #### || Quote
// MAGIC The simplest way to perform streaming analytics
// MAGIC is not having to reason about streaming.