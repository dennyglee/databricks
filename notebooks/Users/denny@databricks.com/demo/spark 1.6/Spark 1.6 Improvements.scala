// Databricks notebook source exported at Mon, 28 Mar 2016 15:48:48 UTC
// MAGIC %md # What's Coming in Spark 1.6?
// MAGIC This *Scala* notebook contains a number of Spark 1.6 improvements including 
// MAGIC * [SPARK-10000](https://issues.apache.org/jira/browse/SPARK-10000): **Unified Memory Management** - Shared memory for execution and caching instead of exclusive division of the regions.
// MAGIC * [SPARK-11197](https://issues.apache.org/jira/browse/SPARK-11197): **SQL Queries on Files** - Concise syntax for running SQL queries over files of any supported format without registering a table. Example Usage.
// MAGIC * [SPARK-9999](https://issues.apache.org/jira/browse/SPARK-9999): **Datasets API** - A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten). 
// MAGIC * [Streaming] [SPARK-2629](https://issues.apache.org/jira/browse/SPARK-2629): **New improved state management** - trackStateByKey - a DStream transformation for stateful stream processing, supercedes updateStateByKey in functionality and performance.
// MAGIC * [Streaming] [SPARK-10885 PR#8950](https://github.com/apache/spark/pull/8950): **Display the failed output op in Streaming**
// MAGIC * [SPARK-4849](https://issues.apache.org/jira/browse/SPARK-4849): **Advanced Layout of Cached Data** - storing partitioning and ordering schemes in In-memory table scan, and adding distributeBy and localSort to DF API.
// MAGIC * [MLlib] [SPARK-6725](https://issues.apache.org/jira/browse/SPARK-6725): **Pipeline persistence**  - Save/load for ML Pipelines, with partial coverage of spark.ml algorithms.
// MAGIC * [SPARK-9836](https://issues.apache.org/jira/browse/SPARK-9836): **R-like Statistics for GLMs** - (Partial) R-like stats for ordinary least squares via summary(model)
// MAGIC  * *Open a new tab* for the [Spark 1.6 R Improvements Notebook](https://dogfood.staging.cloud.databricks.com/#notebook/2170925) or [Spark 1.6 R Improvements Exported HTML Notebook](http://bit.ly/1OBkjMM)
// MAGIC  
// MAGIC List of various other Spark 1.6 Improvements by Category
// MAGIC * Performance
// MAGIC * Spark SQL
// MAGIC * Spark Streaming
// MAGIC * MLlib
// MAGIC * More information
// MAGIC 
// MAGIC Other Miscellaneous examples include:
// MAGIC * [SPARK-11745](https://issues.apache.org/jira/browse/SPARK-11745): **Reading non-standard JSON files** - Added options to read non-standard JSON files (e.g. single-quotes, unquoted attributes)
// MAGIC * [SPARK-10142](https://issues.apache.org/jira/browse/SPARK-10412): **Per-operator Metrics for SQL Execution** - Display statistics on a peroperator basis for memory usage and spilled data size.
// MAGIC * [SPARK-11111](https://issues.apache.org/jira/browse/SPARK-11111): **Fast null-safe joins** - Joins using null-safe equality (<=>) will now execute using SortMergeJoin instead of computing a cartisian product
// MAGIC * [SPARK-8518](https://issues.apache.org/jira/browse/SPARK-8518): **Survival analysis** - Log-linear model for survival analysis

// COMMAND ----------



// COMMAND ----------

// MAGIC %md # Major Improvements

// COMMAND ----------

// MAGIC %md ## SPARK-10000: Unified Memory Management
// MAGIC Shared memory for execution and caching instead of exclusive division of the regions.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Memory Management in Spark: < 1.5
// MAGIC * Two separate memory managers: 
// MAGIC  * Execution memory: computation of shuffles, joins, sorts, aggregations
// MAGIC  * Storage memory: caching and propagating internal data sources across cluster
// MAGIC * Issues with this:
// MAGIC  * Manual intervention to avoid unnecessary spilling
// MAGIC  * No pre-defined defaults for all workloads
// MAGIC  * Need to partition the execution (shuffle) memory and cache memory fractions
// MAGIC * Goal: Unify these two memory regions and borrow from each other	

// COMMAND ----------

// MAGIC %md
// MAGIC ### Unified Memory Management in Spark 1.6
// MAGIC * Can cross between execution and storage memory
// MAGIC  * When execution memory exceeds its own region, it can borrow as much of the storage space as is free and vice versa
// MAGIC  * Borrowed storage memory can be evicted at any time (though not execution memory at this time)
// MAGIC * New configurations to be introduced
// MAGIC * Under memory pressure
// MAGIC  * Evict cached data
// MAGIC  * Evict storage memory
// MAGIC  * Evict execution memory
// MAGIC * Notes:
// MAGIC  * Dynamically allocated reserved storage region that exection memory cannot borrow from
// MAGIC  * Cached data evicted only if actual storage exceeds the dynamically allocated reserved storage region
// MAGIC * Result:
// MAGIC  * Allows for better memory management for multi-tenancy and applications relying heavily on caching
// MAGIC  * No cap on storage memory nor on execution memory
// MAGIC  * Dynamic allocation of reserved storage memory will not require user configuration
// MAGIC 
// MAGIC  
// MAGIC  

// COMMAND ----------

// Execute reduceByKey that will cause spill (DBC cluster with 3 nodes, 90GB RAM)
val result = sc.parallelize(0 until 200000000).map { i => (i / 2, i) }.reduceByKey(math.max).collect()

// COMMAND ----------

// MAGIC %md #### Review Stage Details for reduceByKey job
// MAGIC * Spark 1.6 completes faster than the same size Spark 1.5 cluster
// MAGIC * Note the Spark 1.5 spills to memory and disk
// MAGIC 
// MAGIC ![reduceByKey Spark 1.6 run](https://sparkhub.databricks.com/wp-content/uploads/2015/11/SPARK-10000-1.6-e1448833785393.png)
// MAGIC ![reduceByKey Spark 1.5 run](https://sparkhub.databricks.com/wp-content/uploads/2015/11/SPARK-10000-1.5-e1448833729236.png)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## SPARK-11197: Run SQL queries directly on files
// MAGIC Starting with Spark 1.6, you can run SQL queries directly on files without needing to first create a table pointing to the files.

// COMMAND ----------

// MAGIC %md ### Setup the data
// MAGIC * Create a two line file with the following text into the file system

// COMMAND ----------

dbutils.fs.put("/home/pwendell/1.6/lines","""
Hello hello is it me you are looking for
Hello how are you
""", true)

// COMMAND ----------

// MAGIC %md 
// MAGIC ![Lionel Richie Adele "Hello" meme](http://www.b365.ro/media/image/201510/w620/adele_lionel_ritchie_hello_64012700.png) 

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- View the data
// MAGIC SELECT * FROM text.`/home/pwendell/1.6/lines` WHERE value != ""

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## SPARK-9999: Dataset API 
// MAGIC A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten). 
// MAGIC 
// MAGIC “Encoder” converts from JVM Object into a Dataset Row
// MAGIC 
// MAGIC ![JVM Object to Dataset Row](https://sparkhub.databricks.com/wp-content/uploads/2015/12/Encoder-JVM-Object-to-Dataset-Row.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC ![DataFrame and Dataset](https://sparkhub.databricks.com/wp-content/uploads/2015/12/DataFrame_and_Dataset.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### WordCount Example
// MAGIC A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten)
// MAGIC * In the example below, we will take lines of text and split them up into words. 
// MAGIC * Next, we count the number of occurances of each work in the set using a variety of Spark API.

// COMMAND ----------

// Import the SQL Functions
import org.apache.spark.sql.functions._

// Load DataFrame
val df = sqlContext.read.text("/home/pwendell/1.6/lines")

// Interpret each line as a java.lang.String (using built-in encoder)
val ds = df.as[String]
.flatMap(line => line.split(" "))               // Split on whitespace
.filter(line => line != "")                     // Filter empty words
.groupBy(word => word)
.count()

display(ds.toDF)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md 
// MAGIC ### People JSON Example
// MAGIC A type-safe API (similar to RDDs) that performs many operations on serialized binary data and code generation (i.e. Project Tungsten)
// MAGIC * In the example below, we will create a JSON file  
// MAGIC * Next, we will filter and calculate the average age of the people in the JSON file

// COMMAND ----------

// Create people.json file 
dbutils.fs.put("/home/pwendell/1.6/people.json","""
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
{"name":"Michael", "age":26}
""", true)

// COMMAND ----------

// Create Person class, dataframe and dataset
import org.apache.spark.sql.Dataset

// Establish case class
case class Person(name: String, age: Long)

// Read JSON and establish dataset
val peopledf = sqlContext.read.json("/home/pwendell/1.6/people.json")
val peopleds: Dataset[Person] = peopledf.as[Person]

// COMMAND ----------

// MAGIC %md
// MAGIC Why a *Long* datatype even though the output is *bigint*?
// MAGIC * In HiveQL, *bigint* is backed by *Long*.
// MAGIC * Which is different from a Scala *bigint* and Java *bigint*
// MAGIC * Therefore, when in **schema**, when you see *bigint*, it is actually *Long* type.
// MAGIC * More information at [SPARK-11856] SQL add type cast if the real type is different but compatible with encoder schema [#9840](https://github.com/apache/spark/pull/9840)
// MAGIC 
// MAGIC Thanks to Yin Huai for the clarification!

// COMMAND ----------

// Filtering the dataset
peopleds.filter(p => p.name.startsWith("M")).collect.foreach(println)

// COMMAND ----------

// Filter the dataset, groupBy name, and calculate average
val peopleDS_startsWithM = peopleds.filter(p => p.name.startsWith("M"))
.toDF()
.groupBy($"name")
.avg("age")

// Display output
display(peopleDS_startsWithM)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## [Streaming] SPARK-2629: New improved state management
// MAGIC * Introducing a DStream transformation for stateful stream processing
// MAGIC  * Does not scan every key
// MAGIC  * Easier to implement common use cases 
// MAGIC    * timeout of idle data
// MAGIC    * returning items other than state
// MAGIC * Supercedes updateStateByKey in functionality and performance.
// MAGIC * trackStateByKey (note, this name may change)

// COMMAND ----------

// Code Snippet
// Execute the full code at: https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/StatefulNetworkWordCount.scala
StreamingExamples.setStreamingLogLevels()

val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount")

// Create the context with a 1 second batch size
val ssc = new StreamingContext(sparkConf, Seconds(1))
ssc.checkpoint(".")

// Initial RDD input to trackStateByKey
val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

// Create a ReceiverInputDStream on target ip:port and count the
// words in input stream of \n delimited test (eg. generated by 'nc')
val lines = ssc.socketTextStream(args(0), args(1).toInt)
val words = lines.flatMap(_.split(" "))
val wordDstream = words.map(x => (x, 1))

// Update the cumulative count using updateStateByKey
// This will give a DStream made of state (which is the cumulative count of the words)
val trackStateFunc = (batchTime: Time, word: String, one: Option[Int], state: State[Int]) => {
  val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
  val output = (word, sum)
  state.update(sum)
  Some(output)
}

val stateDstream = wordDstream.trackStateByKey(
  StateSpec.function(trackStateFunc).initialState(initialRDD))

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## [Streaming] SPARK-10885 PR#8950: Display the failed output op in Streaming 
// MAGIC * Display the failed output op count in the batch list
// MAGIC * Display the failure reason of output op in the batch detail page
// MAGIC 
// MAGIC ![Display Streaming Failed Output](https://sparkhub.databricks.com/wp-content/uploads/2015/12/Display-Failed-Output-Streaming.png)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## SPARK-4849: Advanced Layout of Cached Data
// MAGIC * Storing partitioning and ordering schemes in In-memory table scan
// MAGIC  * Allows for performance improvements: e.g. in Joins, an extra partition step can be saved based on this information
// MAGIC * Adding distributeBy and localSort to DF API
// MAGIC  * Similar to HiveQL’s DISTRIBUTE BY
// MAGIC  * Allows the user to control the partitioning and ordering of a data set 

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ## [MLlib] SPARK-6725: Pipeline persistence
// MAGIC Save/load for ML Pipelines, with partial coverage of spark.ml algorithms.
// MAGIC * Persist ML Pipelines to:
// MAGIC   * Save models in the spark.ml API
// MAGIC   * Re-run workflows in a reproducible manner
// MAGIC   * Export models to non-Spark apps (e.g., model server)
// MAGIC * This is more complex than ML model persistence because:
// MAGIC   * Must persist Transformers and Estimators, not just Models.
// MAGIC   * We need a standard way to persist Params.
// MAGIC   * Pipelines and other meta-algorithms can contain other Transformers and Estimators, including as Params.
// MAGIC   * We should save feature metadata with Models
// MAGIC   
// MAGIC Specifically, this jira:
// MAGIC * Adds model export/import to the spark.ml API. 
// MAGIC * Adds the internal Saveable/Loadable API and Parquet-based format
// MAGIC 
// MAGIC ![SPARK-6725 Subtasks](https://sparkhub.databricks.com/wp-content/uploads/2015/12/SPARK-6725-subtasks.png)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## SPARK-9836: R-like Statistics for GLMs
// MAGIC Provide R-like summary statistics for ordinary least squares via normal equation solver
// MAGIC  * *Open a new tab* for the [Spark 1.6 R Improvements Notebook](https://dogfood.staging.cloud.databricks.com/#notebook/2170925) or [Spark 1.6 R Improvements Exported HTML Notebook](http://bit.ly/1OBkjMM)
// MAGIC  

// COMMAND ----------



// COMMAND ----------

// MAGIC %md # List of Spark 1.6 Improvements by Category

// COMMAND ----------

// MAGIC %md ## Performance
// MAGIC * [SPARK-10000](https://issues.apache.org/jira/browse/SPARK-10000): Unified Memory Management - Shared memory for execution and caching instead of exclusive division of the regions.
// MAGIC * [SPARK-10917](https://issues.apache.org/jira/browse/SPARK-10917), [SPARK-11149](https://issues.apache.org/jira/browse/SPARK-11149): In-memory Columnar Cache Performance - Significant (up to 14x) speed up when caching data that contains complex types in DataFrames or SQL.
// MAGIC * [SPARK-11389](https://issues.apache.org/jira/browse/SPARK-11389):SQL Execution Using Off-Heap Memory - Support for configuring query execution to occur using off-heap memory to avoid GC overhead
// MAGIC * [SPARK-4849](https://issues.apache.org/jira/browse/SPARK-4849): Advanced Layout of Cached Data - storing partitioning and ordering schemes in In-memory table scan, and adding distributeBy and localSort to DF API
// MAGIC * [SPARK-9858](https://issues.apache.org/jira/browse/SPARK-9598): Adaptive query execution - Initial support for automatically selecting the number of reducers for joins and aggregations.
// MAGIC * [SPARK-11787](https://issues.apache.org/jira/browse/SPARK-11787): Parquet Performance - Improve Parquet scan performance when using flat schemas.

// COMMAND ----------

// MAGIC %md ## Spark SQL
// MAGIC * [SPARK-9999](https://issues.apache.org/jira/browse/SPARK-9999)  Dataset API  
// MAGIC * [SPARK-11197](https://issues.apache.org/jira/browse/SPARK-11197) SQL Queries on Files
// MAGIC * [SPARK-11745](https://issues.apache.org/jira/browse/SPARK-11745) Reading non-standard JSON files
// MAGIC * [SPARK-10412](https://issues.apache.org/jira/browse/SPARK-10412) Per-operator Metrics for SQL Execution
// MAGIC * [SPARK-11329](https://issues.apache.org/jira/browse/SPARK-11329) Star (*) expansion for StructTypes
// MAGIC * [SPARK-11111](https://issues.apache.org/jira/browse/SPARK-11111) Fast null-safe joins
// MAGIC * [SPARK-10978](https://issues.apache.org/jira/browse/SPARK-10978) Datasource API Avoid Double Filter 

// COMMAND ----------

// MAGIC %md ## Spark Streaming
// MAGIC **API Updates**
// MAGIC * [SPARK-2629](https://issues.apache.org/jira/browse/SPARK-2629)  New improved state management
// MAGIC * [SPARK-11198](https://issues.apache.org/jira/browse/SPARK-11198) Kinesis record deaggregation
// MAGIC * [SPARK-10891](https://issues.apache.org/jira/browse/SPARK-10891) Kinesis message handler function
// MAGIC * [SPARK-6328](https://issues.apache.org/jira/browse/SPARK-6328)  Python Streamng Listener API
// MAGIC 
// MAGIC **UI Improvements**
// MAGIC * Made failures visible in the streaming tab, in the timelines, batch list, and batch details page.
// MAGIC * Made output operations visible in the streaming tab as progress bars

// COMMAND ----------

// MAGIC %md ## MLlib
// MAGIC ** New algorithms / models **
// MAGIC * [SPARK-8518](https://issues.apache.org/jira/browse/SPARK-8518)  Survival analysis - Log-linear model for survival analysis
// MAGIC * [SPARK-9834](https://issues.apache.org/jira/browse/SPARK-9834)  Normal equation for least squares - Normal equation solver, providing R-like model summary statistics
// MAGIC * [SPARK-3147](https://issues.apache.org/jira/browse/SPARK-3147)  Online hypothesis testing - A/B testing in the Spark Streaming framework
// MAGIC * [SPARK-9930](https://issues.apache.org/jira/browse/SPARK-9930)  New feature transformers - ChiSqSelector, QuantileDiscretizer, SQL transformer
// MAGIC * [SPARK-6517](https://issues.apache.org/jira/browse/SPARK-6517)  Bisecting K-Means clustering - Fast top-down clustering variant of K-Means
// MAGIC 
// MAGIC ** API Improvements **
// MAGIC * ML Pipelines
// MAGIC  * [SPARK-6725](https://issues.apache.org/jira/browse/SPARK-6725)  Pipeline persistence - Save/load for ML Pipelines, with partial coverage of spark.ml algorithms
// MAGIC  * [SPARK-5565](https://issues.apache.org/jira/browse/SPARK-5565)  LDA in ML Pipelines - API for Latent Dirichlet Allocation in ML Pipelines
// MAGIC * R API
// MAGIC  * [SPARK-9836](https://issues.apache.org/jira/browse/SPARK-9836)  R-like statistics for GLMs - (Partial) R-like stats for ordinary least squares via summary(model)
// MAGIC  * [SPARK-9681](https://issues.apache.org/jira/browse/SPARK-9681)  Feature interactions in R formula - Interaction operator ":" in R formula
// MAGIC * Python API - Many improvements to Python API to approach feature parity
// MAGIC 
// MAGIC ** Miscellaneous Improvements **
// MAGIC * [SPARK-7685](https://issues.apache.org/jira/browse/SPARK-7685) , [SPARK-9642](https://issues.apache.org/jira/browse/SPARK-9642)  Instance weights for GLMs - Logistic and Linear Regression can take instance weights
// MAGIC * [SPARK-10384](https://issues.apache.org/jira/browse/SPARK-10384), [SPARK-10385](https://issues.apache.org/jira/browse/SPARK-10385) Univariate and bivariate statistics in DataFrames - Variance, stddev, correlations, etc.
// MAGIC * [SPARK-10117](https://issues.apache.org/jira/browse/SPARK-10117) LIBSVM data source - LIBSVM as a SQL data source

// COMMAND ----------

// MAGIC %md ## More Information
// MAGIC * [Apache Spark 1.6.0 Release Preview](http://bit.ly/1Ilk9da)
// MAGIC * [Spark 1.6 Preview available in Databricks](http://bit.ly/1QR0RPC)
// MAGIC * [Spark 1.6 Improvements Notebook (Exported HTML)](http://bit.ly/1lrvdLc)
// MAGIC * [Spark 1.6 R Improvements Notebook (Exported HTML)](http://bit.ly/1OBkjMM)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md # Miscellaneous Examples

// COMMAND ----------

// MAGIC %md ## SPARK-11745: Enable more JSON parsing options for parsing non-standard JSON files
// MAGIC Starting with Spark 1.6, you can read non-standard JSON files including single quotes and unquoted attributes

// COMMAND ----------

//
// Create a non-standard people.json file 
//   - Using single quotes (instead of double quotes)
//   - Include Java / C++ comments
//   - Using numerics with leading zeros
//
dbutils.fs.put("/home/pwendell/1.6/people-nonstandard.json","""
{'name':'Michael', 'desc':/* Hide Comment */ '#SparkRocks'}
{'name':'Andy', 'age':0030, 'desc':'#Plutoflyby'}
{'name':'Justin', 'age':0019}
""", true)

// COMMAND ----------

// 
// Read non-standard people.json file
//   - Specify options to allowComments and allowNumericLeadingZeros to true
//   - By default, allowSingleQuotes is true
//
val people_ns = sqlContext.read.option("allowNumericLeadingZeros", "true").option("allowComments", "true").json("/home/pwendell/1.6/people-nonstandard.json")

// Register Temp Table
people_ns.registerTempTable("people_ns")

// COMMAND ----------

// SQL Query using Scala
sqlContext.sql("SELECT name, age, desc from people_ns").collect.foreach(println)


// COMMAND ----------

// MAGIC %sql SELECT name, age, desc from people_ns

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## SPARK-10142: Per-operator Metrics for SQL Execution
// MAGIC Display statistics on a peroperator basis for memory usage and spilled data size.

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(1) from people_ns where age > 0

// COMMAND ----------

// MAGIC %md 
// MAGIC To view the *per-operator metrics* 
// MAGIC * Click on ***View*** under *Spark Jobs* for your SQL statement above
// MAGIC * Click the ***SQL*** tab
// MAGIC * Click the ***take at OutputAggregator.scala:NN***
// MAGIC * Expand the cell below to take a veiw at the screenshot

// COMMAND ----------

// MAGIC %md ![SPARK-10142 Screenshot](https://sparkhub.databricks.com/wp-content/uploads/2015/11/SPARK-10142_screenshot-e1448833882479.png)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## SPARK-11111: Fast null-safe join
// MAGIC Prior to Spark 1.6, null safe joins are executed with a Cartesian product. As of Spark 1.6, joins with only null safe equality should not result in a Cartesian product.

// COMMAND ----------

sqlContext.sql("select * from people_ns a join people b on (a.age <=> b.age)").explain

// COMMAND ----------

// MAGIC %md Below is the Spark 1.5 plan
// MAGIC 
// MAGIC ![Spark 1.5 Null Cartesian Product](https://sparkhub.databricks.com/wp-content/uploads/2015/11/Spark_1.5_null_cartesian_join-e1448833412690.png)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ## SPARK-8518: Survival analysis - Log-linear model for survival analysis
// MAGIC As part of Spark 1.6, the [Accelerated failure time (AFT)](https://en.wikipedia.org/wiki/Accelerated_failure_time_model) model which is a parametric survival regression model (also known as log of survival time, also known as log-linear model survival analysis) for censored data has been included as part of spark.ml. Note, this is different from [Proportional hazards](https://en.wikipedia.org/wiki/Proportional_hazards_model) model designed for the same purpose, the AFT model is more easily to parallelize because each instance contribute to the objective function independently.

// COMMAND ----------

import org.apache.spark.ml.regression.AFTSurvivalRegression
import org.apache.spark.mllib.linalg.Vectors

val training = sqlContext.createDataFrame(Seq(
  (1.218, 1.0, Vectors.dense(1.560, -0.605)),
  (2.949, 0.0, Vectors.dense(0.346, 2.158)),
  (3.627, 0.0, Vectors.dense(1.380, 0.231)),
  (0.273, 1.0, Vectors.dense(0.520, 1.151)),
  (4.199, 0.0, Vectors.dense(0.795, -0.226))
)).toDF("label", "censor", "features")
val quantileProbabilities = Array(0.3, 0.6)
val aft = new AFTSurvivalRegression()
  .setQuantileProbabilities(quantileProbabilities)
  .setQuantilesCol("quantiles")

val model = aft.fit(training)

// Print the coefficients, intercept and scale parameter for AFT survival regression
println(s"Coefficients: ${model.coefficients} Intercept: " +
  s"${model.intercept} Scale: ${model.scale}")
model.transform(training).show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC For more information, please refer to:
// MAGIC * [SPARK-8518](https://issues.apache.org/jira/browse/SPARK-8518): Log-linear models for survival analysis
// MAGIC * [ML - Survival Regression](https://people.apache.org/~pwendell/spark-nightly/spark-1.6-docs/latest/ml-survival-regression.html) (pre-release Apache Spark 1.6 documentation)
// MAGIC * The full [AFTSurvivalRegressionExample.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/AFTSurvivalRegressionExample.scala) example 

// COMMAND ----------

