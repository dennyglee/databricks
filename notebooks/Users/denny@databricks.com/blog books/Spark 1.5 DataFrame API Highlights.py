# Databricks notebook source exported at Mon, 28 Mar 2016 17:05:51 UTC
# MAGIC %md ## Spark 1.5 DataFrame API Highlights: Date/Time/String Handling, Time Intervals, and UDAFs
# MAGIC This notebook highlights three major improvements to DataFrame API in Spark 1.5, which are:
# MAGIC 
# MAGIC * New built-in functions;
# MAGIC * Time intervals; and
# MAGIC * Experimental user-defined aggregation function (UDAF) interface.

# COMMAND ----------

# MAGIC %md ### round function
# MAGIC `round` is a function that rounds a numeric value to the specified precision. When the given precision is a positive number, a given input numeric value is rounded to the decimal position specified by the precision. When the specified precision is a zero or a negative number, a given input numeric value is rounded to the position of the integral part specified by the precision.

# COMMAND ----------

# Create a simple DataFrame
data = [
  (234.5, "row1"),
  (23.45, "row2"),
  (2.345, "row3"),
  (0.2345, "row4")]
df = sqlContext.createDataFrame(data, ["i", "j"])
 
# Import functions provided by Spark?s DataFrame API
from pyspark.sql.functions import *
 
# Call round function directly
display(df.select(
  df['i'],
  round(df['i'], 1),
  round(df['i'], 0),
  round(df['i'], -1)))

# COMMAND ----------

# MAGIC %md #### SQL Syntax
# MAGIC 
# MAGIC You can also use SQL syntax.

# COMMAND ----------

# Register Temp Table for SQL Queries
df.registerTempTable("df")

# COMMAND ----------

# MAGIC %sql select i, round(i, 1) as `round(i,1)`, round(i, 0) as `round(i,0)`, round(i, -1) as `round(i, -1)` from df

# COMMAND ----------

# MAGIC %md #### Mix-and-Match
# MAGIC 
# MAGIC You can even mix and match SQL syntax with DataFrame operations by using the `expr` function. By using `expr`, you can construct a DataFrame column expression from a SQL expression String.

# COMMAND ----------

display(df.select(
  expr("round(i, 1) AS rounded1"),
  expr("round(i, 0) AS rounded2"),
  expr("round(i, -1) AS rounded3")))

# COMMAND ----------

# MAGIC %md ### Time Interval Literals
# MAGIC As part of Spark 1.5, a new feature is a new data type, interval, that allows developers to represent fixed periods of time (i.e. 1 day or 2 months) as interval literals.  Using interval literals, it is possible to perform subtraction or addition of an arbitrary amount of time from a date or timestamp value. This representation can be useful when you want to add or subtract a time period from a fixed point in time. For example, users can now easily express queries like **?Find all transactions that have happened during the past hour?**.

# COMMAND ----------

# Import functions.
from pyspark.sql.functions import *
 
# Create a simple DataFrame.
data = [
  ("2015-01-01 23:59:59", "2015-01-02 00:01:02", 1),
  ("2015-01-02 23:00:00", "2015-01-02 23:59:59", 2),
  ("2015-01-02 22:59:58", "2015-01-02 23:59:59", 3)]
df = sqlContext.createDataFrame(data, ["start_time", "end_time", "id"])
df = df.select(
  df.start_time.cast("timestamp").alias("start_time"),
  df.end_time.cast("timestamp").alias("end_time"),
  df.id)
 
# Get all records that have a start_time and end_time in the
# same day, and the difference between the end_time and start_time
# is less or equal to 1 hour.
condition = \
  (to_date(df.start_time) == to_date(df.end_time)) & \
  (df.start_time + expr("INTERVAL 1 HOUR") >= df.end_time)

# display data within DataFrame using the condition 
display(df.filter(condition))

# COMMAND ----------

# MAGIC %md ## User-defined Aggregate Function Interface
# MAGIC For power users, Spark 1.5 introduces an experimental API for user-defined aggregate functions (UDAFs). These UDAFs can be used to compute custom calculations over groups of input data (in contrast, UDFs compute a value looking at a single input row), such as calculating geometric mean or calculating the product of values for every group.

# COMMAND ----------

# MAGIC %md Below is an example UDAF implemented in Scala that calculates the [geometric mean](https://en.wikipedia.org/wiki/Geometric_mean) of the given set of double values. The geometric mean can be used as an indicator of the typical value of an input set of numbers by using the product of their values (as opposed to the standard builtin mean which is based on the sum of the input values). For the purpose of simplicity, null handling logic is not shown in the following code.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.expressions.MutableAggregationBuffer
# MAGIC import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
# MAGIC import org.apache.spark.sql.Row
# MAGIC import org.apache.spark.sql.types._
# MAGIC  
# MAGIC class GeometricMean extends UserDefinedAggregateFunction {
# MAGIC   def inputSchema: org.apache.spark.sql.types.StructType =
# MAGIC     StructType(StructField("value", DoubleType) :: Nil)
# MAGIC  
# MAGIC   def bufferSchema: StructType = StructType(
# MAGIC     StructField("count", LongType) ::
# MAGIC     StructField("product", DoubleType) :: Nil
# MAGIC   )
# MAGIC  
# MAGIC   def dataType: DataType = DoubleType
# MAGIC  
# MAGIC   def deterministic: Boolean = true
# MAGIC  
# MAGIC   def initialize(buffer: MutableAggregationBuffer): Unit = {
# MAGIC     buffer(0) = 0L
# MAGIC     buffer(1) = 1.0
# MAGIC   }
# MAGIC  
# MAGIC   def update(buffer: MutableAggregationBuffer,input: Row): Unit = {
# MAGIC     buffer(0) = buffer.getAs[Long](0) + 1
# MAGIC     buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
# MAGIC   }
# MAGIC  
# MAGIC   def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
# MAGIC     buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
# MAGIC     buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
# MAGIC   }
# MAGIC  
# MAGIC   def evaluate(buffer: Row): Any = {
# MAGIC     math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md A UDAF can be used in two ways. First, an instance of a UDAF can be used immediately as a function. 

# COMMAND ----------

# MAGIC %scala 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC // Create a simple DataFrame with a single column called "id"
# MAGIC // containing number 1 to 10.
# MAGIC val df = sqlContext.range(1, 11)
# MAGIC  
# MAGIC // Create an instance of UDAF GeometricMean.
# MAGIC val gm = new GeometricMean
# MAGIC  
# MAGIC // Show the geometric mean of values of column "id".
# MAGIC //df.groupBy().agg(gm(col("id")).as("GeometricMean")).show()
# MAGIC display(df.groupBy().agg(gm(col("id")).as("GeometricMean")))

# COMMAND ----------

# MAGIC %md Second, users can register a UDAF to Spark SQL?s function registry and call this UDAF by the assigned name.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Register the UDAF and call it "gm".
# MAGIC sqlContext.udf.register("gm", gm)
# MAGIC 
# MAGIC // Invoke the UDAF by its assigned name.
# MAGIC // df.groupBy().agg(expr("gm(id) as GeometricMean")).show()
# MAGIC display(df.groupBy().agg(expr("gm(id) as GeometricMean")))