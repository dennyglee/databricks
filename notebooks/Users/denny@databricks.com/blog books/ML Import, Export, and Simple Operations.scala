// Databricks notebook source exported at Fri, 22 Apr 2016 04:34:14 UTC
// MAGIC %md ## ML Import, Export, and Simple Operations
// MAGIC This notebook provides a quick example of how to do an import, export, and simple operations with a sample LIBSVM file.  This example is based on the blog post [ML Pipelines: A New High-Level API for MLlib](https://databricks.com/blog/2015/01/07/ml-pipelines-a-new-high-level-api-for-mllib.html) updated to Spark 1.6.

// COMMAND ----------

// Remove the labeledPointDF folder
dbutils.fs.rm("/tmp/mllib/labeledPointDF", true)

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

// Load a LIBSVM file into an RDD[LabeledPoint].
val labeledPointRDD: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "/databricks-datasets/samples/data/mllib/sample_libsvm_data.txt")

// Convert from RDD to DF and save it as a Parquet.
//    Note, this step isn't necessary per se to read the DataFrame due to .toDF()
//    Please refer to the last cell that allows you to skip this step
labeledPointRDD.toDF().write.parquet("/tmp/mllib/labeledPointDF")

// Load the Parquet file back into a DataFrame
//    Note, this step isn't necessary per se to read the DataFrame due to .toDF()
//    Please refer to the last cell that allows you to skip this step
val labeledPointDF = sqlContext.read.parquet("/tmp/mllib/labeledPointDF")

// COMMAND ----------

// Collect the feature vectors and print them.
labeledPointDF.select('features).collect().foreach(println)

// NOTE: Another way to do this (which bypasses the saving to / loading from Parquet steps)
// labeledPointRDD.toDF().select('features).collect().foreach(println)

// COMMAND ----------

// You can also use the Databricks Notebook `display` command
display(labeledPointRDD.toDF())