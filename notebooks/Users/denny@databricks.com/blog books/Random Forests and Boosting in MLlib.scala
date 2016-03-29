// Databricks notebook source exported at Tue, 29 Mar 2016 04:25:26 UTC
// MAGIC %md ## Random Forests and Boosting in MLlib

// COMMAND ----------

// MAGIC %md We demonstrate how to learn ensemble models using MLlib. The following Scala examples show how to read in a dataset, split the data into training and test sets, learn a model, and print the model and its test accuracy. Refer to the [MLlib Programming Guide](http://spark.apache.org/docs/latest/mllib-ensembles.html) for examples in Java and Python. Note that GBTs do not yet have a Python API, but we expect it to be in the Spark 1.3 release (via [Github PR 3951](https://www.github.com/apache/spark/pull/3951)).

// COMMAND ----------

// MAGIC %md ### Random Forest Example

// COMMAND ----------

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.util.MLUtils
 
// Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "/databricks-datasets/samples/data/mllib/sample_libsvm_data.txt")

// Split data into training/test sets
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))
 
// Train a RandomForest model.
val treeStrategy = Strategy.defaultStrategy("Classification")
val numTrees = 3 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val model = RandomForest.trainClassifier(trainingData, treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
 
// Evaluate model on test instances and compute test error
val testErr = testData.map { point =>
  val prediction = model.predict(point.features)
  if (point.label == prediction) 1.0 else 0.0
}.mean()
println("Test Error = " + testErr)
println("Learned Random Forest:n" + model.toDebugString)


// COMMAND ----------

// MAGIC %md ### Gradient-Boosted Trees Example

// COMMAND ----------

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
 
// Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "/databricks-datasets/samples/data/mllib/sample_libsvm_data.txt")

// Split data into training/test sets
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))
 
// Train a GradientBoostedTrees model.
val boostingStrategy = BoostingStrategy.defaultParams("Classification")
boostingStrategy.numIterations = 3 // Note: Use more in practice
val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
 
// Evaluate model on test instances and compute test error
val testErr = testData.map { point =>
  val prediction = model.predict(point.features)
  if (point.label == prediction) 1.0 else 0.0
}.mean()
println("Test Error = " + testErr)
println("Learned GBT model:n" + model.toDebugString)