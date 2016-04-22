// Databricks notebook source exported at Fri, 22 Apr 2016 04:55:51 UTC
// MAGIC %md ## Scalable Decision Trees with MLlib
// MAGIC This notebook supports the blog post [Scalable Decision Trees in MLlib](https://databricks.com/blog/2014/09/29/scalable-decision-trees-in-mllib.html) in Scala (the original blog post was in Python).  It also adds the Visualizing Decision Trees using the Databricks `display` command.

// COMMAND ----------

// MAGIC %md ### Decision Trees with MLlib Example (in Scala)
// MAGIC Load the parse the sample data, train a DecisionTree model, evaluate it, and save the model.

// COMMAND ----------

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

// Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "/databricks-datasets/samples/data/mllib/sample_libsvm_data.txt")

// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification tree model:\n" + model.toDebugString)

// COMMAND ----------

// MAGIC %md ### Visualize the DecisionTree
// MAGIC Using the same source sample data, convert to DataFrame, use the StringIndexer and DecisionTreeClassifier Model to visualize the decision tree using the `display` command.

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.DecisionTreeClassifier

// Convert the trainingData RDD to DataFrame
val df = trainingData.toDF()

val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .fit(df)
val data2 = labelIndexer.transform(df)

// Create DecisionTreeClassifier model based on this dataset
val dt = new DecisionTreeClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("features")
val dtModel = dt.fit(data2)

// Display the model
display(dtModel)