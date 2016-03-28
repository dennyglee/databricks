// Databricks notebook source exported at Mon, 28 Mar 2016 16:00:11 UTC
// MAGIC %md ## Iris dataset (k-means and bisecting k-means)
// MAGIC Reviewing the differences between k-means and bisecting k-means (experimental)

// COMMAND ----------

// MAGIC %md ### Setup Scikit-Learn Datasets
// MAGIC * Source: [Dataset loading utilities](http://scikit-learn.org/stable/datasets/#dataset-loading-utilities)

// COMMAND ----------

// MAGIC %python
// MAGIC # Imports datasets from scikit-learn
// MAGIC from sklearn import datasets, linear_model
// MAGIC from pyspark.mllib.linalg import Vectors
// MAGIC 
// MAGIC def _convert_vec(vec):
// MAGIC   return Vectors.dense([float(x) for x in vec])
// MAGIC 
// MAGIC def convert_bunch(bunch):
// MAGIC   n = len(bunch.data)
// MAGIC   df = sqlContext.createDataFrame([(_convert_vec(bunch.data[i]), float(bunch.target[i])) for i in range(n)])
// MAGIC   return df.withColumnRenamed("_1", "features").withColumnRenamed("_2", "label")
// MAGIC 
// MAGIC diabetes = datasets.load_diabetes()
// MAGIC df = convert_bunch(diabetes)
// MAGIC df.registerTempTable("diabetes")
// MAGIC 
// MAGIC df = convert_bunch(datasets.load_iris())
// MAGIC df.registerTempTable("iris")

// COMMAND ----------

// MAGIC %sql select * from iris

// COMMAND ----------

// MAGIC %md ### Iris dataset: Determine the optimal number of clusters for k-means
// MAGIC * Find the Bend of the Within Set Sum of Squared Errors

// COMMAND ----------

import scala.util.Random
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

val data = sqlContext.sql("select * from iris")
// The MLLib package requires an RDD[Vector] instead of a dataframe. We need to manually extract the vector.
// This is not necessary when using the ml package instead.
val features = data.map(_.getAs[Vector]("features"))

// COMMAND ----------

features.take(10)

// COMMAND ----------

// Remove any existing kmeans results
dbutils.fs.rm("/tmp/denny/kmeans/", true)

// Running multiple iterations of k-means to determine the number of clusters
val numIterations = 20
val OUTPUT_RESULTS = "/tmp/denny/kmeans/"

for (numClusters <- 1 to 15) {
  val clusters = KMeans.train(features, numClusters, numIterations)
  val WSSSE = clusters.computeCost(features)
  println("Clusters: " + numClusters + ", Within Set Sum of Squared Errors = " + WSSSE)
  val result = numClusters + ", " + WSSSE
  val results = sc.parallelize(List(result), 1)
  val OUTPUT_RESULTS_CLUSTER = OUTPUT_RESULTS + numClusters + "/"
  results.coalesce(1, true).saveAsTextFile(OUTPUT_RESULTS_CLUSTER)
}  


// COMMAND ----------

// Obtain WSSSE results and display
case class Results(numClusters: Int, WSSSE: Double)
val results = sc.textFile("/tmp/denny/kmeans/*").map(_.split(",")).map(p => Results(p(0).trim.toInt, p(1).trim.toDouble)).toDF()
results.registerTempTable("results")
display(results.orderBy("numClusters"))

// COMMAND ----------

// MAGIC %md ### Iris Dataset: Optimal number of clusters for k-means = 3 

// COMMAND ----------

val numClusters = 3
val numIterations = 20
// features was initalized above
val clusters = KMeans.train(features, numClusters, numIterations)
val WSSSE = clusters.computeCost(features)

println(s"Compute Cost: ${WSSSE}")
clusters.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
  println(s"Cluster Center ${idx}: ${center}")
}


// COMMAND ----------

display(clusters, data)

// COMMAND ----------

import org.apache.spark.mllib.clustering.BisectingKMeans

// Remove any existing kmeans results
dbutils.fs.rm("/tmp/denny/kmeans/", true)

// Running multiple iterations of bisecting k-means to determine the number of clusters
val numIterations = 40
val OUTPUT_RESULTS = "/tmp/denny/kmeans/"

for (numClusters <- 1 to 15) {
  val bkm = new BisectingKMeans().setK(numClusters).setMaxIterations(numIterations)
  val clusters = bkm.run(features)
  val WSSSE = clusters.computeCost(features)
  println("Clusters: " + numClusters + ", Within Set Sum of Squared Errors = " + WSSSE)
  val result = numClusters + ", " + WSSSE
  val results = sc.parallelize(List(result), 1)
  val OUTPUT_RESULTS_CLUSTER = OUTPUT_RESULTS + numClusters + "/"
  results.coalesce(1, true).saveAsTextFile(OUTPUT_RESULTS_CLUSTER)
}  

// Clustering the data into 3 clusters by BisectingKMeans.
//.val bkm = new BisectingKMeans().setK(3)
//val model = bkm.run(data)


// COMMAND ----------

// Obtain WSSSE results and display
case class Results(numClusters: Int, WSSSE: Double)
val results = sc.textFile("/tmp/denny/kmeans/*").map(_.split(",")).map(p => Results(p(0).trim.toInt, p(1).trim.toDouble)).toDF()
results.registerTempTable("results")
display(results.orderBy("numClusters"))

// COMMAND ----------

// MAGIC %md ### Iris Dataset: Optimal number of clusters for k-means = 3 

// COMMAND ----------

val numClusters = 3
val numIterations = 40
// features was initalized above
val bkm = new BisectingKMeans().setK(numClusters).setMaxIterations(numIterations)
val clusters = bkm.run(features)
val WSSSE = clusters.computeCost(features)

println(s"Compute Cost: ${WSSSE}")
clusters.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
  println(s"Cluster Center ${idx}: ${center}")
}


// COMMAND ----------



// COMMAND ----------

