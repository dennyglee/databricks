// Databricks notebook source exported at Mon, 28 Mar 2016 15:59:38 UTC
// MAGIC %md ## Images dataset (k-means and bisecting k-means)
// MAGIC Reviewing the differences between k-means and bisecting k-means (experimental)

// COMMAND ----------

// MAGIC %md ### Setup Scikit-Learn Datasets
// MAGIC * Source: [Dataset loading utilities](http://scikit-learn.org/stable/datasets/#dataset-loading-utilities)

// COMMAND ----------

// MAGIC %python
// MAGIC # Imports datasets from scikit-learn
// MAGIC from sklearn import datasets, linear_model
// MAGIC from pyspark.mllib.linalg import Vectors
// MAGIC import numpy as np
// MAGIC 
// MAGIC def numpy_to_df(self, **kwargs):
// MAGIC        """ Converts a set of numpy arrays into a single dataframe.
// MAGIC  
// MAGIC        The argument name is used to infer the name of the column. The value of the argument is a
// MAGIC        numpy array with a shape of length 0, 1, or 2. The dtype is one of the data types supported
// MAGIC        by sparkSQL. This includes np.double, np.float, np.int and np.long.
// MAGIC        See the whole list of supported types in the Spark SQL documentation:
// MAGIC        http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types
// MAGIC  
// MAGIC        The columns may not be in the order they are provided. Each column needs to have the same
// MAGIC        number of elements.
// MAGIC  
// MAGIC        :return: A pyspark.sql.DataFrame object, or raises a ValueError if the data type could not
// MAGIC        be understood.
// MAGIC  
// MAGIC        Example:
// MAGIC  
// MAGIC        >>> X = np.zeros((10,4))
// MAGIC        >>> y = np.ones(10)
// MAGIC        >>> df = conv.numpy_to_df(x = X, y = y)
// MAGIC        >>> df.printSchema()
// MAGIC        root
// MAGIC         |-- y: double (nullable = true)
// MAGIC         |-- x: vector (nullable = true)
// MAGIC        """
// MAGIC        def convert(z):
// MAGIC          if len(z.shape) == 1:
// MAGIC            return z.tolist()
// MAGIC          if len(z.shape) == 2:
// MAGIC            return [Vectors.dense(row) for row in z.tolist()]
// MAGIC          raise ValueError("Cannot convert a numpy array with more than 2 dimensions")
// MAGIC        pairs = [(name, convert(data)) for (name, data) in kwargs.items()]
// MAGIC        vecs = zip(*[data for (_, data) in pairs])
// MAGIC        names = [name for (name, _) in pairs]
// MAGIC        # return self.sqlContext.createDataFrame(vecs, names)
// MAGIC        return (vecs, names)
// MAGIC       
// MAGIC 
// MAGIC def convert(z):
// MAGIC   if len(z.shape) == 1:
// MAGIC     return z.tolist()
// MAGIC   if len(z.shape) == 2:
// MAGIC     return [Vectors.dense(row) for row in z.tolist()]
// MAGIC   raise ValueError("Cannot convert a numpy array with more than 2 dimensions")
// MAGIC   
// MAGIC   
// MAGIC   
// MAGIC X = np.zeros((10,4))
// MAGIC y = np.ones(10)
// MAGIC 
// MAGIC z = [X, y]
// MAGIC 
// MAGIC #print convert(X)
// MAGIC #print convert(y)
// MAGIC 
// MAGIC #pairs = [(name, convert(data)) for (name, data) in kwargs.items()]
// MAGIC 
// MAGIC # df = conv.numpy_to_df(x = X, y = y)
// MAGIC df = numpy_to_df([X, y])
// MAGIC df
// MAGIC # df.printSchema()

// COMMAND ----------

// MAGIC %python
// MAGIC # Imports datasets from scikit-learn
// MAGIC from sklearn import datasets, linear_model
// MAGIC from pyspark.mllib.linalg import Vectors
// MAGIC import numpy as np
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC def _convert_vec(vec):
// MAGIC   return Vectors.dense([float(x) for x in vec])
// MAGIC 
// MAGIC def _convert_list(l):
// MAGIC   return list([x for x in l])
// MAGIC 
// MAGIC 
// MAGIC def numpy_to_df(self, **kwargs):
// MAGIC        """ Converts a set of numpy arrays into a single dataframe.
// MAGIC  
// MAGIC        The argument name is used to infer the name of the column. The value of the argument is a
// MAGIC        numpy array with a shape of length 0, 1, or 2. The dtype is one of the data types supported
// MAGIC        by sparkSQL. This includes np.double, np.float, np.int and np.long.
// MAGIC        See the whole list of supported types in the Spark SQL documentation:
// MAGIC        http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types
// MAGIC  
// MAGIC        The columns may not be in the order they are provided. Each column needs to have the same
// MAGIC        number of elements.
// MAGIC  
// MAGIC        :return: A pyspark.sql.DataFrame object, or raises a ValueError if the data type could not
// MAGIC        be understood.
// MAGIC  
// MAGIC        Example:
// MAGIC  
// MAGIC        >>> X = np.zeros((10,4))
// MAGIC        >>> y = np.ones(10)
// MAGIC        >>> df = conv.numpy_to_df(x = X, y = y)
// MAGIC        >>> df.printSchema()
// MAGIC        root
// MAGIC         |-- y: double (nullable = true)
// MAGIC         |-- x: vector (nullable = true)
// MAGIC        """
// MAGIC        def convert(z):
// MAGIC          if len(z.shape) == 1:
// MAGIC            return z.tolist()
// MAGIC          if len(z.shape) == 2:
// MAGIC            return [Vectors.dense(row) for row in z.tolist()]
// MAGIC          raise ValueError("Cannot convert a numpy array with more than 2 dimensions")
// MAGIC        pairs = [(name, convert(data)) for (name, data) in kwargs.items()]
// MAGIC        vecs = zip(*[data for (_, data) in pairs])
// MAGIC        names = [name for (name, _) in pairs]
// MAGIC        return self.sqlContext.createDataFrame(vecs, names)
// MAGIC 
// MAGIC # define the schema 
// MAGIC #schemaString = "value1 value2 value3 value4 value5 value6 value7 value8 value9 value10" 
// MAGIC #fields = [StructField(field_name, np.ndarray, True) for field_name in schemaString.split()] 
// MAGIC #schema = StructType(fields) 
// MAGIC 
// MAGIC # Apply the schema to the RDD. 
// MAGIC #schemaRDD = sqlContext.applySchema(rdd, schema) 
// MAGIC #schemaRDD = sqlContext.applySchema(rdd.map(list), schema) 
// MAGIC 
// MAGIC def convert_bunch(bunch):
// MAGIC   n = len(bunch.images)
// MAGIC   # df = sqlContext.createDataFrame([(_convert_vec(bunch.images[i]), float(bunch.filenames[i])) for i in range(n)])
// MAGIC   # df = sqlContext.createDataFrame([(bunch.images[i], bunch.filenames[i]) for i in range(n)])
// MAGIC   df = sqlContext.createDataFrame([(numpy_to_df(bunch.images[i]), bunch.filenames[i]) for i in range(n)])
// MAGIC   return df.withColumnRenamed("_1", "features").withColumnRenamed("_2", "label")
// MAGIC 
// MAGIC images = datasets.load_sample_images()
// MAGIC n = len(images.images)
// MAGIC print n
// MAGIC y = images.filenames[0]
// MAGIC x = images.images[0]
// MAGIC 
// MAGIC x
// MAGIC # this does not work because cannot convert str value
// MAGIC #df = sqlContext.createDataFrame([y, y])
// MAGIC 
// MAGIC #conv.numpy_to_df(images.images[0])
// MAGIC #print range(n)
// MAGIC #print size(images)
// MAGIC #df = convert_bunch(images)
// MAGIC # df.registerTempTable("images")

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

