// Databricks notebook source exported at Mon, 28 Mar 2016 15:58:03 UTC
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

