// Databricks notebook source exported at Mon, 28 Mar 2016 15:44:54 UTC
// MAGIC %md # OR Block Scheduling
// MAGIC ### Extract History data and run linear regression with SGD with multiple variables

// COMMAND ----------

// MAGIC %md ## Build Data Frame

// COMMAND ----------

case class ORBookingTimeHistory(KeyID: Long, AltKeyID: Long, GroupID: Long, Case_Date: String, DayName_short: String, DayOfWeekNum: Long, DaysAhead: Long, Day30: Long, Day20: Long, DayX: Long, Day5: Long, RecCreateDt: String)

// COMMAND ----------

val dfOR = sc.textFile("/mnt/cg.montreal/history/ORBookingTimeHistory.csv").map(_.split(",")).map(m => ORBookingTimeHistory(m(0).toLong, m(1).toLong, m(2).toLong, m(3), m(4), m(5).toLong, m(6).toLong, m(7).toLong, m(8).toLong, m(9).toLong, m(10).toLong, m(11))).toDF()


// COMMAND ----------

dfOR.registerTempTable("ORBookingTimeHistory")

// COMMAND ----------

// MAGIC %sql SELECT * FROM ORBookingTimeHistory where Case_Date > '4/1/15' and GroupID = 1 limit 100;

// COMMAND ----------

// MAGIC %sql SELECT * FROM ORBookingTimeHistory where Case_Date > '4/1/15' and GroupID = 2 limit 100;

// COMMAND ----------

// MAGIC %md ## Review Features and Labels (i.e. x and y variables)

// COMMAND ----------

val df = sqlContext.sql("SELECT GroupID, DaysAhead, Day30, Day20, DayX, Day5, KeyID FROM ORBookingTimeHistory WHERE GroupID = 1 AND DaysAhead = 1")
val data = df.rdd
df.count

// COMMAND ----------

df.collect.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md ## Setup MLLib

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD


// COMMAND ----------

// MAGIC %md ### Defintions
// MAGIC * Features: Day30 (2), Day20 (3), DayX (4), 
// MAGIC * Label: Day5 (5)

// COMMAND ----------

val parsedData = data.map { p =>
  val label = p(5).toString.toDouble
  val features = Array(p(2), p(3), p(4)) map (_.toString.toDouble)
  LabeledPoint(label, Vectors.dense(features))
}


// COMMAND ----------

// MAGIC %md #### Quick View of Top 10 Parsed Data values

// COMMAND ----------

parsedData.collect.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md ### Feature Scaling (Regularization)

// COMMAND ----------

import org.apache.spark.mllib.feature.StandardScaler
val scaler = new StandardScaler(withMean = true, withStd = true).fit(parsedData.map(x => x.features))
val scaledData = parsedData.map(x => LabeledPoint(x.label, scaler.transform(Vectors.dense(x.features.toArray))))


// COMMAND ----------

// MAGIC %md #### Quick View of Top 10 Feature Scaled values

// COMMAND ----------

scaledData.collect.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md ## Train the Model

// COMMAND ----------

val numIterations = 3000
val alpha = 0.1
val algorithm = new LinearRegressionWithSGD()
algorithm.setIntercept(true)
algorithm.optimizer.setNumIterations(numIterations)
algorithm.optimizer.setStepSize(alpha)
val model = algorithm.run(scaledData)


// COMMAND ----------

// MAGIC %md ## Historical Predicted vs. Actual

// COMMAND ----------

val valuesAndPreds = scaledData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, point.features, prediction)
  }

// COMMAND ----------

valuesAndPreds.take(10).foreach({case (v, f, p) => 
      println(s"Features: ${f}, Predicted: ${p}, Actual: ${v}")})


// COMMAND ----------

valuesAndPreds.toDF().registerTempTable("LRresults")

// COMMAND ----------

// MAGIC %sql select `_1` as Actual, `_3` as Predicted from LRResults;

// COMMAND ----------

// MAGIC %md # Future Booking Data
// MAGIC #### Appointments have been scheduled within the operating rooms weeks to months in advance.
// MAGIC #### The key issue is there is a regular occurences of cancellations leaving the operating rooms idle
// MAGIC #### This is disadvantageous to the hospital (i.e. expensive operating rooms that are not in use) 
// MAGIC #### But more importantly, disadvantageous to patients whom could be getting their surgeries earlier so they do not have to wait

// COMMAND ----------

// MAGIC %md ## Can we predict the schedule?
// MAGIC #### So how about if we predict the available time slots so we can bring in patients earlier?
// MAGIC #### This way they only need to wait 1-5 days instead of weeks or months

// COMMAND ----------

// MAGIC %md ## Build Data Frame
// MAGIC #### * Build Data Frame for Future Booking data.  
// MAGIC #### * This dataset contains the OR scheduling at a future time (e.g. We have scheduled Dave's hip reconstruction on 7/1/2015)

// COMMAND ----------

case class ORFutureBooking(KeyID: Long, GroupID: Long, OR_Date: String, DayName_short: String, DayOfWeekNum: Long, DaysAhead: Long, Total_Block_Time: Long, Total_Booked_Time: Long, Block_Available: Long, Day30: Long, Day20: Long, DayX: Long, Day5: Long, RecCreateDt: String)
val dfORFutureBooking = sc.textFile("/mnt/cg.montreal/future/time/ORFutureBookingTime.csv").map(_.split(",")).map(m => ORFutureBooking(m(0).toLong, m(1).toLong, m(2), m(3), m(4).toLong, m(5).toLong, m(6).toLong, m(7).toLong, m(8).toLong, m(9).toLong, m(10).toLong, m(11).toLong, m(12).toLong, m(13))).toDF()
dfORFutureBooking.registerTempTable("ORFutureBooking")

// COMMAND ----------

// MAGIC %md ### Parse and Scale Data
// MAGIC #### * Parse out the future data 
// MAGIC #### * Apply regularization against the data

// COMMAND ----------

val dfFuture = sqlContext.sql("SELECT GroupID, DaysAhead, Day30, Day20, DayX, Day5, KeyID FROM ORFutureBooking WHERE GroupID = 1 AND DaysAhead = 1")
val dataFuture = dfFuture.rdd
val parsedFutureData = dataFuture.map { p =>
  val label = p(5).toString.toDouble
  val features = Array(p(2), p(3), p(4)) map (_.toString.toDouble)
  LabeledPoint(label, Vectors.dense(features))
}
val scaledFutureData = parsedFutureData.map(x => LabeledPoint(x.label, scaler.transform(Vectors.dense(x.features.toArray))))

// COMMAND ----------

dfFuture.collect.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md ### Quick View of Future Data for Days Ahead = 1, GroupID = 1

// COMMAND ----------

parsedFutureData.collect.take(10).foreach(println)

// COMMAND ----------

scaledFutureData.collect.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md ## Apply Previous Linear Regression with SGD model
// MAGIC #### For GroupID = 1, DaysAhead = 1 (KeyID = 151), predicted booking is 483

// COMMAND ----------

val valuesAndPredsFuture = scaledFutureData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, point.features, prediction)
  }
valuesAndPredsFuture.take(10).foreach({case (v, f, p) => 
      println(s"Features: ${f}, Predicted: ${p}, Actual: ${v}")})

// COMMAND ----------

// MAGIC %md ### MLLib LinearRegressionSGD prediction: 478.1019349027521

// COMMAND ----------

