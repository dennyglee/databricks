// Databricks notebook source exported at Wed, 8 Jun 2016 21:54:41 UTC
// MAGIC %md ## Collect Tweets and save to S3

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import com.google.gson.Gson

import scala.math.Ordering

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

// COMMAND ----------

// MAGIC %md ## Twitter Auth

// COMMAND ----------

System.setProperty("twitter4j.oauth.consumerKey", "[consumerKey]")
System.setProperty("twitter4j.oauth.consumerSecret", "[consumerSecret]")
System.setProperty("twitter4j.oauth.accessToken", "[accessToken]")
System.setProperty("twitter4j.oauth.accessTokenSecret", "[accessTokenSecret]")

// COMMAND ----------

val outputDirectory = "/mnt/databricks-denny/twitter602/"
val slideInterval = new Duration(1000)
val windowLength = new Duration(5000)
val timeoutJobLength = 100000
val partitionsEachInterval = 1

// Listen to only these tweets
val filters = Array("politics", "clinton", "sanders", "trump", "cruz")

// COMMAND ----------

// MAGIC %md ## Run Twitter Streaming Job

// COMMAND ----------

// Clean up old files
//dbutils.fs.rm(outputDirectory, true)

// COMMAND ----------

// MAGIC %md Create the function to that creates the Streaming Context and sets up the streaming job.

// COMMAND ----------

var newContextCreated = false
var num = 0

// This is a helper class used for 
object SecondValueOrdering extends Ordering[(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, slideInterval)
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth, filters)
  
  
  twitterStream.foreachRDD((rdd, time) => {
    val count = rdd.count()
    if (count > 0) {
      val outputRDD = rdd.repartition(partitionsEachInterval)
      outputRDD.mapPartitions{ it =>
        val gson = new com.google.gson.Gson()
        it.map(gson.toJson(_))
      }.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
    }
  })
     
  newContextCreated = true
  ssc
}

// COMMAND ----------

// MAGIC %md Create the StreamingContext using getActiveOrCreate, as required when starting a streaming job in Databricks

// COMMAND ----------

@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

// COMMAND ----------

// MAGIC %md Start the Spark Streaming Context and return when the Streaming job exits or return with the specified timeout.

// COMMAND ----------

ssc.start()
ssc.awaitTerminationOrTimeout(timeoutJobLength)

// COMMAND ----------

// MAGIC %md Stop any active Streaming Contexts, but don't stop the spark contexts they are attached to.

// COMMAND ----------

//StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

// MAGIC %md ## View the Results.

// COMMAND ----------

display(dbutils.fs.ls(outputDirectory))

// COMMAND ----------

