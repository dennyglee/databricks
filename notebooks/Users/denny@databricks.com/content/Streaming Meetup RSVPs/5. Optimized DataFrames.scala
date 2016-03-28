// Databricks notebook source exported at Mon, 28 Mar 2016 15:44:23 UTC
// MAGIC %md ## Optimized DataFrames
// MAGIC This is the fourth notebook of the **Streaming Meetup RSVPs** set of notebooks.  The purpose of this notebook is to connect to the [Meetup Streaming API](http://www.meetup.com/meetup_api/docs/stream/2/rsvps/), execute a streaming notebook, and using optimized dataframes.
// MAGIC 
// MAGIC *Meetup Sources*
// MAGIC * Based off of the [Meetup RSVP Ticker](http://meetup.github.io/stream/rsvpTicker/)
// MAGIC * Reference: [Meetup Streaming API > RSVPs](http://www.meetup.com/meetup_api/docs/stream/2/rsvps/)

// COMMAND ----------

// MAGIC %md ### Attributions
// MAGIC Various references utilized through this example
// MAGIC * [Spark Streaming Programming Guide](https://people.apache.org/~pwendell/spark-nightly/spark-1.6-docs/latest/streaming-programming-guide.html)
// MAGIC * [Streaming Word Count](https://demo.cloud.databricks.com/#notebook/146957) and [Twitter Hashtag Count](https://demo.cloud.databricks.com/#notebook/147068) notebooks
// MAGIC * Spark Examples [NetworkWordCount.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/NetworkWordCount.scala)
// MAGIC * [Meetup-Stream MeetupReceiver](https://github.com/actions/meetup-stream/blob/master/src/main/scala/receiver/MeetupReceiver.scala)
// MAGIC * [Killrweather KafkaStreamingJson.scala](https://github.com/killrweather/killrweather/blob/master/killrweather-examples/src/main/scala/com/datastax/killrweather/KafkaStreamingJson.scala)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ### Setup: Define the function that sets up the StreamingContext
// MAGIC * Create MeetupReciever 
// MAGIC  * Source: [MeetupReceiver.scala](https://github.com/actions/meetup-stream/blob/master/src/main/scala/receiver/MeetupReceiver.scala)
// MAGIC  * Install the Async-HTTP-Client Library 
// MAGIC    * [Async-HTTP-Client Library Source](http://mvnrepository.com/artifact/com.ning/async-http-client/1.9.31)
// MAGIC    * Follow the Install Library Notebook for steps to install external Scala / Java JARs
// MAGIC * Define the function that creates and sets up the streaming computation (this is the main logic)

// COMMAND ----------

// Spark Streaming
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

// AsyncHTTP Client
import com.ning.http.client.AsyncHttpClientConfig
import com.ning.http.client._


// === Configuration to control the flow of the application ===
val stopActiveContext = true	 
// "true"  = stop if any existing StreamingContext is running;              
// "false" = dont stop, and let it run undisturbed, but your latest code may not be used

// === Configurations for Spark Streaming ===
val batchIntervalSeconds = 1 


/**
 * MeetupReceiver.scala
 *   @author szelvenskiy
 *   Source: https://github.com/actions/meetup-stream/blob/master/src/main/scala/receiver/MeetupReceiver.scala
 *   Uses http://mvnrepository.com/artifact/com.ning/async-http-client/1.9.31
 */

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import com.ning.http.client.AsyncHttpClientConfig
import com.ning.http.client._
import scala.collection.mutable.ArrayBuffer
import java.io.OutputStream
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream

class MeetupReceiver(url: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  
  @transient var client: AsyncHttpClient = _
  
  @transient var inputPipe: PipedInputStream = _
  @transient var outputPipe: PipedOutputStream = _  
       
  def onStart() {    
    val cf = new AsyncHttpClientConfig.Builder()
    cf.setRequestTimeout(Integer.MAX_VALUE)
    cf.setReadTimeout(Integer.MAX_VALUE)
    cf.setPooledConnectionIdleTimeout(Integer.MAX_VALUE)      
    client= new AsyncHttpClient(cf.build())
    
    inputPipe = new PipedInputStream(1024 * 1024)
    outputPipe = new PipedOutputStream(inputPipe)
    val producerThread = new Thread(new DataConsumer(inputPipe))
    producerThread.start()
    
    client.prepareGet(url).execute(new AsyncHandler[Unit]{
        
      def onBodyPartReceived(bodyPart: HttpResponseBodyPart) = {
        bodyPart.writeTo(outputPipe)
        AsyncHandler.STATE.CONTINUE        
      }
      
      def onStatusReceived(status: HttpResponseStatus) = {
        AsyncHandler.STATE.CONTINUE
      }
      
      def onHeadersReceived(headers: HttpResponseHeaders) = {
        AsyncHandler.STATE.CONTINUE
      }
            
      def onCompleted = {
        println("completed")
      }
      
      
      def onThrowable(t: Throwable)={
        t.printStackTrace()
      }
        
    })    
    
    
  }

  def onStop() {
    if (Option(client).isDefined) client.close()
    if (Option(outputPipe).isDefined) {
     outputPipe.flush()
     outputPipe.close() 
    }
    if (Option(inputPipe).isDefined) {
     inputPipe.close() 
    }    
  }
  
  class DataConsumer(inputStream: InputStream) extends Runnable 
  {
       
      override
      def run()
      {        
        val bufferedReader = new BufferedReader( new InputStreamReader( inputStream ))
        var input=bufferedReader.readLine()
        while(input!=null){          
          store(input)
          input=bufferedReader.readLine()
        }            
      }  
      
  }

}

// COMMAND ----------



// COMMAND ----------

/**
 * creatingFunc()
 *   Defines the Streaming Context function
 *   Creates `meetup_stream_json` temporary table
 */

// Flag to detect whether new context was created or not
var newContextCreated = false

// Function to create a new StreamingContext and set it up
def creatingFunc(): StreamingContext = {
    
  // Create a StreamingContext
  val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
  ssc.checkpoint(".")
  
  // Create a stream that connects to the MeetupReceiver
  val stream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/rsvps"))
  
  // Initial state RDD for mapWithState operation
  val initialRDD = ssc.sparkContext.parallelize(List(("WA", 1), ("CA", 1)))
  
  // Create temp table at every batch interval
  stream.foreachRDD { rdd => 
    if (rdd.toLocalIterator.nonEmpty) {
      // Create Streaming DataFrame by reading the data within the RDD
      val df = sqlContext.read.json(rdd)
      
      // Create RDD of group_state and count information
      val rsvps = df.groupBy(df("group.group_state")).count().rdd
      println("\t " + rsvps.take(10).mkString(", ") + ", ...")

      //val rsvpsDstream = rsvps.map(x => (x, 1))
      //println("\t " + rsvpsDstream.take(10).mkString(", ") + ", ...")

      // Update the cumulative count using mapWithState
      // This will give a DStream made of state (which is the cumulative count of the group_state)
      //val mappingFunc = (group_state: String, one: Option[Int], state: State[Int]) => {
      //  val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      //  val output = (group_state, sum)
      //  state.update(sum)
      //  output
      //}
      
      //val stateDstream = sRDD.mapWithState(
      //  StateSpec.function(mappingFunc).initialState(initialRDD))
      //stateDstream.print()

    }
  }
  
  ssc.remember(Minutes(5))  // To make sure data is not deleted by the time we query it interactively
  
  println("Creating function called to create new StreamingContext")
  newContextCreated = true  
  ssc
}  

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ### Start Streaming Job
// MAGIC * Stop existing StreamingContext if any and start/restart the new one
// MAGIC * Here we are going to use the configurations at the top of the notebook to decide whether to:
// MAGIC  * stop any existing StreamingContext, and start a new one, 
// MAGIC  * or recover one from existing checkpoints.

// COMMAND ----------

// Create a StreamingContext
val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
//ssc.checkpoint(".")
  
// Create a stream that connects to the MeetupReceiver
val stream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/rsvps"))
  
// Initial state RDD for mapWithState operation
val initialRDD = ssc.sparkContext.parallelize(List(("WA", 1), ("CA", 1)))
  
// Create temp table at every batch interval
stream.foreachRDD { rdd => 
  if (rdd.toLocalIterator.nonEmpty) {
    // Create Streaming DataFrame by reading the data within the RDD
    val df = sqlContext.read.json(rdd)
      
    // Create RDD of group_state and count information
    val rsvps = df.select(df("group.group_country")).rdd
    val rsvpsDstream = rsvps.map(x => (x(0), 1))
    println("\t " + rsvpsDstream.take(10).mkString(", ") + ", ...")

    // Update the cumulative count using mapWithState
    // This will give a DStream made of state (which is the cumulative count of the group_state)
    val mappingFunc = (group_country: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (group_country, sum)
      state.update(sum)
      output
    }
      
    val stateDstream = rsvpsDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()

  }
}
  
// Start the streaming context in the background.
ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)

// COMMAND ----------

// Stop any existing StreamingContext 
if (stopActiveContext) {	
  StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
} 

// Get or create a streaming context
val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
if (newContextCreated) {
  println("New context created from currently defined creating function") 
} else {
  println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
}

// Start the streaming context in the background.
ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ### Stop Streaming Job
// MAGIC * To stop the StreamingContext, uncomment below and execute 

// COMMAND ----------

StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

