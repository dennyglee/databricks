// Databricks notebook source exported at Mon, 28 Mar 2016 15:43:38 UTC
// MAGIC %md ## mapWithState for Stateful Aggregations (Country)
// MAGIC This is the fourth notebook of the **Streaming Meetup RSVPs** set of notebooks.  The purpose of this notebook is to connect to the [Meetup Streaming API](http://www.meetup.com/meetup_api/docs/stream/2/rsvps/), execute a streaming notebook, and use `mapWithState` perform the aggregation (see if we can reduce the scheduling delays)
// MAGIC 
// MAGIC *Meetup Sources*
// MAGIC * Based off of the [Meetup RSVP Ticker](http://meetup.github.io/stream/rsvpTicker/)
// MAGIC * Reference: [Meetup Streaming API > RSVPs](http://www.meetup.com/meetup_api/docs/stream/2/rsvps/)

// COMMAND ----------

// MAGIC %md ### Attributions
// MAGIC Various references utilized through this example
// MAGIC * [Spark Streaming Programming Guide](https://people.apache.org/~pwendell/spark-nightly/spark-1.6-docs/latest/streaming-programming-guide.html)
// MAGIC * [Streaming Word Count](https://demo.cloud.databricks.com/#notebook/146957) and [Twitter Hashtag Count](https://demo.cloud.databricks.com/#notebook/147068) notebooks
// MAGIC * [Global Aggregations using mapWithState](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#08%20Spark%20Streaming/12%20Global%20Aggregations%20-%20mapWithState.html)
// MAGIC * Spark Examples [NetworkWordCount.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/NetworkWordCount.scala), [StatefulNetworkWordCount.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/StatefulNetworkWordCount.scala)
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


// Verify that the attached Spark cluster is 1.6.0+
require(sc.version.replace(".", "").substring(0,3).toInt >= 160, "Spark 1.6.0+ is required to run this notebook. Please attach it to a Spark 1.6.0+ cluster.")

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

// MAGIC %md ### Provide the function that has the logic for updating the state
// MAGIC The update function is called on a paired (key-value) DStream. The update function is called for every element in the paired DStream. The function takes the following input parameters:
// MAGIC * The current Batch Time
// MAGIC * The key for which the state needs to be updated
// MAGIC * The value observed at the 'Batch Time' for the key.
// MAGIC * The current state for the key.
// MAGIC 
// MAGIC The function should return the new (key, value) pair where value has the updated state information

// COMMAND ----------

/*
In this example:
- key is the group_state.
- value is '1'. Its type is 'Int'.
- state has the running count of the word. It's type is Long. The user can provide more custom classes as type too.
- The return value is the new (key, value) pair where value is the updated count.
*/

def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
  val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
  val output = (key, sum)
  state.update(sum)
  Some(output)
}

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ### State Specification
// MAGIC Along with the update function, you can also provide a bunch of other information for updating the state. This information can be provided through the State spec. You can provide the following information:
// MAGIC * An initial state as RDD - You can load the initial state from some store and then start your streaming job with that state.
// MAGIC * Number of partitions - The key value state dstream is partitioned by keys. If you have a good estimate of the size of the state before, you can provide the number of partitions to partition it accordingly.
// MAGIC * Partitioner - You can also provide a custom partitioner. The default partitioner is hash partitioner. If you have a good understanding of the key space, then you can provide a custom partitioner that can do efficient updates than the default hash partitioner.
// MAGIC * Timeout - This will ensure that keys whose values are not updated for a specific period of time will be removed from the state. This can help in cleaning up the state with old keys.

// COMMAND ----------

val initialRDD = sc.parallelize(List(("us", 0L), ("ca", 0L)))
val stateSpec = StateSpec.function(trackStateFunc _)
                         .initialState(initialRDD)
                         .numPartitions(2)
                         .timeout(Minutes(60))

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ### Extract groupCountry Function
// MAGIC Extract groupCountry from the Meetup JSON using json4s (Jackson)

// COMMAND ----------

import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util.{Try,Success,Failure}

def parseRsvpGroupCountry(rsvpJson: String):String = {
  Try({
    val json = parse(rsvpJson).camelizeKeys
    val groupCountry = compact(render(json \\ "group" \\ "groupCountry")).replace("\"", "")
    groupCountry
  }).toString
}

def parseSuccess(groupState: String):String = {
  val r = """^Success\((.*)\)""".r
  groupState match {
    case r(group) => group
    case _ => ""
  } 
}  

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ### The core streaming function

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
  
  // Create a stream that connects to the MeetupReceiver
  val stream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/rsvps"))
  
  // Extract JSON within the stream using read.json(rdd) and create a pair (key-value) dstream
  val meetupStream = stream.map(line => parseSuccess(parseRsvpGroupCountry(line))).map(groupCountry => (groupCountry, 1))
  
  // This represents the emitted stream from the trackStateFunc. Since we emit every input record with the updated value,
  // this stream will contain the same # of records as the input dstream.
  val meetupStateStream = meetupStream.mapWithState(stateSpec)
  meetupStateStream.print()

  // A snapshot of the state for the current batch. This dstream contains one entry per key.
  val stateSnapshotStream = meetupStateStream.stateSnapshots()  
  stateSnapshotStream.foreachRDD { rdd =>
    rdd.toDF("group_country", "count").registerTempTable("batch_meetup_stream_country")
  }
  
  ssc.remember(Minutes(1))  // To make sure data is not deleted by the time we query it interactively
  
  ssc.checkpoint("dbfs:/streaming/trackstate/120")
  
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
ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 2 * 1000)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ### Query the Emitted Stream
// MAGIC The `mapWithState` interface returns an **emitted** stream. The emitted stream represents all the emitted records from the `trackStateFunc`. In this example, we are emitting every input record with the updated value. You can also choose to emit only certain records in the trackStateFunc.  Note, to get the snapshot stream, you can use `emittedStream.stateSnapshots()`.

// COMMAND ----------

// MAGIC %sql select * from batch_meetup_stream_country order by count desc limit 20;

// COMMAND ----------

// MAGIC %md ### Stop Streaming Job
// MAGIC * To stop the StreamingContext, uncomment below and execute 

// COMMAND ----------

//StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

