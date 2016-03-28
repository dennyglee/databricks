// Databricks notebook source exported at Mon, 28 Mar 2016 15:42:16 UTC
// MAGIC %md ## Preparation and DataFrames 
// MAGIC This is the first notebook of the **Streaming Meetup RSVPs** set of notebooks.  The purpose of this notebook is to connect to the data source within DBFS to view and understand the data formats.  The ultimate source of this data is from the [Meetup Streaming API](http://www.meetup.com/meetup_api/docs/stream/2/rsvps/). 
// MAGIC 
// MAGIC *Meetup Sources*
// MAGIC * Based off of the [Meetup RSVP Ticker](http://meetup.github.io/stream/rsvpTicker/)
// MAGIC * Reference: [Meetup Streaming API > RSVPs](http://www.meetup.com/meetup_api/docs/stream/2/rsvps/)

// COMMAND ----------

// Review sample Meetup RSVPs stored within DBFS
display(dbutils.fs.ls("/mnt/tardis6/meetup-stream/"))

// COMMAND ----------

// Create the df DataFrame reading the JSON files stored in DBFS
val df = sqlContext.read.json("/mnt/tardis6/meetup-stream/")

// COMMAND ----------

// Show the top 20 rows from this DataFrame
df.show()

// COMMAND ----------

// Print the schema of this DataFrame
df.printSchema()

// COMMAND ----------

// Reviewing the DataFrame: Specify the group_country and event_name columns
df.select(df("group.group_country"), df("event.event_name")).show()

// COMMAND ----------

// Reviewing the DataFrame: Filter by "Spark" and specify columns 
df.filter(df("group.group_name").contains("Spark")).select(df("group.group_country"), df("group.group_state"), df("group.group_name"), df("event.event_name"), df("member.member_id"), df("response")).show()

// COMMAND ----------

import core._
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._

// COMMAND ----------

// MAGIC %md ### What's next?
// MAGIC The next notebook starts a streaming application which obtains the Meetup RSVPs from the Meetup Streaming RSVP APIs 

// COMMAND ----------

