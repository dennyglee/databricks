-- Databricks notebook source
-- MAGIC %md # Data Exploration on Databricks

-- COMMAND ----------

-- MAGIC %md ### Parsing weblogs with regular expressions to create a table
-- MAGIC 
-- MAGIC * Original Format: `%s %s %s [%s] \"%s %s HTTP/1.1\" %s %s`
-- MAGIC * Example Web Log Row 
-- MAGIC  * `10.0.0.213 - 2185662 [14/Aug/2015:00:05:15 -0800] "GET /Hurricane+Ridge/rss.xml HTTP/1.1" 200 288`

-- COMMAND ----------

-- MAGIC %fs head /mnt/tardis6/samples/weblog/apache/ex20150814.log

-- COMMAND ----------

-- MAGIC %md ## Create External Table
-- MAGIC Create an external table against the weblog data where we define a regular expression format as part of the serializer/deserializer (SerDe) definition.  Instead of writing ETL logic to do this, our table definition handles this.

-- COMMAND ----------

DROP TABLE IF EXISTS weblog;
CREATE EXTERNAL TABLE weblog (
  ipaddress STRING,
  clientidentd STRING,
  userid STRING,
  datetime STRING,
  method STRING,
  endpoint STRING,
  protocol STRING,
  responseCode INT,
  contentSize BIGINT
)
ROW FORMAT
  SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \\"(\\S+) (\\S+) (\\S+)\\" (\\d{3}) (\\d+)'
)
LOCATION 
  "/mnt/tardis6/samples/weblog/apache/"

-- COMMAND ----------

-- MAGIC %md ## Query your weblogs using Spark SQL
-- MAGIC Instead of parsing and extracting out the datetime, method, endpoint, and protocol columns; the external table has already done this for you.  Now you can treat your weblog data similar to how you would treat any other structured dataset and write Spark SQL against the table.
-- MAGIC 
-- MAGIC Instead of reading the data as strings: `10.0.0.213 - 2185662 [14/Aug/2015:00:05:15 -0800] "GET /Hurricane+Ridge/rss.xml HTTP/1.1" 200 288`, you can review a table as noted below.

-- COMMAND ----------

select * from weblog limit 10;

-- COMMAND ----------

-- MAGIC %md ## Enhanced Spark SQL queries
-- MAGIC At this point, we can quickly write SQL group by statements to understand which web page in the logs has the most number of events. But notice that there is a hierarchy of pages within the endpoint column.  We just want want to understand the top level hierarchy - which area such as the Olympics, Casacdes, or Rainier are more popular

-- COMMAND ----------

select endpoint, count(1) as Events
  from weblog 
 group by endpoint
 order by Events desc 

-- COMMAND ----------

-- MAGIC %md #### To extract out the top level hierarchy within the same Spark SQL, we can write a nested SQL statement with regular expressions and split statements
-- MAGIC * Click on the table and chart icon below the results to switch between table and bar graph.

-- COMMAND ----------

select TaggedEndPoints, NumHits
  from (
    select regexp_replace(split(endpoint, "/")[1], "index.html", "Home") as TaggedEndPoints, count(1) as NumHits
      from weblog 
     group by regexp_replace(split(endpoint, "/")[1], "index.html", "Home")
  ) a
 where TaggedEndPoints not in ('index.html', 'msgs', 'tags', 'Home', 'search')
 order by NumHits desc

-- COMMAND ----------

-- MAGIC %md ## Making sense of web site response codes
-- MAGIC Taking a step back, let's also look at how well the site is working by reviewing the response codes with a group by statement

-- COMMAND ----------

 select responsecode, count(1) as responses 
   from weblog 
  group by responsecode

-- COMMAND ----------

-- MAGIC %md #### But it would be better to view this data using the response text instead of the just the codes.  So let's
-- MAGIC * Create a response description table 
-- MAGIC * Re-write the above SQL statement to join to this table

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # Using Python to create our ResponseCodes DataFrame
-- MAGIC response_codes = spark.createDataFrame([(301, "Moved Permanently"), (200, "OK"), (304, "Not Modified"), (302, "Found")], ["ResponseCode", "ResponseDesc"])
-- MAGIC response_codes.createOrReplaceTempView("response_codes")

-- COMMAND ----------

-- Join to the response codes table
-- Switch to pie chart using the chart button below the results
select r.responsedesc, count(1) as responses
  from weblog f
    inner join response_codes r
      on r.responsecode = f.responsecode
 group by r.responsedesc
 order by responses desc

-- COMMAND ----------

-- MAGIC %md ## Mapping your Data
-- MAGIC Let's expand on our exploration by seeing where the users and sessions are coming from within our web logs.  To do this, we will create mapping table between the web logs IP address to ISO-3166-1 3-letter country codes, 2-letter state codes.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create table that maps IP address to state
-- MAGIC map2state = spark.read.csv('/mnt/tardis6/samples/weblog/map/map2state.csv', header='true', inferSchema='true')
-- MAGIC map2state.createOrReplaceTempView("map2state")

-- COMMAND ----------

select m.state, count(1) as events, count(distinct UserID) as users
  from weblog f
    inner join map2state m
      on m.ipaddress = f.ipaddress
  group by m.state
  order by users desc