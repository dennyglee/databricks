# Databricks notebook source exported at Mon, 28 Mar 2016 15:58:35 UTC
# MAGIC %md # Advertising Technology Sample Notebook (Part 1)
# MAGIC The purpose of this notebook is to provide example code to make sense of advertising-based web logs.  This notebook does the following:
# MAGIC * Setup the connection to your S3 bucket to access the web logs
# MAGIC * Create an external table against these web logs including the use of regular expression to parse the logs
# MAGIC * Identity Country (ISO-3166-1 Three Letter ISO Country Codes) based on IP address by calling a REST Web service API
# MAGIC * Identify Browser and OS information based on the User Agent string within the web logs using the user-agents PyPi package.
# MAGIC * Convert the Apache web logs date information, create a userid, and join back to the Browser and OS information

# COMMAND ----------

# MAGIC %md ## Setup Instructions
# MAGIC * Please refer to the [Databricks Data Import How-To Guide](https://databricks.com/wp-content/uploads/2015/08/Databricks-how-to-data-import.pdf) on how to import data into S3 for use with Databricks notebooks.

# COMMAND ----------

# Setup AWS configuration
import urllib
ACCESS_KEY = "[REPLACE_WITH_ACCESS_KEY]"
SECRET_KEY = "[REPLACE_WITH_SECRET_KEY]"
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "[REPLACE_WITH_BUCKET_NAME]"
MOUNT_NAME = "mdl"

# Mount S3 bucket
dbutils.fs.mount("s3n://%s:%s@%s/" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# View the log files within the mdl mount
display(dbutils.fs.ls("/mnt/mdl/accesslogs/"))

# COMMAND ----------

# MAGIC %fs ls /mnt/mdl/accesslogs/

# COMMAND ----------

# Count the number of rows within the sample Apache Access logs
myAccessLogs = sc.textFile("/mnt/mdl/accesslogs/")
myAccessLogs.count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Create External Table
# MAGIC * Create an external table against the access log data where we define a regular expression format as part of the serializer/deserializer (SerDe) definition.  
# MAGIC * Instead of writing ETL logic to do this, our table definition handles this.
# MAGIC * Original Format: %s %s %s [%s] \"%s %s HTTP/1.1\" %s %s
# MAGIC * Example Web Log Row 
# MAGIC  * 10.0.0.213 - 2185662 [14/Aug/2015:00:05:15 -0800] "GET /Hurricane+Ridge/rss.xml HTTP/1.1" 200 288

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS accesslog;
# MAGIC CREATE EXTERNAL TABLE accesslog (
# MAGIC   ipaddress STRING,
# MAGIC   clientidentd STRING,
# MAGIC   userid STRING,
# MAGIC   datetime STRING,
# MAGIC   method STRING,
# MAGIC   endpoint STRING,
# MAGIC   protocol STRING,
# MAGIC   responseCode INT,
# MAGIC   contentSize BIGINT,
# MAGIC   referrer STRING,
# MAGIC   agent STRING,
# MAGIC   duration STRING,
# MAGIC   ip1 STRING,
# MAGIC   ip2 STRING,
# MAGIC   ip3 STRING,
# MAGIC   ip4 STRING
# MAGIC )
# MAGIC ROW FORMAT
# MAGIC   SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
# MAGIC WITH SERDEPROPERTIES (
# MAGIC   "input.regex" = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\]  \\"(\\S+) (\\S+) (\\S+)\\" (\\d{3}) (\\d+) \\"(.*)\\" \\"(.*)\\" (\\S+) \\"(\\S+), (\\S+), (\\S+), (\\S+)\\"'
# MAGIC )
# MAGIC LOCATION 
# MAGIC   "/mnt/mdl/accesslogs/"

# COMMAND ----------

# MAGIC %sql select ipaddress, datetime, method, endpoint, protocol, responsecode, agent from accesslog limit 10;

# COMMAND ----------

# MAGIC %sql select agent from accesslog limit 10;

# COMMAND ----------

# MAGIC %md ## Obtain ISO-3166-1 Three Letter Country Codes from IP address
# MAGIC * Extract out the distinct set of IP addresses from the Apache Access logs
# MAGIC * Make a REST web service call to freegeoip.net to get the two-letter country codes based on the IP address
# MAGIC  * This creates the **mappedIP2** DataFrame where the schema is encoded.
# MAGIC * Create a DataFrame to extract out a mapping between 2-letter code, 3-letter code, and country name
# MAGIC  * This creates the **countryCodesDF** DataFrame where the schema is inferred
# MAGIC * Join these two data frames together and select out only the four columns needed to create the **mappedIP3** DataFrame

# COMMAND ----------

# Obtain location based on IP address
import urllib2
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

# Obtain the unique agents from the accesslog table
ipaddresses = sqlContext.sql("select distinct ip1 from accesslog where ip1 is not null limit 2").rdd

# Convert None to Empty String
def xstr(s): 
  if s is None: 
    return '' 
  return str(s)

# getCCA2: Obtains two letter country code based on IP address
def getCCA2(ip):
  # Obtain CCA2 code from FreeGeoIP
  url = 'http://freegeoip.net/csv/' + ip
  str = urllib2.urlopen(url).read()
  cca2 = str.split(",")[1]
  
  # return
  return cca2

# Loop through distinct IP addresses and obtain two-letter country codes
mappedIPs = ipaddresses.map(lambda x: (x[0], getCCA2(x[0])))

# mappedIP2: contains the IP address and CCA2 codes
# The schema is encoded in a string.
schemaString = "ip cca2"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Create DataFrame with schema
mappedIP2 = sqlContext.createDataFrame(mappedIPs, schema)

# Obtain the Country Codes 
fields = sc.textFile("/mnt/tardis6/countrycodes/").map(lambda l: l.split(","))
countrycodes = fields.map(lambda x: Row(cn=x[0], cca2=x[1], cca3=x[2]))

# Country Codes DataFrame:
#   Create DataFrame (inferring schema using reflection)
countryCodesDF = sqlContext.createDataFrame(countrycodes)

# Join countrycodes with mappedIPsDF so we can have IP address and three-letter ISO country codes
mappedIP3 = mappedIP2 \
  .join(countryCodesDF, mappedIP2.cca2 == countryCodesDF.cca2, "left_outer") \
  .select(mappedIP2.ip, mappedIP2.cca2, countryCodesDF.cca3, countryCodesDF.cn)
  
# Register the mapping table
mappedIP3.registerTempTable("mappedIP3")

# COMMAND ----------

# MAGIC %sql select * from mappedIP3 limit 20;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Identity the Browser and OS information 
# MAGIC * Extract out the distinct set of user agents from the Apache Access logs
# MAGIC * Use the Python Package [user-agents](https://pypi.python.org/pypi/user-agents) to extract out Browser and OS information from the User Agent strring
# MAGIC * For more information on installing pypi packages in Databricks, refer to [Databricks Guide > Product Overview > Libraries](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#02%20Product%20Overview/07%20Libraries.html)

# COMMAND ----------

from user_agents import parse
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Convert None to Empty String
def xstr(s): 
  if s is None: 
    return '' 
  return str(s)

# Create UDFs to extract out Browser Family and OS Family information
def browserFamily(ua_string) : return xstr(parse(xstr(ua_string)).browser.family)
def osFamily(ua_string) : return xstr(parse(xstr(ua_string)).os.family)
udfBrowserFamily = udf(browserFamily, StringType())
udfOSFamily = udf(osFamily, StringType())

# Obtain the unique agents from the accesslog table
userAgentTbl = sqlContext.sql("select distinct agent from accesslog")

# Add new columns to the UserAgentInfo DataFrame containing browser and OS information
userAgentInfo = userAgentTbl.withColumn('browserFamily', udfBrowserFamily(userAgentTbl.agent))
userAgentInfo = userAgentInfo.withColumn('OSFamily', udfOSFamily(userAgentTbl.agent))

# Register the DataFrame as a table
userAgentInfo.registerTempTable("UserAgentInfo")

# COMMAND ----------

# Review the top 10 rows from the UserAgentInfo DataFrame
display(sqlContext.sql("SELECT browserFamily, OSFamily, agent FROM UserAgentInfo LIMIT 10"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## UserID, Date, and Joins
# MAGIC To make finish basic preparation of these web logs, we will do the following: 
# MAGIC * Convert the Apache web logs date information
# MAGIC * Create a userid based on the IP address and User Agent (these logs do not have a UserID)
# MAGIC  * We are generating the UserID (a way to uniquify web site visitors) by combining these two columns
# MAGIC * Join back to the Browser and OS information as well as Country (based on IP address) information
# MAGIC * Also include call to udfWeblog2Time function to convert the Apache web log date into a Spark SQL / Hive friendly format (for session calculations below)

# COMMAND ----------

from pyspark.sql.types import DateType
from pyspark.sql.functions import udf
import time

# weblog2Time function
#   Input: 04/Nov/2015:08:15:00 +0000
#   Output: 2015-11-04 08:15:00
def weblog2Time(weblog_timestr):
  weblog_time = time.strptime(weblog_timestr, "%d/%b/%Y:%H:%M:%S +0000")
  weblog_t = time.mktime(weblog_time)
  return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(weblog_t))

# Define UDF
udfWeblog2Time = udf(weblog2Time, DateType())

# Register the UDF
sqlContext.registerFunction("udfWeblog2Time", lambda x: weblog2Time(x))

# Create the UserID and Join back to the UserAgentInfo DataFrame and mappedIP3 DataFrame
accessLogsPrime = sqlContext.sql("select hash(a.ip1, a.agent) as UserId, m.cca3, udfWeblog2Time(a.datetime) as datetime, u.browserFamily, u.OSFamily, a.endpoint, a.referrer, a.method, a.responsecode, a.contentsize from accesslog a join UserAgentInfo u on u.agent = a.agent join mappedIP3 m on m.ip = a.ip1")

# Register the DataFrame as a table
accessLogsPrime.registerTempTable("accessLogsPrime")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Cache the table for faster queries
# MAGIC cache table accessLogsPrime

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Review the top 10 rows from this table
# MAGIC select UserId, datetime, cca3, browserFamily, OSFamily, method, responseCode, contentSize from accessLogsPrime limit 10;

# COMMAND ----------

# MAGIC %sql select browserFamily, count(distinct UserID) as Users, count(1) as Events from accessLogsPrime group by browserFamily order by Users desc limit 5;

# COMMAND ----------

# MAGIC %sql select hour(datetime) as Hour, count(1) as events from accessLogsPrime group by hour(datetime) order by hour(datetime)

# COMMAND ----------

# MAGIC %sql select browserFamily, count(1) FROM UserAgentInfo GROUP BY browserFamily

# COMMAND ----------

# MAGIC %sql select OSFamily, count(distinct UserID) as Users from accessLogsPrime group by OSFamily order by Users desc limit 10;

# COMMAND ----------

# MAGIC %sql select cca3, count(distinct UserID) as users from accessLogsPrime group by cca3

# COMMAND ----------

