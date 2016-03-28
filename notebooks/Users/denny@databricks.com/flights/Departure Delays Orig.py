# Databricks notebook source exported at Mon, 28 Mar 2016 16:02:10 UTC
# MAGIC %md # Flight Departure Delays
# MAGIC Authors: Denny Lee, Joseph Bradley, and Bill Chambers
# MAGIC 
# MAGIC This notebook provides different views of the Flight Departure Delays data using DataFrames and GraphFrames.  
# MAGIC 
# MAGIC Example Departure Delay Reports:
# MAGIC * [2014 Flight Departure Performance via d3.js Crossfilter](http://dennyglee.com/2014/06/06/2014-flight-departure-performance-via-d3-js-crossfilter/)
# MAGIC * [2014 Flight Departure Performance via d3.js Crossfilter CodePen Sample](http://codepen.io/dennyglee/pen/pfahG)
# MAGIC 
# MAGIC Source Data: 
# MAGIC * [OpenFlights: Airport, airline and route data](http://openflights.org/data.html)
# MAGIC * [United States Department of Transportation: Bureau of Transportation Statistics](http://apps.bts.gov/xml/ontimesummarystatistics/src/index.xml)
# MAGIC * [United States Department of Transportation: Bureau of Transportation Statistics (TranStats)](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time)
# MAGIC * *Note, the data used here was extracted from the US DOT:BTS between 1/1/2014 and 3/31/2014*
# MAGIC 
# MAGIC 
# MAGIC GraphFrames Reference:
# MAGIC * Graph Analysis with GraphFrames [Link to be provided]
# MAGIC * [GraphFrames User Guide](http://graphframes.github.io/user-guide.html)
# MAGIC * [GraphFrames: DataFrame-based Graphs (GitHub)](https://github.com/graphframes/graphframes)
# MAGIC 
# MAGIC D3 Reference:
# MAGIC * Michael Armbrust's Spark Summit East 2016 [Wikipedia Clickstream Analysis](https://docs.cloud.databricks.com/docs/latest/featured_notebooks/Wikipedia%20Clickstream%20Data.html)
# MAGIC * [D3 Airports Example](http://mbostock.github.io/d3/talk/20111116/airports.html)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Preparation
# MAGIC [TODO] Instead of using Tables, access the data via CSV or mount

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Starting with the Airports Data
# MAGIC Airport information obtained from [OpenFlights: Airport, airline and route data](http://openflights.org/data.html) 
# MAGIC * Focus will be on flights within the United States as the BTS datasets focuses on US-origin and/or US-destination flights

# COMMAND ----------

# MAGIC %sql select * from airportcodes_na limit 10;

# COMMAND ----------

# Build `airports` DataFrame
airports = sqlContext.sql("select * from airportcodes_na")

# COMMAND ----------

display(airports)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Access the Departure Delays datasets
# MAGIC Starting with the Departure Delays datasets accessed from the [United States Department of Transportation: Bureau of Transportation Statistics](http://apps.bts.gov/xml/ontimesummarystatistics/src/index.xml), we will join the build the `departureDelays_geo` DataFrame that joins the departureDelays data and airport information.

# COMMAND ----------

# Build `departureDelays_geo` DataFrame
#  Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)  
departureDelays_geo = sqlContext.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(f.date, 1, 2), '-')), substr(f.date, 3, 2)), ' '), substr(f.date, 5, 2)), ':'), substr(f.date, 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airportcodes_na o on o.iata = f.origin join airportcodes_na d on d.iata = f.destination") 

# RegisterTempTable
departureDelays_geo.registerTempTable("departureDelays_geo")

# Cache and Count
departureDelays_geo.cache()
departureDelays_geo.count()

# COMMAND ----------

# View the `departureDelays_geo` DataFrame schema
departureDelays_geo.printSchema()

# COMMAND ----------

# View Data
display(departureDelays_geo)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- View states where there were originating flight delays (for this sample dataset)
# MAGIC select state_dst, count(1) as delays, sum(delay) as delayTime from departureDelays_geo where delay > 0 group by state_dst

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Building the Graph
# MAGIC Now that we've imported our data, we're going to need to build our graph. To do so we're going to do two things. We are going to build the structure of the vertices (or nodes) and we're going to build the structure of the edges. What's awesome about GraphFrames is that this process is incredibly simple. 
# MAGIC * Rename IATA airport code to **id** in the Vertices Table
# MAGIC * Start and End airports to **src** and **dst** for the Edges Table (flights)
# MAGIC 
# MAGIC These are required naming conventions for vertices and edges in GraphFrames as of the time of this writing (Feb. 2016).

# COMMAND ----------

from graphframes import *

# Import graphframes (from Spark-Packages)
import graphframes
from graphframes.examples import Graphs

# Create Vertices (airports) and Edges (flights)
tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
tripEdges = departureDelays_geo.select("tripid", "delay", "src", "dst", "city_dst", "state_dst")

# Cache Vertices and Edges
tripEdges.cache()
tripVertices.cache()

# COMMAND ----------

# Vertices
#   The vertices of our graph are the airports
display(tripVertices)

# COMMAND ----------

# Edges
#  The edges of our graph are the flights between airports
display(tripEdges)

# COMMAND ----------

# Build `tripGraph` GraphFrame
#  This GraphFrame builds up on the vertices and edges based on our trips (flights)
tripGraph = GraphFrame(tripVertices, tripEdges)
print tripGraph

# COMMAND ----------

# Build `tripGraphPrime` GraphFrame
#   This graphframe contains a smaller subset of data to make it easier to display motifs and subgraphs (below)
tripEdgesPrime = departureDelays_geo.select("tripid", "delay", "src", "dst")
tripGraphPrime = GraphFrame(tripVertices, tripEdgesPrime)

# COMMAND ----------

# Airports and Flight Information
print "Airports: %d, Flights / Trips: %d" % (tripGraph.vertices.count(), tripGraph.edges.count())

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Graphing the Airports (Vertices) and Trips (Edges)
# MAGIC Reviewing the various properties of the property graph to understand the incoming and outgoing connections between airports.

# COMMAND ----------

# Degrees
#  The number of degrees - the number of incoming and outgoing connections - for various airports within this sample dataset
display(tripGraph.degrees)

# COMMAND ----------

# Finding the longest Delay
longestDelay = tripGraph.edges.groupBy().max("delay")
display(longestDelay)

# COMMAND ----------

# States with the longest cummulative delays in March 2014 (origin: Seattle)
srcSeattleByState = tripGraph.edges.filter("src = 'SEA' and delay > 0")
display(srcSeattleByState)

# COMMAND ----------

# Determining number of on-time / early flights vs. delayed flights
print "On-time / Early Flights: %d, Delayed Flights: %d" % (tripGraph.edges.filter("delay <= 0").count(), tripGraph.edges.filter("delay > 0").count())

# COMMAND ----------

#
# Traversing the tripGraph 
#
# Cities from top 5 states with the longest cummulative delays in March 2014 (origin: Seattle)
srcSeattleTopCACities = tripGraph.edges.filter("src = 'SEA' and state_dst in ('CA', 'TX', 'FL', 'IL', 'CO') and delay > 0")

# Group By City and State for view
dfCities = srcSeattleTopCACities \
  .select("city_dst", "state_dst", "delay") \
  .groupBy("city_dst", "state_dst") \
  .agg({"delay": "sum"}) \
  .withColumnRenamed("SUM(delay)", "delays") \
  .orderBy("delays")

# Display this information
display(dfCities)


# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 
# MAGIC -- Querying the original DataFrame
# MAGIC --
# MAGIC -- Review on-time (or early arrivals
# MAGIC select city_dst, state_dst, count(1), sum(-1.*delay) as ontime from departuredelays_geo where state_dst in ('CA', 'TX', 'NV', 'AZ', 'CO') and city_src = 'Seattle' and delay <= 0 group by city_dst, state_dst order by sum(-1.*delay)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## City / Flight Relationships through Motif Finding
# MAGIC To more easily understand the complex relationship of city airports and their flights with each other, we can use motifs to find the pairs of airports (i.e. vertices) with flights (i.e. edges) in both directions.  The result is a dataframe in which the column names are given by the motif keys.

# COMMAND ----------

# Creating the Motif
#   Using tripGraphPrime to more easily display the associated edge (e, e2) relationships 
#   in comparison to the city / airports (a, b)
motifs = tripGraphPrime.find("(a)-[e]->(b); (b)-[e2]->(a)")
display(motifs)

# COMMAND ----------

# Filter the Motif
#  Filter the motifs for only flights (edges) where the delay > 500 minutes
#  and the origination (a) or destination (b) airport is 'SEA' (Seattle)
tripMotifs = motifs.filter("e.delay > 500 and a.id = 'SEA'")
display(tripMotifs)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## City / Flight Relationships through Subgraphs
# MAGIC Another way to find the relationships (per the motif filtering above) is to build a subgraph.  Below, we are building a subgraph where the originating airport is 'SEA' (Seattle) and for flights whre the delay > 500 minutes.

# COMMAND ----------

# Find paths where origin airport is Seattle and delay > 500 minutes
paths = tripGraph.find("(a)-[e]->(b)")\
  .filter("e.delay > 500")\
  .filter("a.id = 'SEA'")
  
# The `paths` variable contains the vertex information, which we can extract:
e2 = paths.select("e.src", "e.dst", "e.tripid")

# Construct subgraph
tripSubGraph = GraphFrame(tripGraph.vertices, e2)

# COMMAND ----------

# Potential Destination Cities where origin airport is Seattle
display(tripSubGraph.vertices)

# COMMAND ----------

# Flights that were > 500 minutes delayed (origin: SEA)
display(tripSubGraph.edges)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Determining Airport Ranking using PageRank
# MAGIC There are a large number of flights and connections through these various airports included in this Departure Delay Dataset.  Using the `pageRank` algorithm, Spark traverses the GraphFrames by counting the number and quality of links to determine a rough estimate of how important the airport is.

# COMMAND ----------

# Determining Airport ranking of importance using `pageRank`
ranks = tripGraph.pageRank(resetProbability=0.15, tol=0.01)
display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(10))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Most popular flights (single city hops)
# MAGIC Using the `tripGraph`, we can quickly determine what are the most popular single city hop flights

# COMMAND ----------

# Determine the most popular flights (single city hops)
topTrips = tripGraph \
  .edges \
  .groupBy("src", "dst") \
  .count() \

# Creating a DataFrame 
#    We're registering it so we can join back to the `airportcodes_na` table 
#    so we can see the origination and destination cities (instead of airport code)
topTrips.registerTempTable("topTrips")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- List out the top 10 by Trips (in descending order)
# MAGIC select a.city as Origin, b.city as Destination, t.count as Trips 
# MAGIC   from topTrips t
# MAGIC    join airportcodes_na a
# MAGIC      on a.iata = t.src
# MAGIC    join airportcodes_na b
# MAGIC      on b.iata = t.dst
# MAGIC  order by count desc limit 10;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Top Transfer Cities
# MAGIC Many airports are used as transfer points instead of the final Destination.  An easy way to calculate this is by caluclating the ratio of inDegrees (the number of flights to the airport) / outDegrees (the number of flights leaving the airport).  Note, this is a simple calculation that does not take into account of timing or scheduling of flights, just the overall aggregate number within the dataset.

# COMMAND ----------

airports.printSchema()

# COMMAND ----------

# Calculate the inDeg (flights into the airport) and outDeg (flights leaving the airport)
inDeg = tripGraph.inDegrees
outDeg = tripGraph.outDegrees

# Calculate the degreeRatio (inDeg/outDeg)
degreeRatio = inDeg.join(outDeg, inDeg.id == outDeg.id) \
  .drop(outDeg.id) \
  .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio") \
  .cache()

# Join back to the `airports` DataFrame (instead of registering temp table as above)
transferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio")
  
# List out the top 10 transfer city airports
display(transferAirports.orderBy("degreeRatio").limit(10))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Shortest Paths
# MAGIC With so many flights between various cities, you can use the `{GraphFrames}.shortestPath` method to determine the `distances` between two different cities.  In this case, the `distances` is the number of hops between one city to another (as opposed to the number of miles)

# COMMAND ----------

# Example 1: Seattle to Atlanta 
results = tripGraph.shortestPaths(landmarks=["SEA", "ATL"])
display(results)

# COMMAND ----------

# MAGIC %md Notice that there all the cities in the dataset are calculated against and many airports / cities are avoided with the output of `distances = {}`.  If you scroll through, you will see some Seattle to Atlanta paths such as: `Las Vegas, NV, USA, LAS, {"ATL":1, "SEA":1}`

# COMMAND ----------

# Example 1': Seattle to Atlanta
#   Filter for only distances that have size(distances) > 0
display(results.select("id", "city", "state", "distances").filter("size(distances) > 0"))

# COMMAND ----------

# Example 2: San Francisco to Buffalo 
results = tripGraph.shortestPaths(landmarks=["SFO", "BUF"])
display(results.select("id", "city", "state", "distances").filter("size(distances) > 0"))

# COMMAND ----------

# Example 3: Buffalo to Corpus Christi
results = tripGraph.shortestPaths(landmarks=["BLI", "CRP"])
display(results.select("id", "city", "state", "distances").filter("size(distances) > 0"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## TODO: 
# MAGIC * Include [Airports Graph](http://mbostock.github.io/d3/talk/20111116/airports.html)

# COMMAND ----------

