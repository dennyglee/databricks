# Databricks notebook source exported at Mon, 28 Mar 2016 16:02:51 UTC
# MAGIC %md
# MAGIC *If you see ![](http://training.databricks.com/databricks_guide/ImportNotebookIcon3.png) at the top-left or top-right, click on the link to import this notebook in order to run it.* 

# COMMAND ----------

# MAGIC %md # On-Time Flight Performance with GraphFrames for Apache Spark
# MAGIC This notebook provides an analysis of On-Time Flight Performance and Departure Delays data using GraphFrames for Apache Spark.
# MAGIC 
# MAGIC Source Data: 
# MAGIC * [OpenFlights: Airport, airline and route data](http://openflights.org/data.html)
# MAGIC * [United States Department of Transportation: Bureau of Transportation Statistics (TranStats)](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time)
# MAGIC  * Note, the data used here was extracted from the US DOT:BTS between 1/1/2014 and 3/31/2014*
# MAGIC 
# MAGIC References:
# MAGIC * [GraphFrames User Guide](http://graphframes.github.io/user-guide.html)
# MAGIC * [GraphFrames: DataFrame-based Graphs (GitHub)](https://github.com/graphframes/graphframes)
# MAGIC * [D3 Airports Example](http://mbostock.github.io/d3/talk/20111116/airports.html)

# COMMAND ----------

# MAGIC %md ### Preparation
# MAGIC Extract the Airports and Departure Delays information from S3 / DBFS

# COMMAND ----------

# Set File Paths
tripdelaysFilePath = "/databricks-datasets/flights/departuredelays.csv"
airportsnaFilePath = "/databricks-datasets/flights/airport-codes-na.txt"

# Obtain airports dataset
airportsna = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true', delimiter='\t').load(airportsnaFilePath)
airportsna.registerTempTable("airports_na")

# Obtain departure Delays data
departureDelays = sqlContext.read.format("com.databricks.spark.csv").options(header='true').load(tripdelaysFilePath)
departureDelays.registerTempTable("departureDelays")
departureDelays.cache()

# Available IATA codes from the departuredelays sample dataset
tripIATA = sqlContext.sql("select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
tripIATA.registerTempTable("tripIATA")

# Only include airports with atleast one trip from the departureDelays dataset
airports = sqlContext.sql("select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
airports.registerTempTable("airports")
airports.cache()

# COMMAND ----------

# Build `departureDelays_geo` DataFrame
#  Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)  
departureDelays_geo = sqlContext.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination") 

# RegisterTempTable
departureDelays_geo.registerTempTable("departureDelays_geo")

# Cache and Count
departureDelays_geo.cache()
departureDelays_geo.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building the Graph
# MAGIC Now that we've imported our data, we're going to need to build our graph. To do so we're going to do two things. We are going to build the structure of the vertices (or nodes) and we're going to build the structure of the edges. What's awesome about GraphFrames is that this process is incredibly simple. 
# MAGIC * Rename IATA airport code to **id** in the Vertices Table
# MAGIC * Start and End airports to **src** and **dst** for the Edges Table (flights)
# MAGIC 
# MAGIC These are required naming conventions for vertices and edges in GraphFrames as of the time of this writing (Feb. 2016).

# COMMAND ----------

# MAGIC %md **WARNING:** If the graphframes package, required in the cell below, is not installed, follow the instructions [here](http://cdn2.hubspot.net/hubfs/438089/notebooks/help/Setup_graphframes_package.html).

# COMMAND ----------

# Note, ensure you have already installed the GraphFrames spack-package
from pyspark.sql.functions import *
from graphframes import *

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

# Build `tripGraphPrime` GraphFrame
#   This graphframe contains a smaller subset of data to make it easier to display motifs and subgraphs (below)
tripEdgesPrime = departureDelays_geo.select("tripid", "delay", "src", "dst")
tripGraphPrime = GraphFrame(tripVertices, tripEdgesPrime)

# COMMAND ----------

# MAGIC %md ## Simple Queries
# MAGIC Let's start with a set of simple graph queries to understand flight performance and departure delays

# COMMAND ----------

# MAGIC %md #### Determine the number of airports and trips

# COMMAND ----------

print "Airports: %d" % tripGraph.vertices.count()
print "Trips: %d" % tripGraph.edges.count()


# COMMAND ----------

# MAGIC %md #### Determining the longest delay in this dataset

# COMMAND ----------

# Finding the longest Delay
longestDelay = tripGraph.edges.groupBy().max("delay")
display(longestDelay)

# COMMAND ----------

# MAGIC %md #### Determining the number of delayed vs. on-time / early flights

# COMMAND ----------

# Determining number of on-time / early flights vs. delayed flights
print "On-time / Early Flights: %d" % tripGraph.edges.filter("delay <= 0").count()
print "Delayed Flights: %d" % tripGraph.edges.filter("delay > 0").count()

# COMMAND ----------

# MAGIC %md #### What flights departing SFO are most likely to have significant delays
# MAGIC Note, delay can be <= 0 meaning the flight left on time or early

# COMMAND ----------

tripGraph.edges\
  .filter("src = 'SFO' and delay > 0")\
  .groupBy("src", "dst")\
  .avg("delay")\
  .sort(desc("avg(delay)"))

# COMMAND ----------

display(tripGraph.edges.filter("src = 'SFO' and delay > 0").groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")))

# COMMAND ----------

# MAGIC %md #### What destinations tend to have delays

# COMMAND ----------

# After displaying tripDelays, use Plot Options to set `state_dst` as a Key.
tripDelays = tripGraph.edges.filter("delay > 0")
display(tripDelays)

# COMMAND ----------

# MAGIC %md #### What destinations tend to have significant delays departing from SEA

# COMMAND ----------

# States with the longest cumulative delays (with individual delays > 100 minutes) (origin: Seattle)
display(tripGraph.edges.filter("src = 'SEA' and delay > 100"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Vertex Degrees
# MAGIC * `inDegrees`: Incoming connections to the airport
# MAGIC * `outDegrees`: Outgoing connections from the airport 
# MAGIC * `degrees`: Total connections to and from the airport
# MAGIC 
# MAGIC Reviewing the various properties of the property graph to understand the incoming and outgoing connections between airports.

# COMMAND ----------

# Degrees
#  The number of degrees - the number of incoming and outgoing connections - for various airports within this sample dataset
display(tripGraph.degrees.sort(desc("degree")).limit(20))

# COMMAND ----------

# MAGIC %md ## City / Flight Relationships through Motif Finding
# MAGIC To more easily understand the complex relationship of city airports and their flights with each other, we can use motifs to find patterns of airports (i.e. vertices) connected by flights (i.e. edges). The result is a DataFrame in which the column names are given by the motif keys.

# COMMAND ----------

# MAGIC %md #### What delays might we blame on SFO

# COMMAND ----------

# Using tripGraphPrime to more easily display 
#   - The associated edge (ab, bc) relationships 
#   - With the different the city / airports (a, b, c) where SFO is the connecting city (b)
#   - Ensuring that flight ab (i.e. the flight to SFO) occured before flight bc (i.e. flight leaving SFO)
#   - Note, TripID was generated based on time in the format of MMDDHHMM converted to int
#       - Therefore bc.tripid < ab.tripid + 10000 means the second flight (bc) occured within approx a day of the first flight (ab)
# Note: In reality, we would need to be more careful to link trips ab and bc.
motifs = tripGraphPrime.find("(a)-[ab]->(b); (b)-[bc]->(c)")\
  .filter("(b.id = 'SFO') and (ab.delay > 500 or bc.delay > 500) and bc.tripid > ab.tripid and bc.tripid < ab.tripid + 10000")
display(motifs)

# COMMAND ----------

# MAGIC %md ## Determining Airport Ranking using PageRank
# MAGIC There are a large number of flights and connections through these various airports included in this Departure Delay Dataset.  Using the `pageRank` algorithm, Spark iteratively traverses the graph and determines a rough estimate of how important the airport is.

# COMMAND ----------

# Determining Airport ranking of importance using `pageRank`
ranks = tripGraph.pageRank(resetProbability=0.15, maxIter=5)
display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20))

# COMMAND ----------

# MAGIC %md ## Most popular flights (single city hops)
# MAGIC Using the `tripGraph`, we can quickly determine what are the most popular single city hop flights

# COMMAND ----------

# Determine the most popular flights (single city hops)
import pyspark.sql.functions as func
topTrips = tripGraph \
  .edges \
  .groupBy("src", "dst") \
  .agg(func.count("delay").alias("trips")) 

# COMMAND ----------

# Show the top 20 most popular flights (single city hops)
display(topTrips.orderBy(topTrips.trips.desc()).limit(20))

# COMMAND ----------

# MAGIC %md ## Top Transfer Cities
# MAGIC Many airports are used as transfer points instead of the final Destination.  An easy way to calculate this is by calculating the ratio of inDegree (the number of flights to the airport) / outDegree (the number of flights leaving the airport).  Values close to 1 may indicate many transfers, whereas values < 1 indicate many outgoing flights and > 1 indicate many incoming flights.  Note, this is a simple calculation that does not take into account of timing or scheduling of flights, just the overall aggregate number within the dataset.

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
nonTransferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio < .9 or degreeRatio > 1.1")

# List out the city airports which have abnormal degree ratios.
display(nonTransferAirports)

# COMMAND ----------

# Join back to the `airports` DataFrame (instead of registering temp table as above)
transferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio between 0.9 and 1.1")
  
# List out the top 10 transfer city airports
display(transferAirports.orderBy("degreeRatio").limit(10))

# COMMAND ----------

# MAGIC %md ## Breadth First Search 
# MAGIC Breadth-first search (BFS) is designed to traverse the graph to quickly find the desired vertices (i.e. airports) and edges (i.e flights).  Let's try to find the shortest number of connections between cities based on the dataset.  Note, these examples do not take into account of time or distance, just hops between cities.

# COMMAND ----------

# Example 1: Direct Seattle to San Francisco 
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SEA'",
  toExpr = "id = 'SFO'",
  maxPathLength = 1)
display(filteredPaths)

# COMMAND ----------

# MAGIC %md As you can see, there are a number of direct flights between Seattle and San Francisco.

# COMMAND ----------

# Example 2: Direct San Francisco and Buffalo
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SFO'",
  toExpr = "id = 'BUF'",
  maxPathLength = 1)
display(filteredPaths)

# COMMAND ----------

# MAGIC %md But there are no direct flights between San Francisco and Buffalo.

# COMMAND ----------

# Example 2a: Flying from San Francisco to Buffalo
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SFO'",
  toExpr = "id = 'BUF'",
  maxPathLength = 2)
display(filteredPaths)

# COMMAND ----------

# MAGIC %md But there are flights from San Francisco to Buffalo with Minneapolis as the transfer point.

# COMMAND ----------

# MAGIC %md ## Loading the D3 Visualization
# MAGIC Using the airports D3 visualization to visualize airports and flight paths

# COMMAND ----------

# MAGIC %scala
# MAGIC package d3a
# MAGIC // We use a package object so that we can define top level classes like Edge that need to be used in other cells
# MAGIC 
# MAGIC import org.apache.spark.sql._
# MAGIC import com.databricks.backend.daemon.driver.EnhancedRDDFunctions.displayHTML
# MAGIC 
# MAGIC case class Edge(src: String, dest: String, count: Long)
# MAGIC 
# MAGIC case class Node(name: String)
# MAGIC case class Link(source: Int, target: Int, value: Long)
# MAGIC case class Graph(nodes: Seq[Node], links: Seq[Link])
# MAGIC 
# MAGIC object graphs {
# MAGIC val sqlContext = SQLContext.getOrCreate(org.apache.spark.SparkContext.getOrCreate())
# MAGIC import sqlContext.implicits._
# MAGIC 
# MAGIC def force(clicks: Dataset[Edge], height: Int = 100, width: Int = 960): Unit = {
# MAGIC   val data = clicks.collect()
# MAGIC   val nodes = (data.map(_.src) ++ data.map(_.dest)).map(_.replaceAll("_", " ")).toSet.toSeq.map(Node)
# MAGIC   val links = data.map { t =>
# MAGIC     Link(nodes.indexWhere(_.name == t.src.replaceAll("_", " ")), nodes.indexWhere(_.name == t.dest.replaceAll("_", " ")), t.count / 20 + 1)
# MAGIC   }
# MAGIC   showGraph(height, width, Seq(Graph(nodes, links)).toDF().toJSON.first())
# MAGIC }
# MAGIC 
# MAGIC /**
# MAGIC  * Displays a force directed graph using d3
# MAGIC  * input: {"nodes": [{"name": "..."}], "links": [{"source": 1, "target": 2, "value": 0}]}
# MAGIC  */
# MAGIC def showGraph(height: Int, width: Int, graph: String): Unit = {
# MAGIC 
# MAGIC displayHTML(s"""<!DOCTYPE html>
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <link type="text/css" rel="stylesheet" href="https://mbostock.github.io/d3/talk/20111116/style.css"/>
# MAGIC     <style type="text/css">
# MAGIC       #states path {
# MAGIC         fill: #ccc;
# MAGIC         stroke: #fff;
# MAGIC       }
# MAGIC 
# MAGIC       path.arc {
# MAGIC         pointer-events: none;
# MAGIC         fill: none;
# MAGIC         stroke: #000;
# MAGIC         display: none;
# MAGIC       }
# MAGIC 
# MAGIC       path.cell {
# MAGIC         fill: none;
# MAGIC         pointer-events: all;
# MAGIC       }
# MAGIC 
# MAGIC       circle {
# MAGIC         fill: steelblue;
# MAGIC         fill-opacity: .8;
# MAGIC         stroke: #fff;
# MAGIC       }
# MAGIC 
# MAGIC       #cells.voronoi path.cell {
# MAGIC         stroke: brown;
# MAGIC       }
# MAGIC 
# MAGIC       #cells g:hover path.arc {
# MAGIC         display: inherit;
# MAGIC       }
# MAGIC     </style>
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.js"></script>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.csv.js"></script>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.geo.js"></script>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.geom.js"></script>
# MAGIC     <script>
# MAGIC       var graph = $graph;
# MAGIC       var w = $width;
# MAGIC       var h = $height;
# MAGIC 
# MAGIC       var linksByOrigin = {};
# MAGIC       var countByAirport = {};
# MAGIC       var locationByAirport = {};
# MAGIC       var positions = [];
# MAGIC 
# MAGIC       var projection = d3.geo.azimuthal()
# MAGIC           .mode("equidistant")
# MAGIC           .origin([-98, 38])
# MAGIC           .scale(1400)
# MAGIC           .translate([640, 360]);
# MAGIC 
# MAGIC       var path = d3.geo.path()
# MAGIC           .projection(projection);
# MAGIC 
# MAGIC       var svg = d3.select("body")
# MAGIC           .insert("svg:svg", "h2")
# MAGIC           .attr("width", w)
# MAGIC           .attr("height", h);
# MAGIC 
# MAGIC       var states = svg.append("svg:g")
# MAGIC           .attr("id", "states");
# MAGIC 
# MAGIC       var circles = svg.append("svg:g")
# MAGIC           .attr("id", "circles");
# MAGIC 
# MAGIC       var cells = svg.append("svg:g")
# MAGIC           .attr("id", "cells");
# MAGIC 
# MAGIC       var arc = d3.geo.greatArc()
# MAGIC           .source(function(d) { return locationByAirport[d.source]; })
# MAGIC           .target(function(d) { return locationByAirport[d.target]; });
# MAGIC 
# MAGIC       d3.select("input[type=checkbox]").on("change", function() {
# MAGIC         cells.classed("voronoi", this.checked);
# MAGIC       });
# MAGIC 
# MAGIC       // Draw US map.
# MAGIC       d3.json("https://mbostock.github.io/d3/talk/20111116/us-states.json", function(collection) {
# MAGIC         states.selectAll("path")
# MAGIC           .data(collection.features)
# MAGIC           .enter().append("svg:path")
# MAGIC           .attr("d", path);
# MAGIC       });
# MAGIC 
# MAGIC       // Parse links
# MAGIC       graph.links.forEach(function(link) {
# MAGIC         var origin = graph.nodes[link.source].name;
# MAGIC         var destination = graph.nodes[link.target].name;
# MAGIC 
# MAGIC         var links = linksByOrigin[origin] || (linksByOrigin[origin] = []);
# MAGIC         links.push({ source: origin, target: destination });
# MAGIC 
# MAGIC         countByAirport[origin] = (countByAirport[origin] || 0) + 1;
# MAGIC         countByAirport[destination] = (countByAirport[destination] || 0) + 1;
# MAGIC       });
# MAGIC 
# MAGIC       d3.csv("https://mbostock.github.io/d3/talk/20111116/airports.csv", function(data) {
# MAGIC 
# MAGIC         // Build list of airports.
# MAGIC         var airports = graph.nodes.map(function(node) {
# MAGIC           return data.find(function(airport) {
# MAGIC             if (airport.iata === node.name) {
# MAGIC               var location = [+airport.longitude, +airport.latitude];
# MAGIC               locationByAirport[airport.iata] = location;
# MAGIC               positions.push(projection(location));
# MAGIC 
# MAGIC               return true;
# MAGIC             } else {
# MAGIC               return false;
# MAGIC             }
# MAGIC           });
# MAGIC         });
# MAGIC 
# MAGIC         // Compute the Voronoi diagram of airports' projected positions.
# MAGIC         var polygons = d3.geom.voronoi(positions);
# MAGIC 
# MAGIC         var g = cells.selectAll("g")
# MAGIC             .data(airports)
# MAGIC           .enter().append("svg:g");
# MAGIC 
# MAGIC         g.append("svg:path")
# MAGIC             .attr("class", "cell")
# MAGIC             .attr("d", function(d, i) { return "M" + polygons[i].join("L") + "Z"; })
# MAGIC             .on("mouseover", function(d, i) { d3.select("h2 span").text(d.name); });
# MAGIC 
# MAGIC         g.selectAll("path.arc")
# MAGIC             .data(function(d) { return linksByOrigin[d.iata] || []; })
# MAGIC           .enter().append("svg:path")
# MAGIC             .attr("class", "arc")
# MAGIC             .attr("d", function(d) { return path(arc(d)); });
# MAGIC 
# MAGIC         circles.selectAll("circle")
# MAGIC             .data(airports)
# MAGIC             .enter().append("svg:circle")
# MAGIC             .attr("cx", function(d, i) { return positions[i][0]; })
# MAGIC             .attr("cy", function(d, i) { return positions[i][1]; })
# MAGIC             .attr("r", function(d, i) { return Math.sqrt(countByAirport[d.iata]); })
# MAGIC             .sort(function(a, b) { return countByAirport[b.iata] - countByAirport[a.iata]; });
# MAGIC       });
# MAGIC     </script>
# MAGIC   </body>
# MAGIC </html>""")
# MAGIC   }
# MAGIC 
# MAGIC   def help() = {
# MAGIC displayHTML("""
# MAGIC <p>
# MAGIC Produces a force-directed graph given a collection of edges of the following form:</br>
# MAGIC <tt><font color="#a71d5d">case class</font> <font color="#795da3">Edge</font>(<font color="#ed6a43">src</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">dest</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">count</font>: <font color="#a71d5d">Long</font>)</tt>
# MAGIC </p>
# MAGIC <p>Usage:<br/>
# MAGIC <tt>%scala</tt></br>
# MAGIC <tt><font color="#a71d5d">import</font> <font color="#ed6a43">d3._</font></tt><br/>
# MAGIC <tt><font color="#795da3">graphs.force</font>(</br>
# MAGIC &nbsp;&nbsp;<font color="#ed6a43">height</font> = <font color="#795da3">500</font>,<br/>
# MAGIC &nbsp;&nbsp;<font color="#ed6a43">width</font> = <font color="#795da3">500</font>,<br/>
# MAGIC &nbsp;&nbsp;<font color="#ed6a43">clicks</font>: <font color="#795da3">Dataset</font>[<font color="#795da3">Edge</font>])</tt>
# MAGIC </p>""")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala d3a.graphs.help()

# COMMAND ----------

# MAGIC %md #### Visualize On-time and Early Arrivals

# COMMAND ----------

# MAGIC %scala
# MAGIC // On-time and Early Arrivals
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo where delay <= 0 group by src, dst""").as[Edge])

# COMMAND ----------

# MAGIC %md #### Visualize Delayed Trips Departing from the West Coast
# MAGIC 
# MAGIC Notice that most of the delayed trips are with Western US cities

# COMMAND ----------

# MAGIC %scala
# MAGIC // Delayed Trips from CA, OR, and/or WA
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo where state_src in ('CA', 'OR', 'WA') and delay > 0 group by src, dst""").as[Edge])

# COMMAND ----------

# MAGIC %md #### Visualize All Flights (from this dataset)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Trips (from DepartureDelays Dataset)
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo group by src, dst""").as[Edge])