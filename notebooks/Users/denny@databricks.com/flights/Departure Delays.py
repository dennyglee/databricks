# Databricks notebook source exported at Mon, 28 Mar 2016 16:01:31 UTC
# MAGIC %md # Flight Departure Delays
# MAGIC Authors: Denny Lee, Joseph Bradley, and Bill Chambers
# MAGIC 
# MAGIC This notebook provides different views of the Flight Departure Delays data using GraphFrames.  
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
# MAGIC * Joseph Bradley's GraphFrames Blog Post (Python) [Link to provided soon]
# MAGIC * [D3 Airports Example](http://mbostock.github.io/d3/talk/20111116/airports.html)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Preparation
# MAGIC Extract the Airports and Departure Delays information from S3 / DBFS

# COMMAND ----------

# Set File Paths
tripdelaysFilePath = "/mnt/tardis6/departuredelays/departuredelays.csv"
airportsnaFilePath = "/mnt/tardis6/airportcodes/na/airport-codes-na.txt"

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

# Include airports with atleast one trip from the departureDelays dataset
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

import pyspark.sql.functions as func
topTrips = tripGraph \
  .edges \
  .groupBy("src", "dst") \
  .agg(func.count("delay").alias("trips")) 

  # Show the top 20 most popular flights (single city hops)
display(topTrips.orderBy(topTrips.trips.desc()).limit(20))

# COMMAND ----------

# Degrees
#  The number of degrees - the number of incoming and outgoing connections - for various airports within this sample dataset
# display(tripGraph.degrees.orderBy("degree").limit(20))
display(tripGraph.degrees.limit(20))

# COMMAND ----------

# Finding the longest Delay
longestDelay = tripGraph.edges.groupBy().max("delay")
display(longestDelay)

# COMMAND ----------

tripDelays = tripGraph.edges.filter("delay > 0")
display(tripDelays)

# COMMAND ----------

# States with the longest cummulative delays (with individual delays > 100 minutes) (origin: Seattle)
srcSeattleByState = tripGraph.edges.filter("src = 'SEA' and delay > 100")
display(srcSeattleByState)

# COMMAND ----------

# Determining number of on-time / early flights vs. delayed flights
print "On-time / Early Flights: %d, Delayed Flights: %d" % (tripGraph.edges.filter("delay <= 0").count(), tripGraph.edges.filter("delay > 0").count())

# COMMAND ----------

#
# Traversing the tripGraph 
#
# Cities from top 3 states with the longest cummulative delays(origin: Seattle)
srcSeattleTopCACities = tripGraph.edges.filter("src = 'SEA' and state_dst in ('CA', 'CO', 'TX') and delay > 0")

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
# MAGIC -- Querying the original DataFrame for comparison purposes
# MAGIC --
# MAGIC -- Review on-time (or early arrivals
# MAGIC select city_dst, state_dst, count(1), sum(-1.*delay) as ontime from departuredelays_geo where state_dst in ('CA', 'AK', 'AZ') and city_src = 'Seattle' and delay <= 0 group by city_dst, state_dst order by sum(-1.*delay)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## City / Flight Relationships through Motif Finding
# MAGIC To more easily understand the complex relationship of city airports and their flights with each other, we can use motifs to find the pairs of airports (i.e. vertices) with flights (i.e. edges) in both directions.  The result is a dataframe in which the column names are given by the motif keys.

# COMMAND ----------

# Creating the Motif
#   Using tripGraphPrime to more easily display the associated edge (e, e2) relationships 
#   in comparison to the city / airports (a, b).  Note, we are also filtering so that the first edge flight (e) is before the second edge flight (e2)
#   as the TripID is generated based on time.
motifs = tripGraphPrime.find("(a)-[e]->(b); (b)-[e2]->(a)").filter("(a.id = 'SFO' or b.id = 'SFO') and (e.delay > 500 or e2.delay > 500) and e2.tripid > e.tripid")
display(motifs)

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
display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20))

# COMMAND ----------



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



# COMMAND ----------

# MAGIC %md ## Top Transfer Cities
# MAGIC Many airports are used as transfer points instead of the final Destination.  An easy way to calculate this is by calculating the ratio of inDegrees (the number of flights to the airport) / outDegrees (the number of flights leaving the airport).  Note, this is a simple calculation that does not take into account of timing or scheduling of flights, just the overall aggregate number within the dataset.

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

# Example 1: Seattle to San Francisco 
results = tripGraph.shortestPaths(landmarks=["SEA", "SFO"])
display(results.orderBy("id"))

# COMMAND ----------

# MAGIC %md As can be seen in this graph, there are direct flights from Seattle to San Franciso and vice versa where the distances are `{"SEA":0, "SFO":1}` and `{"SFO":0, "SEA":1}` respectively.  

# COMMAND ----------

# Example 2: Seattle to Atlanta 
results = tripGraph.shortestPaths(landmarks=["SEA", "ATL"])
display(results.orderBy("id"))

# COMMAND ----------

# MAGIC %md While there is a direct flight from Seattle to Atlanta, there are also a number of cities where using these two landmarks would still not result in a connection - i.e. where `distances = {}`. 

# COMMAND ----------

# Example 2': Seattle to Atlanta
#   Filter for only distances that have size(distances) = 0
display(results.select("id", "city", "state", "distances").filter("size(distances) = 0"))

# COMMAND ----------

# Example 3: San Francisco to Buffalo 
results = tripGraph.shortestPaths(landmarks=["SFO", "BUF"])
display(results.select("id", "city", "state", "distances").filter("size(distances) > 0").orderBy("id"))

# COMMAND ----------

# MAGIC %md Notice you can get from San Francisco to Buffalo with one stop though there are various other multi-stop trips as well.

# COMMAND ----------

# Example 4: Bellingham to Corpus Christi
results = tripGraph.shortestPaths(landmarks=["BLI", "CRP"])
display(results.select("id", "city", "state", "distances").filter("size(distances) > 0").orderBy("id"))

# COMMAND ----------

# MAGIC %md Getting from Bellingham to Corpus Christi is a bit trickier as there are no directs and would need to route through multiple cities.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Trips Forced Graph View
# MAGIC This is a forced graph D3 visualization of the tripGraph GraphFrame

# COMMAND ----------

# MAGIC %scala
# MAGIC package d3
# MAGIC // We use a package object so that we can define top level classes like Edge that need to be used in other cells
# MAGIC 
# MAGIC import org.apache.spark.sql._
# MAGIC import com.databricks.backend.daemon.driver.EnhancedRDDFunctions.displayHTML
# MAGIC 
# MAGIC case class Vertex(id: String, name: String)
# MAGIC case class Edge(src: String, dest: String, name: String)
# MAGIC 
# MAGIC case class Node(i: Int, id: String, name: String)
# MAGIC case class Link(source: Int, target: Int, name: String)
# MAGIC case class Graph(nodes: Seq[Node], links: Seq[Link])
# MAGIC 
# MAGIC object graphs {
# MAGIC val sqlContext = SQLContext.getOrCreate(org.apache.spark.SparkContext.getOrCreate())  
# MAGIC import sqlContext.implicits._
# MAGIC   
# MAGIC def force(vertices: Dataset[Vertex], edges: Dataset[Edge], height: Int = 100, width: Int = 960): Unit = {
# MAGIC   val nodes = vertices.collect().toSeq.sortBy(_.id).zipWithIndex.map { case (n, i) =>
# MAGIC     Node(i, n.id, n.name)
# MAGIC   }
# MAGIC   val links = edges.collect().map { e =>
# MAGIC     Link(nodes.find(_.id == e.src).get.i, nodes.find(_.id == e.dest).get.i, e.name)
# MAGIC   }
# MAGIC   showGraph(height, width, Seq(Graph(nodes, links)).toDF().toJSON.first())
# MAGIC }
# MAGIC 
# MAGIC /**
# MAGIC  * Displays a force directed graph using d3
# MAGIC  * input: {"nodes": [{"name": "..."}], "links": [{"source": 1, "target": 2, "name": "Bob"}]}
# MAGIC  */
# MAGIC def showGraph(height: Int, width: Int, graph: String): Unit = {
# MAGIC 
# MAGIC displayHTML(s"""
# MAGIC <!DOCTYPE html>
# MAGIC <html>
# MAGIC <head>
# MAGIC   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
# MAGIC   <title>Plotting graphs</title>
# MAGIC   <meta charset="utf-8">
# MAGIC <style>
# MAGIC 
# MAGIC .node_circle {
# MAGIC   stroke: #777;
# MAGIC   stroke-width: 1.3px;
# MAGIC }
# MAGIC 
# MAGIC .node_label {
# MAGIC   pointer-events: none;
# MAGIC }
# MAGIC 
# MAGIC .link {
# MAGIC   stroke: #777;
# MAGIC   stroke-opacity: .2;
# MAGIC }
# MAGIC 
# MAGIC .node_name {
# MAGIC   stroke: #777;
# MAGIC   stroke-width: 1.0px;
# MAGIC   fill: #999;
# MAGIC }
# MAGIC 
# MAGIC text.legend {
# MAGIC   font-family: Verdana;
# MAGIC   font-size: 13px;
# MAGIC   fill: #000;
# MAGIC }
# MAGIC 
# MAGIC .node text {
# MAGIC   font-family: "Helvetica Neue","Helvetica","Arial",sans-serif;
# MAGIC   font-size: 17px;
# MAGIC   font-weight: 200;
# MAGIC }
# MAGIC 
# MAGIC </style>
# MAGIC </head>
# MAGIC 
# MAGIC <body>
# MAGIC <script src="//d3js.org/d3.v3.min.js"></script>
# MAGIC <script>
# MAGIC 
# MAGIC var graph = $graph;
# MAGIC 
# MAGIC var width = $width,
# MAGIC     height = $height;
# MAGIC 
# MAGIC var color = d3.scale.category20();
# MAGIC 
# MAGIC var force = d3.layout.force()
# MAGIC     .charge(-700)
# MAGIC     .linkDistance(180)
# MAGIC     .size([width, height]);
# MAGIC 
# MAGIC var svg = d3.select("body").append("svg")
# MAGIC     .attr("width", width)
# MAGIC     .attr("height", height);
# MAGIC     
# MAGIC force
# MAGIC     .nodes(graph.nodes)
# MAGIC     .links(graph.links)
# MAGIC     .start();
# MAGIC 
# MAGIC var link = svg.selectAll(".link")
# MAGIC     .data(graph.links)
# MAGIC     .enter().append("line")
# MAGIC     .attr("class", "link")
# MAGIC     .style("stroke-width", function(d) { return d.name; });
# MAGIC 
# MAGIC var node = svg.selectAll(".node")
# MAGIC     .data(graph.nodes)
# MAGIC     .enter().append("g")
# MAGIC     .attr("class", "node")
# MAGIC     .call(force.drag);
# MAGIC 
# MAGIC node.append("circle")
# MAGIC     .attr("r", 10)
# MAGIC     .style("fill", function (d) {
# MAGIC     if (d.name.startsWith("other")) { return color(1); } else { return color(2); };
# MAGIC })
# MAGIC 
# MAGIC node.append("text")
# MAGIC       .attr("dx", 10)
# MAGIC       .attr("dy", ".35em")
# MAGIC       .text(function(d) { return d.name });
# MAGIC       
# MAGIC //Now we are giving the SVGs co-ordinates - the force layout is generating the co-ordinates which this code is using to update the attributes of the SVG elements
# MAGIC force.on("tick", function () {
# MAGIC     link.attr("x1", function (d) {
# MAGIC         return d.source.x;
# MAGIC     })
# MAGIC         .attr("y1", function (d) {
# MAGIC         return d.source.y;
# MAGIC     })
# MAGIC         .attr("x2", function (d) {
# MAGIC         return d.target.x;
# MAGIC     })
# MAGIC         .attr("y2", function (d) {
# MAGIC         return d.target.y;
# MAGIC     });
# MAGIC     d3.selectAll("circle").attr("cx", function (d) {
# MAGIC         return d.x;
# MAGIC     })
# MAGIC         .attr("cy", function (d) {
# MAGIC         return d.y;
# MAGIC     });
# MAGIC     d3.selectAll("text").attr("x", function (d) {
# MAGIC         return d.x;
# MAGIC     })
# MAGIC         .attr("y", function (d) {
# MAGIC         return d.y;
# MAGIC     });
# MAGIC });
# MAGIC </script>
# MAGIC </html>
# MAGIC """)
# MAGIC }
# MAGIC   
# MAGIC   def help() = {
# MAGIC displayHTML("""
# MAGIC <p>
# MAGIC Produces a force-directed graph given a collection of edges of the following form:</br>
# MAGIC <tt><font color="#a71d5d">case class</font> <font color="#795da3">Edge</font>(<font color="#ed6a43">src</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">dest</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">name</font>: <font color="#a71d5d">Long</font>)</tt>
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

# MAGIC %md In order to display a graph with some Scala code, we register the vertices and the edges as SQL tables. They are now accessible in the Scala code.

# COMMAND ----------

# MAGIC %scala d3.graphs.help()

# COMMAND ----------

tripVertices.registerTempTable("tripVertices")
tripEdges.registerTempTable("tripEdges")

# COMMAND ----------

# MAGIC %scala
# MAGIC import d3._
# MAGIC 
# MAGIC d3.graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1000,
# MAGIC   vertices = sql("""
# MAGIC     SELECT CAST(id as string), CAST(City as string) as name FROM tripVertices""").as[Vertex],
# MAGIC   edges = sql("""
# MAGIC     SELECT CAST(src as string), CAST(dst as string) as dest, CAST(max(city_dst) as string) as name
# MAGIC     FROM tripEdges GROUP BY src, dst ORDER BY count(1) desc LIMIT 500""").as[Edge])

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## TODO: 
# MAGIC * Include [Airports Graph](http://mbostock.github.io/d3/talk/20111116/airports.html)