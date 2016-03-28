# Databricks notebook source exported at Mon, 28 Mar 2016 16:01:49 UTC
# MAGIC %md ## Departure Delays Airports D3 Visualization
# MAGIC 
# MAGIC Authors: Denny Lee and Jake Bellacera
# MAGIC 
# MAGIC Provides Departure Delays Airport D3 Visualization based on the [Airports D3](http://mbostock.github.io/d3/talk/20111116/airports.html).  
# MAGIC 
# MAGIC D3 Reference:
# MAGIC * Michael Armbrust's Spark Summit East 2016 [Wikipedia Clickstream Analysis](https://docs.cloud.databricks.com/docs/latest/featured_notebooks/Wikipedia%20Clickstream%20Data.html)
# MAGIC * Joseph Bradley's GraphFrames Blog Post (Python) [Link to provided soon]
# MAGIC * [D3 Airports Example](http://mbostock.github.io/d3/talk/20111116/airports.html)
# MAGIC 
# MAGIC Note: This notebook requires Firefox

# COMMAND ----------

#
# Note: This code does not need to be executed if already running the [Departure Delays] notebook
#

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

#
# Note: This code does not need to be executed if already running the [Departure Delays] notebook
#

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

# MAGIC %md ## Loading the D3 Visualization

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

# MAGIC %sql select * from departureDelays_geo limit 20;

# COMMAND ----------

# MAGIC %scala
# MAGIC // On-time and Early Arrivals
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo where delay <= 0 group by src, dst""").as[Edge])

# COMMAND ----------

# MAGIC %scala
# MAGIC // Delayed Trips from CA, OR, and/or WA
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo where state_src in ('CA', 'OR', 'WA') and delay > 0 group by src, dst""").as[Edge])

# COMMAND ----------

# MAGIC %scala
# MAGIC // Trips (from DepartureDelays Dataset)
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo group by src, dst""").as[Edge])

# COMMAND ----------

# MAGIC %scala 
# MAGIC displayHTML(s"""
# MAGIC <!DOCTYPE html>
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <link type="text/css" rel="stylesheet" href="https://mbostock.github.io/d3/talk/20111116/style.css"/>
# MAGIC     <style type="text/css">
# MAGIC 
# MAGIC #states path {
# MAGIC   fill: #ccc;
# MAGIC   stroke: #fff;
# MAGIC }
# MAGIC 
# MAGIC path.arc {
# MAGIC   pointer-events: none;
# MAGIC   fill: none;
# MAGIC   stroke: #000;
# MAGIC   display: none;
# MAGIC }
# MAGIC 
# MAGIC path.cell {
# MAGIC   fill: none;
# MAGIC   pointer-events: all;
# MAGIC }
# MAGIC 
# MAGIC circle {
# MAGIC   fill: steelblue;
# MAGIC   fill-opacity: .8;
# MAGIC   stroke: #fff;
# MAGIC }
# MAGIC 
# MAGIC #cells.voronoi path.cell {
# MAGIC   stroke: brown;
# MAGIC }
# MAGIC 
# MAGIC #cells g:hover path.arc {
# MAGIC   display: inherit;
# MAGIC }
# MAGIC 
# MAGIC     </style>
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.js"></script>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.csv.js"></script>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.geo.js"></script>
# MAGIC     <script src="https://mbostock.github.io/d3/talk/20111116/d3/d3.geom.js"></script>
# MAGIC     <script>
# MAGIC 
# MAGIC var w = 1280,
# MAGIC     h = 800;
# MAGIC 
# MAGIC var projection = d3.geo.azimuthal()
# MAGIC     .mode("equidistant")
# MAGIC     .origin([-98, 38])
# MAGIC     .scale(1400)
# MAGIC     .translate([640, 360]);
# MAGIC 
# MAGIC var path = d3.geo.path()
# MAGIC     .projection(projection);
# MAGIC 
# MAGIC var svg = d3.select("body").insert("svg:svg", "h2")
# MAGIC     .attr("width", w)
# MAGIC     .attr("height", h);
# MAGIC 
# MAGIC var states = svg.append("svg:g")
# MAGIC     .attr("id", "states");
# MAGIC 
# MAGIC var circles = svg.append("svg:g")
# MAGIC     .attr("id", "circles");
# MAGIC 
# MAGIC var cells = svg.append("svg:g")
# MAGIC     .attr("id", "cells");
# MAGIC 
# MAGIC d3.select("input[type=checkbox]").on("change", function() {
# MAGIC   cells.classed("voronoi", this.checked);
# MAGIC });
# MAGIC 
# MAGIC d3.json("https://mbostock.github.io/d3/talk/20111116/us-states.json", function(collection) {
# MAGIC   states.selectAll("path")
# MAGIC       .data(collection.features)
# MAGIC     .enter().append("svg:path")
# MAGIC       .attr("d", path);
# MAGIC });/flights/
# MAGIC 
# MAGIC d3.csv("http://go.databricks.com/hubfs/notebooks/flights/flights-airport.csv", function(flights) {
# MAGIC   var linksByOrigin = {},
# MAGIC       countByAirport = {},
# MAGIC       locationByAirport = {},
# MAGIC       positions = [];
# MAGIC 
# MAGIC   var arc = d3.geo.greatArc()
# MAGIC       .source(function(d) { return locationByAirport[d.source]; })
# MAGIC       .target(function(d) { return locationByAirport[d.target]; });
# MAGIC 
# MAGIC   flights.forEach(function(flight) {
# MAGIC     var origin = flight.origin,
# MAGIC         destination = flight.destination,
# MAGIC         links = linksByOrigin[origin] || (linksByOrigin[origin] = []);
# MAGIC     links.push({source: origin, target: destination});
# MAGIC     countByAirport[origin] = (countByAirport[origin] || 0) + 1;
# MAGIC     countByAirport[destination] = (countByAirport[destination] || 0) + 1;
# MAGIC   });
# MAGIC 
# MAGIC   d3.csv("https://mbostock.github.io/d3/talk/20111116/airports.csv", function(airports) {
# MAGIC 
# MAGIC     // Only consider airports with at least one flight.
# MAGIC     airports = airports.filter(function(airport) {
# MAGIC       if (countByAirport[airport.iata]) {
# MAGIC         var location = [+airport.longitude, +airport.latitude];
# MAGIC         locationByAirport[airport.iata] = location;
# MAGIC         positions.push(projection(location));
# MAGIC         return true;
# MAGIC       }
# MAGIC     });
# MAGIC 
# MAGIC     // Compute the Voronoi diagram of airports' projected positions.
# MAGIC     var polygons = d3.geom.voronoi(positions);
# MAGIC 
# MAGIC     var g = cells.selectAll("g")
# MAGIC         .data(airports)
# MAGIC       .enter().append("svg:g");
# MAGIC 
# MAGIC     g.append("svg:path")
# MAGIC         .attr("class", "cell")
# MAGIC         .attr("d", function(d, i) { return "M" + polygons[i].join("L") + "Z"; })
# MAGIC         .on("mouseover", function(d, i) { d3.select("h2 span").text(d.name); });
# MAGIC 
# MAGIC     g.selectAll("path.arc")
# MAGIC         .data(function(d) { return linksByOrigin[d.iata] || []; })
# MAGIC       .enter().append("svg:path")
# MAGIC         .attr("class", "arc")
# MAGIC         .attr("d", function(d) { return path(arc(d)); });
# MAGIC 
# MAGIC     circles.selectAll("circle")
# MAGIC         .data(airports)
# MAGIC       .enter().append("svg:circle")
# MAGIC         .attr("cx", function(d, i) { return positions[i][0]; })
# MAGIC         .attr("cy", function(d, i) { return positions[i][1]; })
# MAGIC         .attr("r", function(d, i) { return Math.sqrt(countByAirport[d.iata]); })
# MAGIC         .sort(function(a, b) { return countByAirport[b.iata] - countByAirport[a.iata]; });
# MAGIC   });
# MAGIC });
# MAGIC 
# MAGIC     </script>
# MAGIC   </body>
# MAGIC </html>""")

# COMMAND ----------

