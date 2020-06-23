# Databricks notebook source
# MAGIC %md ## NYT WA COVID-19 Analysis
# MAGIC This notebook processes and performs quick analysis from the [New York Times COVID-19 dataset](https://github.com/nytimes/covid-19-data).  The data is updated in the `/databricks-datasets/COVID/covid-19-data/` location regularly so you can access the data directly.

# COMMAND ----------

# Standard Libraries
import io

# External Libraries
import requests
import numpy as np
import pandas as pd
import altair as alt
from vega_datasets import data

# topographical
topo_usa = 'https://vega.github.io/vega-datasets/data/us-10m.json'
topo_wa = 'https://raw.githubusercontent.com/deldersveld/topojson/master/countries/us-states/WA-53-washington-counties.json'
topo_king = 'https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/WA/King.geo.json'

# COMMAND ----------

# MAGIC %md ### Download Mapping County FIPS to lat, long_

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/dennylee/COVID/map_fips/ && wget -O /dbfs/tmp/dennylee/COVID/map_fips/countyfips_lat_long.csv https://raw.githubusercontent.com/dennyglee/tech-talks/master/datasets/countyfips_lat_long.csv && ls -al /dbfs/tmp/dennylee/COVID/map_fips/

# COMMAND ----------

# Create mapping of county FIPS to centroid long_ and lat
map_fips = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/dennylee/COVID/map_fips/countyfips_lat_long.csv")
map_fips = (map_fips
              .withColumnRenamed("STATE", "state")
              .withColumnRenamed("COUNTYNAME", "county")
              .withColumnRenamed("LAT", "lat")
              .withColumnRenamed("LON", "long_"))
map_fips.createOrReplaceTempView("map_fips")

# COMMAND ----------

map_fips_dedup = spark.sql("""select fips, min(state) as state, min(county) as county, min(long_) as long_, min(lat) as lat from map_fips group by fips""")
map_fips_dedup.createOrReplaceTempView("map_fips_dedup")

# COMMAND ----------

# MAGIC %md ### Download 2019 Population Estimates

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/dennylee/COVID/population_estimates_by_county/ && wget -O /dbfs/tmp/dennylee/COVID/population_estimates_by_county/co-est2019-alldata.csv https://raw.githubusercontent.com/databricks/tech-talks/master/datasets/co-est2019-alldata.csv && ls -al /dbfs/tmp/dennylee/COVID/population_estimates_by_county/

# COMMAND ----------

map_popest_county = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/dennylee/COVID/population_estimates_by_county/co-est2019-alldata.csv")
map_popest_county.createOrReplaceTempView("map_popest_county")
fips_popest_county = spark.sql("select State * 1000 + substring(cast(1000 + County as string), 2, 3) as fips, STNAME, CTYNAME, census2010pop, POPESTIMATE2019 from map_popest_county")
fips_popest_county.createOrReplaceTempView("fips_popest_county")

# COMMAND ----------

# MAGIC %md ## Specify `nyt_daily` table
# MAGIC * Source: `/databricks-datasets/COVID/covid-19-data/`
# MAGIC * Contains the COVID-19 daily reports

# COMMAND ----------

# MAGIC %sh ls -lsgA /dbfs/databricks-datasets/COVID/covid-19-data/

# COMMAND ----------

nyt_daily = spark.read.option("inferSchema", True).option("header", True).csv("/databricks-datasets/COVID/covid-19-data/us-counties.csv")
nyt_daily.createOrReplaceTempView("nyt_daily")
display(nyt_daily)

# COMMAND ----------

# MAGIC %md # COVID-19 Cases and Deaths for Specific Counties

# COMMAND ----------

# WA State 2 week window
wa_state_window = spark.sql("""
SELECT date, 100 + datediff(date, '2020-03-06T00:00:00.000+0000') as day_num, county, fips, cases, deaths, 100000.*cases/population_estimate AS cases_per_100Kpop, 100000.*deaths/population_estimate AS deaths_per_100Kpop
  from (
SELECT CAST(f.date AS date) AS date, f.county, f.fips, SUM(f.cases) AS cases, SUM(f.deaths) AS deaths, MAX(p.POPESTIMATE2019) AS population_estimate 
  FROM nyt_daily f 
    JOIN fips_popest_county p
      ON p.fips = f.fips
 WHERE f.state = 'Washington' 
 GROUP BY f.date, f.county, f.fips
) a""")
wa_state_window.createOrReplaceTempView("wa_state")

# COMMAND ----------

# DBTITLE 1,WA State COVID-19 Confirmed Cases
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases 
# MAGIC   FROM wa_state f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases) as cases FROM wa_state WHERE date = (SELECT max(date) FROM wa_state) GROUP BY county ORDER BY cases DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county
# MAGIC  WHERE date >= '2020-04-01'

# COMMAND ----------

# DBTITLE 1,WA State COVID-19 Deaths 
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.deaths 
# MAGIC   FROM wa_state f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(deaths) as deaths FROM wa_state WHERE date = (SELECT max(date) FROM wa_state) GROUP BY county ORDER BY deaths DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county
# MAGIC  WHERE date >= '2020-04-01'

# COMMAND ----------

# DBTITLE 1,WA State COVID-19 Confirmed Cases per 100K
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases_per_100Kpop 
# MAGIC   FROM wa_state f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases_per_100Kpop) as cases_per_100Kpop FROM wa_state WHERE date = (SELECT max(date) FROM wa_state) GROUP BY county ORDER BY cases_per_100Kpop DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county
# MAGIC  WHERE date >= '2020-04-01'    

# COMMAND ----------

# DBTITLE 1,WA State COVID-19 Deaths per 100K
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.deaths_per_100Kpop 
# MAGIC   FROM wa_state f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(deaths_per_100Kpop) as deaths_per_100Kpop FROM wa_state WHERE date = (SELECT max(date) FROM wa_state) GROUP BY county ORDER BY deaths_per_100Kpop DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county
# MAGIC  WHERE date >= '2020-04-01'    

# COMMAND ----------

# MAGIC %md ## Visualize Cases by State Choropleth Maps
# MAGIC * Join the data with `map_fips_dedup` to obtain the county centroid lat, long_

# COMMAND ----------

max_date = sql("SELECT MAX(date) FROM wa_state").head()
wa_state_weekly = sql("SELECT * FROM wa_state WHERE MOD(DATEDIFF(date, '" + str(max_date[0]) + "'), 7) = 0")
wa_state_weekly.createOrReplaceTempView("wa_state_weekly")

# COMMAND ----------

# Extract Day Number and county centroid lat, long_
wa_daynum = spark.sql("""select f.fips, f.county, f.date, f.day_num, cases as confirmed, cast(f.cases_per_100Kpop as int) as confirmed_per100K, deaths, cast(f.deaths_per_100Kpop as int) as deaths_per100K, m.lat, m.long_ from wa_state_weekly f join map_fips_dedup m on m.fips = f.fips""")
wa_daynum.createOrReplaceTempView("wa_daynum")

# COMMAND ----------

# Obtain Topography
topo_usa = 'https://vega.github.io/vega-datasets/data/us-10m.json'
topo_wa = 'https://raw.githubusercontent.com/deldersveld/topojson/master/countries/us-states/WA-53-washington-counties.json'
us_counties = alt.topo_feature(topo_usa, 'counties')
wa_counties = alt.topo_feature(topo_wa, 'cb_2015_washington_county_20m')

# COMMAND ----------

# Review WA
confirmed_wa = wa_daynum.select("fips", "day_num", "date", "confirmed", "confirmed_per100K", "county").where("confirmed > 0").toPandas()
confirmed_wa['date'] = confirmed_wa['date'].astype(str)
deaths_wa = wa_daynum.select("lat", "long_", "day_num", "date", "deaths", "deaths_per100K", "county").where("deaths > 0").toPandas()
deaths_wa['date'] = deaths_wa['date'].astype(str)

# COMMAND ----------

# MAGIC %md ## COVID-19 Confirmed Cases and Deaths by WA and NY County Map and Graph

# COMMAND ----------

# map_state_graph
def map_state_graph(state_txt, state_counties, confirmed, confirmed_min, confirmed_max, deaths, deaths_min, deaths_max, state_fips):
  
  # pivot confirmed cases (by date)
  confirmed_pv2 = confirmed[['fips', 'date', 'confirmed']].copy()
  confirmed_pv2['fips'] = confirmed_pv2['fips'].astype(str)
  confirmed_pv2['date'] = confirmed_pv2['date'].astype(str)
  confirmed_pv2['confirmed'] = confirmed_pv2['confirmed'].astype('int64')
  confirmed_pv2 = confirmed_pv2.pivot_table(index='fips', columns='date', values='confirmed', fill_value=0).reset_index()

  # pivot deaths
  deaths_pv2 = deaths[['lat', 'long_', 'date', 'deaths']].copy()
  deaths_pv2['date'] = deaths_pv2['date'].astype(str)
  deaths_pv2['deaths'] = deaths_pv2['deaths'].astype('int64')
  deaths_pv2 = deaths_pv2.pivot_table(index=['lat', 'long_'], columns='date', values='deaths', fill_value=0).reset_index()

  # Extract column names for slider
  column_names2 = confirmed_pv2.columns.tolist()

  # Remove first element (`fips`)
  column_names2.pop(0)

  # date selection
  pts = alt.selection(type="single", encodings=['x'])

  # State
  base_state = alt.Chart(state_counties).mark_geoshape(
      fill='white',
      stroke='lightgray',
  ).properties(
      width=1000,
      height=700,
  ).project(
      type='mercator'
  )

  # State Counties
  base_state_counties = alt.Chart(us_counties).mark_geoshape(
    stroke='black',
    strokeWidth=0.05,
  ).transform_lookup(
    lookup='id',
   from_=alt.LookupData(confirmed_pv2, 'fips', column_names2)
   ).transform_fold(
     column_names2, as_=['date', 'confirmed']
  ).transform_calculate(
      state_id = "(datum.id / 1000)|0",
      date = 'datum.date',
      confirmed = 'isValid(datum.confirmed) ? datum.confirmed : -1'
  ).encode(
       color = alt.condition(
          'datum.confirmed > 0',      
          alt.Color('confirmed:Q', scale=alt.Scale(domain=(confirmed_min, confirmed_max), type='symlog')),
          alt.value('white')
        )  
  ).transform_filter(
    pts
  ).transform_filter(
      (alt.datum.state_id)==state_fips
  )

  # Bar Graph
  bar = alt.Chart(confirmed).mark_bar().encode(
      x='date:N',
      y='confirmed_per100K:Q',
      color=alt.condition(pts, alt.ColorValue("steelblue"), alt.ColorValue("grey"))
  ).properties(
      width=1000,
      height=300,
      title='Confirmed Cases per 100K'
  ).add_selection(pts)

  # Deaths
  points = alt.Chart(deaths).mark_point(opacity=0.75, filled=True).encode(
    longitude='long_:Q',
    latitude='lat:Q',
    size=alt.Size('sum(deaths):Q', scale=alt.Scale(domain=[deaths_min, deaths_max]), title='Deaths'),
    color=alt.value('#BD595D'),
    stroke=alt.value('brown'),
    tooltip=[
      alt.Tooltip('lat'),
      alt.Tooltip('long_'),
      alt.Tooltip('deaths'),
      alt.Tooltip('county:N'),      
      alt.Tooltip('date:N'),      
    ],
  ).properties(
    # update figure title
    title=f'COVID-19 Confirmed Cases and Deaths by County'
  ).transform_filter(
      pts
  )

  return (base_state + base_state_counties + points) & bar

# COMMAND ----------

map_state_graph('WA', wa_counties, confirmed_wa, 1, 10000, deaths_wa, 1, 700, 53)

# COMMAND ----------

