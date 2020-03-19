# Databricks notebook source
# MAGIC %md 
# MAGIC ## 2. Analyze CORD-19 Datasets
# MAGIC ### COVID-19 Open Research Dataset Challenge (CORD-19) Working Notebooks
# MAGIC 
# MAGIC This is a working notebook for the [COVID-19 Open Research Dataset Challenge (CORD-19)](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge) to help you jump start your analysis of the CORD-19 dataset.  
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/3648/1*596Ur1UdO-fzQsaiGPrNQg.png" width="700"/>
# MAGIC 
# MAGIC Attributions:
# MAGIC * The licenses for each dataset used for this workbook can be found in the *all _ sources _ metadata csv file* which is included in the [downloaded dataset](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge/download).  
# MAGIC * For the 2020-03-03 dataset: 
# MAGIC   * `comm_use_subset`: Commercial use subset (includes PMC content) -- 9000 papers, 186Mb
# MAGIC   * `noncomm_use_subset`: Non-commercial use subset (includes PMC content) -- 1973 papers, 36Mb
# MAGIC   * `biorxiov_medrxiv`: bioRxiv/medRxiv subset (pre-prints that are not peer reviewed) -- 803 papers, 13Mb
# MAGIC * When using Databricks or Databricks Community Edition, a copy of this dataset has been made available at `/databricks-datasets/COVID/CORD-19`
# MAGIC * This notebook is freely available to share, licensed under [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/us/)

# COMMAND ----------

# MAGIC %md #### Configure Parquet Path Variables
# MAGIC Save the data in Parquet format at: `/tmp/dennylee/COVID/CORD-19/2020-03-13/`

# COMMAND ----------

# Configure Parquet Paths in Python
comm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/comm_use_subset.parquet"
noncomm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/noncomm_use_subset.parquet"
biorxiv_medrxiv_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/biorxiv_medrxiv/biorxiv_medrxiv.parquet"
json_schema_path = "/databricks-datasets/COVID/CORD-19/2020-03-13/json_schema.txt"

# Configure Path as Shell Enviroment Variables
import os
os.environ['comm_use_subset_pq_path']=''.join(comm_use_subset_pq_path)
os.environ['noncomm_use_subset_pq_path']=''.join(noncomm_use_subset_pq_path)
os.environ['biorxiv_medrxiv_pq_path']=''.join(biorxiv_medrxiv_pq_path)
os.environ['json_schema_path']=''.join(json_schema_path)

# COMMAND ----------

# MAGIC %md #### Read Parquet Files
# MAGIC As these are correctly formed JSON files, you can use `spark.read.json` to read these files.  Note, you will need to specify the *multiline* option.

# COMMAND ----------

# Reread files
comm_use_subset = spark.read.format("parquet").load(comm_use_subset_pq_path)
noncomm_use_subset = spark.read.format("parquet").load(noncomm_use_subset_pq_path)
biorxiv_medrxiv = spark.read.format("parquet").load(biorxiv_medrxiv_pq_path)

# COMMAND ----------

# Count number of records
comm_use_subset_cnt = comm_use_subset.count()
noncomm_use_subset_cnt = noncomm_use_subset.count()
biorxiv_medrxiv_cnt = biorxiv_medrxiv.count()

# Print out
print ("comm_use_subset: %s, noncomm_use_subset: %s, biorxiv_medrxiv: %s" % (comm_use_subset_cnt, noncomm_use_subset_cnt, biorxiv_medrxiv_cnt))

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat /dbfs$json_schema_path

# COMMAND ----------

comm_use_subset.createOrReplaceTempView("comm_use_subset")
comm_use_subset.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select paper_id, metadata.title, metadata.authors, metadata from comm_use_subset limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select paper_id, metadata.title, explode(metadata.authors) from comm_use_subset limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select paper_id, min(country) as AuthorCountry
# MAGIC   from (
# MAGIC select paper_id, authors.affiliation.location.country as country
# MAGIC   from (
# MAGIC     select paper_id, metadata.title as title, explode(metadata.authors) as authors from comm_use_subset 
# MAGIC   ) a
# MAGIC  where authors.affiliation.location.country is not null  
# MAGIC ) x
# MAGIC group by paper_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select authors.affiliation.location.country as country, count(distinct paper_id) as papers 
# MAGIC   from (
# MAGIC     select paper_id, metadata.title as title, explode(metadata.authors) as authors from comm_use_subset 
# MAGIC   ) a
# MAGIC group by country

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC   from (
# MAGIC     select paper_id, metadata.title as title, explode(metadata.authors) as authors from comm_use_subset 
# MAGIC   ) a
# MAGIC where authors.affiliation.location.country like '%USA, USA, USA, USA%'

# COMMAND ----------

# papers by Author Country
papersByCountry = spark.sql("""
select paper_id, min(country) as AuthorCountry
  from (
select paper_id, authors.affiliation.location.country as country
  from (
    select paper_id, metadata.title as title, explode(metadata.authors) as authors from comm_use_subset 
  ) a
 where authors.affiliation.location.country is not null  
) x
group by paper_id
""")

# Create temp view
papersByCountry.createOrReplaceTempView("papersByCountry")

# COMMAND ----------

mapCountryCleansed = spark.read.options(header='true', inferSchema='true', sep='|').csv("tmp/dennylee/mappings/mapCountryCleansed")
mapCountryCleansed.createOrReplaceTempView("mapCountryCleansed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select m.Alpha3, count(distinct p.paper_id) as papers
# MAGIC   from papersByCountry p
# MAGIC     left join mapCountryCleansed m
# MAGIC       on m.AuthorCountry = p.AuthorCountry
# MAGIC  group by m.Alpha3

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### Reference
# MAGIC Refrence code for mapping "country" values 

# COMMAND ----------

mapCountry = spark.sql("""select distinct AuthorCountry from papersByCountry""")
mapCountry.createOrReplaceTempView("mapCountry")
mapCountry.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mapCountry order by AuthorCountry

# COMMAND ----------

dbutils.fs.put("/tmp/dennylee/mappings/mapCountryCleansed", """
AuthorCountry|Alpha2|Alpha3
12 Korea|KR|KOR
2 Republic of Korea|KR|KOR
Algeria|DZ|DZA
Argentina|AR|ARG
Argentina, China|AR|ARG
Australia|AU|AUS
Australia, Australia|AU|AUS
Australia, Canada|AU|AUS
Austria|AT|AUT
Bahrain|BH|BHR
Bangladesh|BD|BGD
Belgium|BE|BEL
Belgium;, France|BE|BEL
Benin|BJ|BEN
Botswana|BW|BWA
Brasil|BR|BRA
Brazil|BR|BRA
Brazil (, Brazil|BR|BRA
Brazil Correspondence|BR|BRA
Brazil., Brazil|BR|BRA
Bulgaria|BGR
CYPRUS|CY|CYP
California|US|USA
Cambodia|KH|KHM
Cambodia, France|KH|KHM
Cameroon|CM|CMR
Cameroun|CM|CMR
Canada|CA|CAN
Canada, France|CA|CAN
Canada, United States of America|CA|CAN
Canada;|CA|CAN
Centre, France|FR|FRA
Chile|CL|CHL
China|CN|CHN
China SAR|HK|HKG
China, 6 Ministry|CN|CHN
China, China|CN|CHN
China, People's Republic of China|CN|CHN
China-Japan|CN|CHN
China-Japan, China|CN|CHN
China., China|CN|CHN
Colombia|CO|COL
Croatia|HR|HRV
Croatia, Croatia|HR|HRV
Cuba|CU|CUB
Cyprus|CY|CYP
Czech Republic|CZ|CZE
Democratic Republic of Congo|CD|COD
Democratic Republic of the Congo|CD|COD
Denmark|DK|DNK
Denmark, Sweden|DK|DNK
Denmark;, The Netherlands|DK|DNK
Ecuador|EC|ECU
Egypt|EG|EGY
Egypt, Republic of Congo|EG|EGY
England, UK|GB|GBR
England, United Kingdom|GB|GBR
España. Correspondence|ES|ESP
Estonia|EE|EST
Ethiopia|ET|ETH
FRANCE|FR|FRA
Finland|FI|FIN
France|FR|FRA
France Correspondence|FR|FRA
France;, France|FR|FRA
Gabon|GA|GAB
Gdansk Poland|PL|POL
Georgia|GE|GEO
Georgia, USA|US|USA
Germany|DE|DEU
Germany, Canada|DE|DEU
Germany, Germany|DE|DEU
Germany, Germany, Germany|DE|DEU
Germany, SPAIN|DE|DEU
Germany, Sweden|DE|DEU
Germany;, USA., USA|DE|DEU
Ghana|GH|GHA
Greece|GR|GRC
Grenada|GD|GRD
Guatemala|GT|GTM
Guinea|GN|GIN
Haiti|HT|HTI
Hungary|HU|HUN
India|IN|IND
India, Pakistan;, Pakistan|IN|IND
India. *Correspondence|IN|IND
India;, Norway|IN|IND
Indonesia|ID|IDN
Iran|IR|IRN
Iran, Malaysia|IR|IRN
Iraq|IQ|IRQ
Ireland|IE|IRL
Israel|IL|ISR
Israel, USA|IL|ISR
Italy|IT|ITA
Italy, United States, Germany, United States|IT|ITA
Jamaica|JM|JAM
Jamaica (|JM|JAM
Japan|JP|JPN
Japan Racing Association, Japan|JP|JPN
Japan, Japan|JP|JPN
Jordan|JO|JOR
Jordan, Jordan|JO|JOR
Kazakhstan|KZ|KAZ
Kelantan|MY|MYS
Kenya|KE|KEN
Kenya, Kenya|KE|KEN
Kingdom of Bahrain|BH|BHR
Korea|KR|KOR
Korea Correspondence, UK|KR|KOR
Korea, Korea, South Korea|KR|KOR
Kuwait|KW|KWT
Kyrgyzstan|KG|KGZ
Lebanon|LB|LBN
Liberia|LR|LBR
Lin-, Taiwan|TW|TWN
Lithuania|LT|LTU
Luxembourg|LU|LUX
Madagascar|MG|MDG
Malawi|MW|MWI
Malaysia|MY|MYS
Mali|ML|MLI
Mexico|MX|MEX
Mongolia|MN|MNG
Morocco|MA|MAR
México|MX|MEX
Nepal|NP|NPL
Nepal;|NP|NPL
Netherlands|NL|NLD
New Jersey|US|USA
New Zealand|NZ|NZL
Nicaragua|NI|NIC
Niger|NE|NER
Nigeria|NG|NGA
Norway|NO|NOR
Oman|OM|OMN
P. R. China|CN|CHN
P. R. China, P. R. China|CN|CHN
P.R China|CN|CHN
P.R. China|CN|CHN
P.R. China, P.R. China|CN|CHN
P.R. China., P.R. China|CN|CHN
P.R. of China|CN|CHN
P.R.China|CN|CHN
PR China|CN|CHN
PRC|CN|CHN
Pakistan|PK|PAK
Palestine|PS|PSE
Pennsylvania|US|USA
Pennsylvania;|US|USA
People's Republic of China|CN|CHN
People9s Republic of China|CN|CHN
Peru|PE|PER
Philippines|PH|PHL
Poland|PL|POL
Portugal|PT|PRT
Qatar|QA|QAT
ROC|CN|CHN
Republic of Ireland|IE|GBR
Republic of Kazakhstan|KZ|KAZ
Republic of Korea|KR|KOR
Republic of Panama|PA|PAN
Republic of Singapore|SG|SGP
Republic of The Gambia|GM|GMB
Reunion Island|FR|FRA
Reunion Island, France|FR|FRA
Romania|RO|ROU
Russia|RU|RUS
Saudi Arabia|SA|SAU
Saudi Arabia, Saudi Arabia|SA|SAU
Scotland, UK|GB|GBR
Scotland, United Kingdom|GB|GBR
Sellman BR||
Senegal|SN|SEN
Serbia|RS|SRB
Singapore|SG|SGP
Singapore ¤|SG|SGP
Singapore, Singapore|SG|SGP
Singapore, Singapore, Singapore|SG|SGP
Singapore. Correspondence|SG|SGP
Slovak Republic|SK|SVK
Slovakia|SK|SVK
Slovenia|SI|SVN
South||
South Africa|ZA|ZAF
South China, China|CN|CHN
South Korea|KR|KOR
South Korea. Correspondence|KR|KOR
Spain|ES|ESP
Spain, France|ES|ESP
Spain, UNITED STATES|ES|ESP
Spain, United States of America|ES|ESP
Sri Lanka|LK|LKA
Stratoxon LLC USA, USA, USA|US|USA
Sudan|SD|SDN
Sweden|SE|SWE
Sweden, Germany|SE|SWE
Sweden, Netherlands|SE|SWE
Sweden, Norway|SE|SWE
Switzerland|CH|CHE
Switzerland, Cameroon|CH|CHE
Switzerland., UK|CH|CHE
Taiwan|TW|TWN
Taiwan (R.O.C.|TW|TWN
Taiwan (ROC|TW|TWN
Taiwan R.O.C|TW|TWN
Taiwan ROC|TW|TWN
Taiwan ROC Republic of China|TW|TWN
Taiwan(|TW|TWN
Taiwan, ROC|TW|TWN
Taiwan, Republic of China|TW|TWN
Tanzania|TZ|TZA
Thailand|TH|THA
Thailand (DL|TH|THA
The Gambia|GM|GMB
The Netherlands|NL|NLD
The Netherlands ARTICLE HISTORY|NL|NLD
The Netherlands., The Netherlands|NL|NLD
The P.R. China|CN|CHN
Tunisia|TN|TUN
Turkey|TR|TUR
U.S.A|US|USA
UAE|AE|ARE
UK|GB|GBR
UK ARTICLE HISTORY|GB|GBR
UK ARTICLE HISTORY, UK|GB|GBR
UK, UK|GB|GBR
UK., Germany|GB|GBR
UK;, Germany|GB|GBR
UNITED STATES|US|USA
US, USA|US|USA
USA|US|USA
USA, China|US|USA
USA, USA|US|USA
USA, USA, USA, USA|US|USA
USA.|US|USA
USA., Vietnam|US|USA
USA;, Germany|US|USA
Uganda|UG|UGA
Ukraine|UA|UKR
United Arab Emirates|AE|ARE
United Arab, United Arab Emirates|AE|ARE
United Kingdom|GB|GBR
United Kingdom, United Kingdom|GB|GBR
United Kingdom, United States of America|GB|GBR
United Stated of America}|US|USA
United States|US|USA
United States of America|US|USA
United States of America, Germany|US|USA
United States of America, United States|US|USA
United States of America, United States of America|US|USA
United States, Germany|US|USA
United States, USA|US|USA
United States, United States|US|USA
United States, United States of America|US|USA
United States, United States, Italy, Greece|US|USA
United-Kingdom|GB|GBR
Uruguay|UY|URY
UsA|US|USA
Vietnam|VN|VNM
Virginia, USA|US|USA
australia, australia|AU|AUS
israel|IL|ISR
italy|IT|ITA
the Netherlands|NL|NLD
""", True)

# COMMAND ----------

