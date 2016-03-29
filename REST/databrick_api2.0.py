import requests
import json
import sys
from collections import namedtuple

#
# databricks_api.py
# 
# This is a sample Python script for making Databricks REST API 2.0 requests
# Valid request include: 
#	list: get the list of clusters
#	version: get the available Spark version
#
# How to run:
#	databricks_api.py [clustername] [list]
#	databricks_api.py [clustername] [version]
#


# ---- Place in username and password START ----------
#clustername = 'https://demo.cloud.databricks.com/'
user = '#fill-in-here#'
pwd = '#fill-in-here#'
# ---- Place in username and password END ------------


# Obtain argument list
if len(sys.argv) <= 1:
	print " "
	print "--- databricks_api.py --- "
	print "This is a sample Python script for Databricks REST API 2.0 requests"
	print " "
	print "ERROR: Please specify your Databricks cluster name"
	print " "
	print "Examples:"
	print "  databricks_api.py [clustername] [list] "	
	print "  databricks_api.py [clustername] [version] "	
	print " "
	exit()


clustername = sys.argv[1]
task = sys.argv[2]
# add else statement to validate other valid statements


print " "
print "--- databricks_api.py --- "
print "This is a sample Python script for Databricks REST API 2.0 requests"
print "Task: %s \n" % task

if task == "list":
	#
	# Extract out the clusters list
	#
	request_string = clustername + 'api/2.0/clusters/list'
	r = requests.get(request_string, auth=(user, pwd))

	# Extract out the cluster_id, cluster_name
	Cluster = namedtuple('Cluster', 'cluster_id driver executors jdbc_port cluster_name spark_version aws_attributes node_type_id state state_message start_time terminated_time last_state_loss_time num_workers cluster_memory_mb cluster_cores')
	data = json.loads(r.text)

	clusters = [Cluster(**k) for k in data["clusters"]]
	print "cluster_id, cluster_name, state, spark_version"
	for c in clusters:
		print "%s, %s, %s, %s " % (c.cluster_id, c.cluster_name, c.state, c.spark_version)

elif task == "version":
	#
	# Print out available spark versions
	#
	request_string = clustername + 'api/2.0/clusters/spark-versions'
	r = requests.get(request_string, auth=(user, pwd))

	# Extract out the version information
	Versions = namedtuple('Version', 'key name')
	data = json.loads(r.text)

	print "key, name"
	versions = [Versions(**v) for v in data["versions"]]
	for v in versions:
		print "%s, %s " % (v.key, v.name)



