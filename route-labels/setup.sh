#!/bin/sh

# 
# Options:
# -D mapred.output.compress=true -D mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec
# -D mapred.compress.map.output=true -D mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec
# -D mapred.task.timeout=21600000
#

sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install -y bzip2 python-pip python-shapely python-numpy python-pyproj qhull-bin
sudo pip install networkx Skeletron StreetNames
