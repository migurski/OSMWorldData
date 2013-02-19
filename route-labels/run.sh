#!/bin/sh

cd `dirname $0`
PATH="$PATH:/usr/local/bin"

find . -maxdepth 1 -name 'routes-*.json.bz2' -delete

DIR=`date +%Y-%m-%d`-routes
mkdir -p $DIR/routes-geojson
mkdir -p $DIR/routes-geojson-100th

python extract-routes.py -p gis gis

ln -f setup.sh $DIR/
ln -f routes-*01.json.bz2 $DIR/routes-geojson-100th/
mv routes-*.json.bz2 $DIR/routes-geojson/

curl -sL https://raw.github.com/migurski/Skeletron/master/skeletron-hadoop-mapper.py -o $DIR/skeletron-hadoop-mapper.py
curl -sL https://raw.github.com/migurski/Skeletron/master/skeletron-hadoop-reducer.py -o $DIR/skeletron-hadoop-reducer.py

s3put -b osm-hadoop-data -g public-read -p `pwd` $DIR
