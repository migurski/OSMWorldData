#!/bin/sh

cd `dirname $0`

find . -maxdepth 1 -name 'streets-*.json.bz2' -delete

DIR=`date +%Y-%m-%d`-streets
mkdir -p $DIR/streets-geojson
mkdir -p $DIR/streets-geojson-100th

python extract-streets-z12.py -p gis gis

ln -f setup.sh $DIR/
ln -f streets-*01.json.bz2 $DIR/streets-geojson-100th/
mv streets-*.json.bz2 $DIR/streets-geojson/

curl -sL https://raw.github.com/migurski/Skeletron/master/skeletron-hadoop-mapper.py -o $DIR/skeletron-hadoop-mapper.py
curl -sL https://raw.github.com/migurski/Skeletron/master/skeletron-hadoop-reducer.py -o $DIR/skeletron-hadoop-reducer.py

s3put -b osm-hadoop-data -g public-read -p `pwd` $DIR
