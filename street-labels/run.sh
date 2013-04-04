#!/bin/sh

cd `dirname $0`

find . -maxdepth 1 -name 'streets-*.json.bz2' -delete

DIR=`date +%Y-%m-%d`-streets
mkdir -p $DIR/streets-geojson
mkdir -p $DIR/streets-geojson-100th

python extract-streets.py -p gis gis

ln -f setup.sh $DIR/
ln -f process-streets.py $DIR/
ln -f streets-*01.json.bz2 $DIR/streets-geojson-100th/
mv streets-*.json.bz2 $DIR/streets-geojson/

s3put -b osm-streets-routes-data -g public-read -p `pwd` $DIR
