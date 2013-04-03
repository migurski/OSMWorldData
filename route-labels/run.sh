#!/bin/sh

cd `dirname $0`

find . -maxdepth 1 -name 'routes-*.json.bz2' -delete

DIR=`date +%Y-%m-%d`-routes
mkdir -p $DIR/routes-geojson
mkdir -p $DIR/routes-geojson-100th

python extract-routes.py -p gis gis

ln -f setup.sh $DIR/
ln -f process-routes.py $DIR/
ln -f routes-*01.json.bz2 $DIR/routes-geojson-100th/
mv routes-*.json.bz2 $DIR/routes-geojson/

s3put -b osm-streets-routes-data -g public-read -p `pwd` $DIR
