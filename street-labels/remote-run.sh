#!/bin/sh -e

if [ $# -ne 1 ]; then
	echo "Usage: $0 <directory>"
	echo "e.g. $0 2013-01-01-streets"
	exit 1
fi

DIR=${1%/}

if [ ! -d $DIR ]; then
	echo "Directory '$DIR' not found."
	echo ""
	echo "Usage: $0 <directory>"
	echo "e.g. $0 2013-01-01-streets"
	exit 1
fi

echo "Launching EC2 machines (launch-streets)..."
python launch-streets.py osm-streets-routes-data $DIR

echo "Verifying output (verify-streets)..."
python verify-streets.py osm-streets-routes-data/$DIR/streets-geojson/ osm-streets-routes-data/$DIR/output/

echo "Downloading street data (download-streets)..."
python download-streets.py -u gis -p gis osm-streets-routes-data/$DIR/output/ gis
