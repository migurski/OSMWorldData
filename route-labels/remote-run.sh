#!/bin/sh -e

if [ $# -ne 1 ]; then
	echo "Usage: $0 <directory>"
	echo "e.g. $0 2013-01-01-routes"
	exit 1
fi

DIR=${1%/}

if [ ! -d $DIR ]; then
	echo "Directory '$DIR' not found."
	echo ""
	echo "Usage: $0 <directory>"
	echo "e.g. $0 2013-01-01-routes"
	exit 1
fi

echo "Launching EC2 machines (launch-routes)..."
python launch-routes.py osm-streets-routes-data $DIR

echo "Verifying output (verify-routes)..."
python verify-routes.py osm-streets-routes-data/$DIR/routes-geojson/ osm-streets-routes-data/$DIR/output/

echo "Downloading route data (download-routes)..."
python download-routes.py -u gis -p gis osm-streets-routes-data/$DIR/output/ gis
