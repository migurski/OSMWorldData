#!/usr/bin/tcsh -ex
setenv BUCKET osm-streets-routes-data
cd `dirname $0`

pushd .

cd street-labels

setenv DIR `ls -td 20??-??-??-streets | head -n 1`
python launch-streets.py $BUCKET $DIR
python verify-streets.py $BUCKET/$DIR/streets-geojson/ $BUCKET/$DIR/output/
python download-streets.py -p gis $BUCKET/$DIR/output/ gis

popd

cd route-labels

setenv DIR `ls -td 20??-??-??-routes | head -n 1`
python launch-routes.py $BUCKET $DIR
python verify-routes.py $BUCKET/$DIR/routes-geojson/ $BUCKET/$DIR/output/
python download-routes.py -p gis $BUCKET/$DIR/output/ gis
