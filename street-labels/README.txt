ogr2ogr -t_srs EPSG:4326 \
        -lco ENCODING=UTF-8 -lco COORDINATE_PRECISION=6 \
        -f GeoJSON /vsistdout \
        PG:"dbname='*' host='*' user='*' password='*'" <table name> \
      | pv \
      | bzip2 -v \
      > streets.json.bz2
