from json import dump
from time import time
from uuid import uuid1
from bz2 import BZ2File
from itertools import count, izip, groupby
from optparse import OptionParser
from multiprocessing import Pool

import logging

from shapely.wkb import loads
from shapely.geometry import MultiLineString
from StreetNames import short_street_name
from psycopg2 import connect, OperationalError

def build_temporary_tables(db, opts):
    '''
    '''
    # SF Bay Area
    geom = 'MakeBox2D(MakePoint(-123.117, 38.562), MakePoint(-120.981, 36.831))'
    
    # Medium areas: NYC, Uppsala, Manila, London, Shanghai
    geom = "GeomFromText('MULTIPOLYGON(((-80.043640 45.130711, -67.969666 45.130711, -67.969666 35.981340, -80.043640 35.981340, -80.043640 45.130711)), ((11.604309 62.755983, 23.678284 62.755983, 23.678284 56.687916, 11.604309 56.687916, 11.604309 62.755983)), ((-6.112518 55.113728, 5.961456 55.113728, 5.961456 47.595977, -6.112518 47.595977, -6.112518 55.113728)), ((114.955444 20.365228, 127.029419 20.365228, 127.029419 8.700499, 114.955444 8.700499, 114.955444 20.365228)), ((115.460815 36.232089, 127.534790 36.232089, 127.534790 25.914821, 115.460815 25.914821, 115.460815 36.232089)))')"
    
    # SF/Oakland
    geom = 'MakeBox2D(MakePoint(-122.5225, 37.8867), MakePoint(-122.1510, 37.6974))'
    
    db.execute('''
        CREATE TEMPORARY TABLE masks
            AS SELECT (Dump(Transform(SetSrid(%(geom)s, 4326), 900913))).geom AS way
        ''' % locals())

    logging.debug('Selecting street_ids...')
    
    start = time()
    
    # db.execute('''
    #     CREATE TEMPORARY TABLE street_ids
    #     AS SELECT streets.osm_id, streets.name, streets.kind
    #     FROM masks,
    #     (
    #         SELECT way, osm_id, name,
    # 
    #                (CASE WHEN highway IN ('motorway') THEN 'highway'
    #                      WHEN highway IN ('trunk', 'primary') THEN 'major_road'
    #                      ELSE 'minor_road' END) AS kind
    # 
    #         FROM %s
    #         WHERE name IS NOT NULL
    #           AND (highway IN ('trunk', 'primary') OR
    #                highway IN ('secondary'))
    #     ) AS streets
    #     
    #     WHERE streets.way && masks.way
    #     ''' % opts.table)
    
    db.execute('''
        CREATE TEMPORARY TABLE street_ids
        AS
        SELECT osm_id, name,
               (CASE WHEN highway IN ('motorway') THEN 'highway'
                     WHEN highway IN ('trunk', 'primary') THEN 'major_road'
                     ELSE 'minor_road' END) AS kind

        FROM %s
        WHERE name IS NOT NULL
        ''' % opts.table)
    
    logging.debug('Indexing street names...')
    
    db.execute('CREATE INDEX street_names ON street_ids(name)')
    
    # make it possible to rollback to this point in the event of an error
    db.execute('COMMIT')
    
    db.execute('SELECT COUNT(osm_id), COUNT(distinct name) FROM street_ids')
    streets_count, names_count = db.fetchone()
    
    logging.info('Found %d ways with %d unique names in %d seconds' % (streets_count, names_count, time() - start))
    
    return streets_count, names_count

def generate_bookends(db, opts):
    '''
    '''
    db.execute('SELECT COUNT(osm_id) FROM street_ids')
    (streets_count, ) = db.fetchone()

    for offset in range(0, streets_count, opts.count):
        
        db.execute('''SELECT name FROM street_ids
                      ORDER BY name LIMIT 1 OFFSET %d''' % offset)
        
        low_street = db.fetchone()[0]
        
        db.execute('''SELECT name FROM street_ids
                      ORDER BY name LIMIT 1 OFFSET %d''' % (offset + opts.count))
        
        if db.rowcount:
            high_street = db.fetchone()[0]
        
            db.execute('''SELECT COUNT(osm_id) FROM street_ids
                          WHERE name >= %s AND name < %s''',
                       (low_street, high_street))
        else:
            high_street = None
        
            db.execute('''SELECT COUNT(osm_id) FROM street_ids
                          WHERE name >= %s''',
                       (low_street, ))
        
        
        logging.debug('%d streets between %s and %s' % (db.fetchone()[0], low_street, high_street))
        
        yield (low_street, high_street)

def get_street_multilines(db, opts, low_street, high_street):
    '''
    '''
    if high_street is None:
        name_test = 'i.name >= %s'
        values = (low_street, )

    else:
        name_test = 'i.name >= %s AND i.name < %s'
        values = (low_street, high_street)

    table = opts.table
    
    try:
        #
        # Try to let Postgres do the grouping for us, it's faster.
        #
        db.execute('''
            SELECT s.name, i.kind, s.highway,
                   AsBinary(Transform(Collect(s.way), 4326)) AS way_wkb
            
            FROM %(table)s AS s, street_ids AS i
            
            WHERE s.osm_id = i.osm_id AND %(name_test)s
            GROUP BY s.name, i.kind, s.highway
            ORDER BY s.name''' % locals(), values)
    
        multilines = [(name, kind, highway, loads(bytes(way_wkb)))
                      for (name, kind, highway, way_wkb) in db.fetchall()]

    except OperationalError, err:
        #
        # Known to happen: "array size exceeds the maximum allowed (1073741823)"
        # Try again, but this time we'll need to do our own grouping.
        #
        logging.debug('Rolling back and doing our own grouping: %s' % err)
    
        db.execute('ROLLBACK')

        db.execute('''
            SELECT s.name, i.kind, s.highway,
                   AsBinary(Transform(s.way, 4326)) AS way_wkb
            
            FROM %(table)s AS s, street_ids AS i
            
            WHERE s.osm_id = i.osm_id AND %(name_test)s
            ORDER BY s.name, i.kind, s.highway''' % locals(), values)
        
        logging.debug('...executed...')
        
        groups = groupby(db.fetchall(), lambda (n, k, h, w): (n, k, h))
        multilines = []
        
        logging.debug('...fetched...')
        
        for ((name, kind, highway), group) in groups:
            lines = [loads(bytes(way_wkb)) for (n, k, h, way_wkb) in group]
            multilines.append((name, kind, highway, MultiLineString(lines)))
    
        logging.debug('...collected.')
        
    return multilines

def output_geojson_bzipped(index, streets):
    '''
    '''
    try:
        ids = [str(uuid1()) for (n, k, h, g) in streets]
        geometries = [geom.__geo_interface__ for (n, k, h, geom) in streets]

        properties = [dict(name=short_street_name(n), long_name=n, kind=k, highway=h)
                      for (n, k, h, g) in streets]
        
        features = [dict(type='Feature', id=id, properties=p, geometry=g)
                    for (id, p, g) in zip(ids, properties, geometries)]
        
        geojson = dict(type='FeatureCollection', features=features)
        output = BZ2File('streets-%06d.json.bz2' % index, 'w')
        dump(geojson, output)
        output.close()
    
    except Exception, e:
        return index, len(streets), e
    
    return index, len(streets), True

optparser = OptionParser(usage="""%prog [options] <database>""")

defaults = dict(host='localhost', user='gis', passwd=None, table='planet_osm_line', count=5000)

optparser.set_defaults(**defaults)

optparser.add_option('--host', dest='host',
                     help='Postgres hostname, default %(host)s.' % defaults)

optparser.add_option('-u', '--user', dest='user',
                     help='Postgres username, default "%(user)s".' % defaults)

optparser.add_option('-p', '--passwd', dest='passwd',
                     help='Postgres password, default "%(passwd)s".' % defaults)

optparser.add_option('-t', '--table', dest='table',
                     help='Osm2psql lines table name, default "%(table)s".' % defaults)

if __name__ == '__main__':

    opts, (dbname, ) = optparser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)08s - %(message)s')
    
    db = connect(host=opts.host, database=dbname, user=opts.user, password=opts.passwd)
    db = db.cursor()
    
    #
    # Build temporary table with street IDs
    #
    build_temporary_tables(db, opts)
    
    #
    # Ship everything off to be bzipped
    #
    bookends = generate_bookends(db, opts)
    pool = Pool(6)
    
    for ((low_street, high_street), index) in izip(bookends, count(1)):
        streets = get_street_multilines(db, opts, low_street, high_street)
        
        def callback((index, count, status)):
            if status is True:
                logging.info('%(index)d. Wrote %(count)d streets' % locals())
            else:
                logging.info('%(index)d. Failed: %(status)s' % locals())
        
        pool.apply_async(output_geojson_bzipped, (index, streets), callback=callback)
        
    db.close()
    pool.close()
    pool.join()
