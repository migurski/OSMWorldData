from json import dump
from time import time
from uuid import uuid1
from bz2 import BZ2File
from itertools import count, izip
from optparse import OptionParser
from multiprocessing import Pool

import logging

from psycopg2 import connect
from shapely.wkb import loads

def get_relations_list(db, opts):
    '''
    '''
    db.execute('''SELECT id, tags
                  FROM %s
                  WHERE 'network' = ANY(tags)
                    AND 'ref' = ANY(tags)
                  ''' % opts.table)
    
    relations = []
    
    for (id, tags) in db.fetchall():
        tags = dict([keyval for keyval in zip(tags[0::2], tags[1::2])])
        
        if 'network' not in tags or 'ref' not in tags:
            continue
        
        network = tags.get('network', '')
        route = tags.get('route', tags.get('type', ''))
        
        if route == 'route_master' and 'route_master' in tags:
            route = tags.get('route_master', '')

        # Skip bike
        if network in ('lcn', 'rcn', 'ncn', 'icn', 'mtb'):
            continue
        
        # Skip walking
        if network in ('lwn', 'rwn', 'nwn', 'iwn'):
            continue

        # Skip buses, trains
        if route in ('bus', 'bicycle', 'tram', 'train', 'subway', 'light_rail', 'trolleybus'):
            continue
        
        elif tags.get('line', '') in ('bus', ):
            continue
        
        # if tags.get('network', '') not in ('US:I', ): continue
        
        relations.append((id, tags))
    
    return relations

def cascaded_union(shapes):
    '''
    '''
    if len(shapes) == 0:
        return None
    
    if len(shapes) == 1:
        return shapes[0]
    
    if len(shapes) == 2:
        if shapes[0] and shapes[1]:
            return shapes[0].union(shapes[1])
        
        if shapes[0] is None:
            return shapes[1]
        
        if shapes[1] is None:
            return shapes[0]
        
        return None
    
    cut = len(shapes) / 2
    
    shapes1 = cascaded_union(shapes[:cut])
    shapes2 = cascaded_union(shapes[cut:])
    
    return cascaded_union([shapes1, shapes2])

def relation_key(tags):
    '''
    '''
    return (tags.get('network', ''), tags.get('ref', ''), tags.get('modifier', ''))

def get_relation_ways(db, rel_id):
    '''
    '''
    rel_ids = [rel_id]
    rels_seen = set()
    way_ids = set()
    
    while rel_ids:
        rel_id = rel_ids.pop(0)
        
        if rel_id in rels_seen:
            break
        
        rels_seen.add(rel_id)
        
        db.execute('''SELECT members
                      FROM planet_osm_rels
                      WHERE id = %d''' \
                    % rel_id)
        
        try:
            (members, ) = db.fetchone()

        except TypeError:
            # missing relation
            continue
        
        if not members:
            continue
        
        for member in members[0::2]:
            if member.startswith('r'):
                rel_ids.append(int(member[1:]))
            
            elif member.startswith('w'):
                way_ids.add(int(member[1:]))
    
    return way_ids

def get_way_linestring(db, way_id):
    '''
    '''
    db.execute('''SELECT AsBinary(Transform(way, 4326))
                  FROM planet_osm_line
                  WHERE osm_id = %s''', (way_id, ))
    
    if db.rowcount:
        return loads(bytes(db.fetchone()[0]))

    else:
        return None
    
    db.execute('SELECT SRID(way) FROM planet_osm_point LIMIT 1')
    
    (srid, ) = db.fetchone()
    
    if srid not in (4326, 900913):
        raise Exception('Unknown SRID %d' % srid)
    
    db.execute('''SELECT X(location) AS lon, Y(location) AS lat
                  FROM (
                    SELECT
                      CASE
                      WHEN %s = 900913
                      THEN Transform(SetSRID(MakePoint(n.lon * 0.01, n.lat * 0.01), 900913), 4326)
                      WHEN %s = 4326
                      THEN MakePoint(n.lon * 0.0000001, n.lat * 0.0000001)
                      END AS location
                    FROM (
                      SELECT unnest(nodes)::int AS id
                      FROM planet_osm_ways
                      WHERE id = %d
                    ) AS w,
                    planet_osm_nodes AS n
                    WHERE n.id = w.id
                  ) AS points''' \
                % (srid, srid, way_id))
    
    coords = db.fetchall()
    
    if len(coords) < 2:
        return None
    
    return LineString(coords)

def gen_relation_groups(db, relations):
    '''
    '''
    relation_keys = [relation_key(tags) for (id, tags) in relations]
    
    group, coords, last_key = [], 0, None
    
    for (key, (id, tags)) in sorted(zip(relation_keys, relations)):

        if coords > 100000 and key != last_key:
            yield group
            group, coords = [], 0
        
        way_ids = get_relation_ways(db, id)
        way_lines = [get_way_linestring(db, way_id) for way_id in way_ids]
        rel_coords = sum([len(line.coords) for line in way_lines if line])
        multiline = cascaded_union(way_lines)
        
        logging.debug('%s -- %d nodes' % (', '.join(key), rel_coords))
        
        if multiline:
            group.append((id, tags, multiline))
            coords += rel_coords
            last_key = key

    yield group

def output_geojson_bzipped(index, routes):
    '''
    '''
    try:
        ids = [id for (id, t, g) in routes]
        geometries = [geom.__geo_interface__ for (i, t, geom) in routes]
        properties = [tags for (i, tags, g) in routes]
        
        features = [dict(type='Feature', id=id, properties=p, geometry=g)
                    for (id, p, g) in zip(ids, properties, geometries)]
        
        geojson = dict(type='FeatureCollection', features=features)
        output = BZ2File('routes-%06d.json.bz2' % index, 'w')
        dump(geojson, output)
        output.close()
    
    except Exception, e:
        return index, len(routes), e
    
    return index, len(routes), True

optparser = OptionParser(usage="""%prog [options] <database>""")

defaults = dict(host='localhost', user='gis', passwd=None, table='planet_osm_rels', count=5000)

optparser.set_defaults(**defaults)

optparser.add_option('--host', dest='host',
                     help='Postgres hostname, default %(host)s.' % defaults)

optparser.add_option('-u', '--user', dest='user',
                     help='Postgres username, default "%(user)s".' % defaults)

optparser.add_option('-p', '--passwd', dest='passwd',
                     help='Postgres password, default "%(passwd)s".' % defaults)

optparser.add_option('-t', '--table', dest='table',
                     help='Osm2psql relations table name, default "%(table)s".' % defaults)

if __name__ == '__main__':

    opts, (dbname, ) = optparser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)08s - %(message)s')
    
    db = connect(host=opts.host, database=dbname, user=opts.user, password=opts.passwd)
    db = db.cursor()
    
    #
    # Build temporary table with relation IDs
    #
    relations = get_relations_list(db, opts)
    route_groups = gen_relation_groups(db, relations)
    pool = Pool(6)
    
    for (routes, index) in izip(route_groups, count(1)):
        
        def callback((index, count, status)):
            if status is True:
                logging.info('%(index)d. Wrote %(count)d routes' % locals())
            else:
                logging.info('%(index)d. Failed: %(status)s' % locals())
        
        pool.apply_async(output_geojson_bzipped, (index, routes), callback=callback)
        
    db.close()
    pool.close()
    pool.join()