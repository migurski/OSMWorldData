from time import time
from uuid import uuid1
from bz2 import BZ2File
from itertools import count, izip, groupby
from optparse import OptionParser
from multiprocessing import Pool
from json import JSONEncoder
from re import compile

import logging

from psycopg2 import connect
from shapely.wkb import loads

float_pat = compile(r'^-?\d+\.\d+(e-?\d+)?$')
charfloat_pat = compile(r'^[\[,\,]-?\d+\.\d+(e-?\d+)?$')

def get_relations_list(db, opts):
    '''
    '''
    db.execute('''SELECT id, tags
                  FROM %s_rels
                  WHERE 'network' = ANY(tags)
                    AND 'ref' = ANY(tags)
                  ''' % opts.table_prefix)
    
    relations = []
    
    for (id, tags) in db.fetchall():
        tags = dict([keyval for keyval in zip(tags[0::2], tags[1::2])])
        
        if 'network' not in tags or 'ref' not in tags:
            continue
        
        if 'modifier' not in tags:
            tags['modifier'] = ''
        
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

def relation_key((id, tags)):
    '''
    '''
    return (tags.get('network', ''), tags.get('ref', ''), tags.get('modifier', ''))

def get_relation_ways(db, opts, rel_id):
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
                      FROM %s_rels
                      WHERE id = %d''' \
                    % (opts.table_prefix, rel_id))
        
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

def get_way_linestring(db, opts, way_id):
    '''
    '''
    db.execute('''SELECT AsBinary(Transform(way, 4326))
                  FROM %s_line
                  WHERE osm_id = %%s''' % opts.table_prefix, (way_id, ))
    
    if db.rowcount == 0:
        return None

    return loads(bytes(db.fetchone()[0]))

def gen_relation_groups(db, opts, relations):
    '''
    '''
    relations.sort(key=relation_key)
    
    group_list, group_coords = [], 0
    
    for (key, _relations) in groupby(relations, relation_key):
    
        rel_coords, way_lines = 0, []
        
        for (id, tags) in _relations:
            way_ids = get_relation_ways(db, opts, id)
            way_lines += [get_way_linestring(db, opts, way_id) for way_id in way_ids]
            rel_coords += sum([len(line.coords) for line in way_lines if line])

        logging.debug('%s -- %d nodes' % (', '.join(key), rel_coords))
        
        group_list.append((id, tags, way_lines))
        group_coords += rel_coords
    
        if group_coords > 100000:
            yield group_list
            group_list, group_coords = [], 0
    
    yield group_list

def output_geojson_bzipped(index, routes):
    '''
    '''
    try:
        ids = [id for (id, t, g) in routes]
        geometries = [cascaded_union(geoms) for (i, t, geoms) in routes]
        geometries = [geom.__geo_interface__ for geom in geometries if geom]
        properties = [tags for (i, tags, g) in routes]
        
        features = [dict(type='Feature', id=id, properties=p, geometry=g)
                    for (id, p, g) in zip(ids, properties, geometries)]
        
        geojson = dict(type='FeatureCollection', features=features)
        encoder = JSONEncoder(separators=(',', ':'))
        encoded = encoder.iterencode(geojson)

        output = BZ2File('routes-%06d.json.bz2' % index, 'w')
        
        for token in encoded:
            if charfloat_pat.match(token):
                # in python 2.7, we see a character followed by a float literal
                output.write(token[0] + '%.6f' % float(token[1:]))
            
            elif float_pat.match(token):
                # in python 2.6, we see a simple float literal
                output.write('%.6f' % float(token))
            
            else:
                output.write(token)        

        output.close()
    
    except Exception, e:
        return index, len(routes), e
    
    return index, len(routes), True

optparser = OptionParser(usage="""%prog [options] <database>""")

defaults = dict(host='localhost', user='gis', passwd=None, table_prefix='planet_osm', count=5000, loglevel=logging.INFO)

optparser.set_defaults(**defaults)

optparser.add_option('--host', dest='host',
                     help='Postgres hostname, default %(host)s.' % defaults)

optparser.add_option('-u', '--user', dest='user',
                     help='Postgres username, default "%(user)s".' % defaults)

optparser.add_option('-p', '--passwd', dest='passwd',
                     help='Postgres password, default "%(passwd)s".' % defaults)

optparser.add_option('-t', '--table-prefix', dest='table_prefix',
                     help='Osm2psql table name prefix, default "%(table_prefix)s".' % defaults)

optparser.add_option('-v', '--verbose', dest='loglevel',
                     action='store_const', const=logging.DEBUG,
                     help='Output extra progress information.')

optparser.add_option('-q', '--quiet', dest='loglevel',
                     action='store_const', const=logging.WARNING,
                     help='Output no progress information.')

if __name__ == '__main__':

    opts, (dbname, ) = optparser.parse_args()
    
    logging.basicConfig(level=opts.loglevel, format='%(levelname)08s - %(message)s')
    
    db = connect(host=opts.host, database=dbname, user=opts.user, password=opts.passwd)
    db = db.cursor()
    
    #
    # Build temporary table with relation IDs
    #
    relations = get_relations_list(db, opts)
    route_groups = gen_relation_groups(db, opts, relations)
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
