from multiprocessing import Pool
from subprocess import Popen, PIPE
from os.path import basename, splitext
from optparse import OptionParser
from os import remove, sep, close
from StringIO import StringIO
from tempfile import mkstemp
from itertools import count
from bz2 import decompress

import logging

from boto import connect_s3
from psycopg2 import connect

mercator = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +no_defs'

def ogr2ogr(dbinfo, index, table, file):
    '''
    '''
    try:
        cmd = '''ogr2ogr -t_srs mercator -nln table -overwrite
                         -lco ENCODING=UTF-8 -lco GEOMETRY_NAME=way
                         -select highway,name,long_name,zoomlevel,pixelwidth
                         -f PostgreSQL pgconnect file'''.split()
    
        cmd[2], cmd[4], cmd[-2], cmd[-1] = mercator, table, dbinfo, file
        
        logging.debug(' '.join(cmd))
        
        cmd = Popen(cmd)
        cmd.wait()
        
        assert cmd.returncode == 0, 'ogr2ogr returned with status=%d' % cmd.returncode
        
        remove(file)
        result = True
    
    except Exception, err:
        result = err

    return index, file, table, result

def ogr2ogr_callback((index, file, table, result)):
    '''
    '''
    if result is True:
        logging.info('%(index)d. Imported %(file)s to %(table)s' % locals())
    else:
        logging.info('%(index)d. Failed: %(result)s' % locals())

optparser = OptionParser(usage="""%prog [options] <s3 bucket/path> <db name>

Amazon S3 connection info is expected in ~/.boto, see:
    http://code.google.com/p/boto/wiki/BotoConfig""")

defaults = dict(host='localhost', user='gis', passwd=None, table='streets_altogether', loglevel=logging.INFO)

optparser.set_defaults(**defaults)

optparser.add_option('--host', dest='host',
                     help='Postgres hostname, default %(host)s.' % defaults)

optparser.add_option('-u', '--user', dest='user',
                     help='Postgres username, default "%(user)s".' % defaults)

optparser.add_option('-p', '--passwd', dest='passwd',
                     help='Postgres password, default "%(passwd)s".' % defaults)

optparser.add_option('-t', '--table', dest='table',
                     help='Postgres results table name, default "%(table)s".' % defaults)

optparser.add_option('-v', '--verbose', dest='loglevel',
                     action='store_const', const=logging.DEBUG,
                     help='Output extra progress information.')

optparser.add_option('-q', '--quiet', dest='loglevel',
                     action='store_const', const=logging.WARNING,
                     help='Output no progress information.')

if __name__ == '__main__':

    opts, (aws_path, db_name) = optparser.parse_args()
    
    aws_bucket, aws_prefix = aws_path.split(sep, 1)
    
    logging.basicConfig(level=logging.INFO, format='%(levelname)08s - %(message)s')
    
    s3 = connect_s3().get_bucket(aws_bucket)
    
    dbinfo = "PG:dbname='%s' host='%s' user='%s' password='%s'" % (db_name, opts.host, opts.user, opts.passwd)
    
    #    tables = []
    #    pool = Pool()
    #    
    #    for (key, index) in zip(s3.list(aws_prefix), count(1)):
    #        name = key.name
    #
    #        if not name.endswith('.bz2'):
    #            logging.debug('%(index)d. Skipping %(name)s' % locals())
    #            continue
    #    
    #        logging.info('%(index)d. Getting %(name)s' % locals())
    #        
    #        buffer = StringIO()
    #        key.get_contents_to_file(buffer)
    #        raw = decompress(buffer.getvalue())
    #        
    #        base, ext = splitext(basename(name))
    #        handle, file = mkstemp(dir='.', suffix='.json')
    #        close(handle)
    #        
    #        with open(file, 'w') as json:
    #            json.write(raw)
    #            json.close()
    #        
    #        table = 'streets_' + base.replace('-', '_').replace('.', '_')
    #        tables.append(table)
    #        
    #        pool.apply_async(ogr2ogr, (dbinfo, index, table, file), callback=ogr2ogr_callback)
    #    
    #    pool.close()
    #    pool.join()
    tables = 'streets_part_00000', 'streets_part_00001', 'streets_part_00002', 'streets_part_00003', 'streets_part_00004', 'streets_part_00005', 'streets_part_00006', 'streets_part_00007', 'streets_part_00008', 'streets_part_00009', 'streets_part_00010', 'streets_part_00011', 'streets_part_00012', 'streets_part_00013', 'streets_part_00014', 'streets_part_00015', 'streets_part_00016', 'streets_part_00017', 'streets_part_00018', 'streets_part_00019', 'streets_part_00020', 'streets_part_00021', 'streets_part_00022', 'streets_part_00023'
    
    #
    #
    #
    
    dest_table = opts.table
    
    logging.info('Creating table %(dest_table)s' % locals())

    db = connect(host=opts.host, database=db_name, user=opts.user, password=opts.passwd).cursor()
    
    db.execute('SELECT srid(way) FROM %s LIMIT 1' % tables[0])
    (srid, ) = db.fetchone()
    
    db.execute('BEGIN')
    db.execute('DROP TABLE IF EXISTS %(dest_table)s' % locals())
    
    db.execute('''CREATE TABLE %(dest_table)s
                  (
                      highway    TEXT,
                      name       TEXT,
                      long_name  TEXT,
                      zoomlevel  INT,
                      pixelwidth INT
                  )''' % locals())
    
    db.execute("SELECT AddGeometryColumn('%(dest_table)s', 'way', %(srid)d, 'LINESTRING', 2)" % locals())
    
    for tmp_table in tables:
        logging.debug('Inserting from %(tmp_table)s to %(dest_table)s' % locals())
    
        db.execute('''INSERT INTO %(dest_table)s
                      SELECT highway, name, long_name, zoomlevel, pixelwidth, way
                      FROM %(tmp_table)s''' % locals())
        
        db.execute('DROP TABLE %(tmp_table)s' % locals())
    
    logging.info('Indexing table %(dest_table)s' % locals())

    # Geohash idea from http://workshops.opengeo.org/postgis-intro/clusterindex.html
    db.execute('CREATE INDEX %(dest_table)s_gist ON %(dest_table)s USING GIST (way)' % locals())
    db.execute('CREATE INDEX %(dest_table)s_geohash ON %(dest_table)s (ST_Geohash(Transform(Centroid(way), 4326), 8))' % locals())
    db.execute('CREATE INDEX %(dest_table)s_highways ON %(dest_table)s (highway)' % locals())
    
    logging.info('Clustering table %(dest_table)s' % locals())

    db.execute('CLUSTER %(dest_table)s USING %(dest_table)s_geohash' % locals())

    db.execute('COMMIT')
    db.close()

