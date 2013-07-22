from multiprocessing import Pool
from subprocess import Popen, PIPE
from os.path import basename, splitext
from optparse import OptionParser
from os import remove, sep, close
from StringIO import StringIO
from tempfile import mkstemp
from itertools import count
from bz2 import decompress
from time import strftime

import logging

from boto import connect_s3
from psycopg2 import connect

mercator = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +no_defs'

def ogr2ogr(dbinfo, index, table, file):
    '''
    '''
    try:
        cmd = '''ogr2ogr -t_srs <mercator> -nln <table> -overwrite
                         -lco ENCODING=UTF-8 -lco GEOMETRY_NAME=way -nlt GEOMETRY
                         -select network,ref,modifier,zoomlevel,pixelwidth
                         -f PostgreSQL <pgconnect> <file>'''.split()
    
        cmd[2], cmd[4], cmd[-2], cmd[-1] = mercator, table, dbinfo, file
        
        logging.debug(' '.join(cmd))
        
        ogr2ogr = Popen(cmd, stderr=PIPE)
        ogr2ogr.wait()
        
        #
        # sometimes, modifier column is missing, try without.
        #
        if ogr2ogr.returncode != 0:
            cmd[-6:-4] = '-sql', "SELECT network, ref, '' AS modifier, zoomlevel, pixelwidth FROM OgrGeoJSON"
            
            logging.debug(' '.join(cmd))
            
            ogr2ogr = Popen(cmd)
            ogr2ogr.wait()
            
            assert ogr2ogr.returncode == 0, 'ogr2ogr returned with status=%d' % cmd.returncode
            
        result = True
    
    except Exception, err:
        result = err
    
    finally:
        remove(file)

    return index, file, table, result

def ogr2ogr_callback((index, file, table, result)):
    '''
    '''
    if result is True:
        logging.info('%(index)d. Imported %(file)s to %(table)s' % locals())
    else:
        with open('errors.txt', 'a') as errors:
            print >> errors, '%(index)d. Failed: %(result)s' % locals()
    
        logging.info('%(index)d. Failed: %(result)s' % locals())

optparser = OptionParser(usage="""%prog [options] <s3 bucket/path> <db name>

Amazon S3 connection info is expected in ~/.boto, see:
    http://code.google.com/p/boto/wiki/BotoConfig""")

defaults = dict(host='localhost', user='osm2pgsql', passwd=None, table='routes_skeletron', srid=900913, loglevel=logging.INFO)

optparser.set_defaults(**defaults)

optparser.add_option('--host', dest='host',
                     help='Postgres hostname, default %(host)s.' % defaults)

optparser.add_option('-u', '--user', dest='user',
                     help='Postgres username, default "%(user)s".' % defaults)

optparser.add_option('-p', '--passwd', dest='passwd',
                     help='Postgres password, default "%(passwd)s".' % defaults)

optparser.add_option('-s', '--srid', dest='srid', type='int',
                     help='Postgres geometry SRID, default "%(srid)d".' % defaults)

optparser.add_option('-t', '--table', dest='table',
                     help='Postgres results table name, default "%(table)s".' % defaults)

optparser.add_option('-j', '--jobs', dest='jobs', type='int',
                     help='Number of processing jobs, default all.')

optparser.add_option('-v', '--verbose', dest='loglevel',
                     action='store_const', const=logging.DEBUG,
                     help='Output extra progress information.')

optparser.add_option('-q', '--quiet', dest='loglevel',
                     action='store_const', const=logging.WARNING,
                     help='Output no progress information.')

if __name__ == '__main__':

    opts, (aws_path, db_name) = optparser.parse_args()
    
    aws_bucket, aws_prefix = aws_path.split(sep, 1)
    
    logging.basicConfig(level=opts.loglevel, format='%(levelname)08s - %(message)s')
    
    s3 = connect_s3().get_bucket(aws_bucket)
    
    dbinfo = "PG:dbname='%s' host='%s' user='%s' password='%s'" % (db_name, opts.host, opts.user, opts.passwd)
    
    tables = []
    pool = Pool(opts.jobs)
    
    for (key, index) in zip(s3.list(aws_prefix), count(1)):
        name = key.name

        if not name.endswith('.bz2'):
            logging.debug('%(index)d. Skipping %(name)s' % locals())
            continue
    
        logging.info('%(index)d. Getting %(name)s' % locals())
        
        buffer = StringIO()
        key.get_contents_to_file(buffer)
        raw = decompress(buffer.getvalue())
        
        base, ext = splitext(basename(name))
        handle, file = mkstemp(dir='.', suffix='.json')
        close(handle)
        
        with open(file, 'w') as json:
            json.write(raw)
            json.close()
        
        table = 'routes_' + base.replace('-', '_').replace('.', '_')
        tables.append(table)
        
        pool.apply_async(ogr2ogr, (dbinfo, index, table, file), callback=ogr2ogr_callback)
    
    pool.close()
    pool.join()
    
    #
    #
    #
    
    srid = opts.srid
    dest_table = opts.table
    next_table = '%s_%s' % (dest_table, strftime('%Y%m%d%H%M'))
    
    logging.info('Creating table %(next_table)s' % locals())

    db = connect(host=opts.host, database=db_name, user=opts.user, password=opts.passwd).cursor()
    
    db.execute('''CREATE TABLE %(next_table)s
                  (
                      network    TEXT,
                      ref        TEXT,
                      modifier   TEXT,
                      zoomlevel  INT,
                      pixelwidth INT
                  )''' % locals())
    
    db.execute("SELECT AddGeometryColumn('%(next_table)s', 'way', %(srid)d, 'LINESTRING', 2)" % locals())
    
    for tmp_table in tables:
        logging.info('Inserting from %(tmp_table)s to %(next_table)s' % locals())
        
        db.execute('BEGIN')
        db.execute('SELECT count(way) FROM %(tmp_table)s' % locals())
        (rows, ) = db.fetchone()
        
        if rows:
            db.execute('''INSERT INTO %(next_table)s
                          SELECT network, ref, modifier, zoomlevel, pixelwidth, SetSRID((Dump(way)).geom, %(srid)d)
                          FROM %(tmp_table)s''' % locals())
        
        db.execute('DROP TABLE %(tmp_table)s' % locals())
        db.execute("DELETE FROM geometry_columns WHERE f_table_name = '%(tmp_table)s'" % locals())
        db.execute('COMMIT')
    
    logging.info('Indexing table %(next_table)s' % locals())

    # Geohash idea from http://workshops.opengeo.org/postgis-intro/clusterindex.html
    db.execute('CREATE INDEX %(next_table)s_gist ON %(next_table)s USING GIST (way)' % locals())
    db.execute('CREATE INDEX %(next_table)s_geohash ON %(next_table)s (ST_Geohash(Transform(Centroid(way), 4326), 8))' % locals())
    db.execute('CREATE INDEX %(next_table)s_networks ON %(next_table)s (network)' % locals())
    
    logging.info('Clustering table %(next_table)s' % locals())

    db.execute('CLUSTER %(next_table)s USING %(next_table)s_geohash' % locals())

    logging.info('Renaming to %(dest_table)s' % locals())

    db.execute('BEGIN')
    db.execute('DROP TABLE IF EXISTS %(dest_table)s' % locals())
    db.execute('ALTER TABLE %(next_table)s RENAME TO %(dest_table)s' % locals())
    db.execute("DELETE FROM geometry_columns WHERE f_table_name = '%(dest_table)s'" % locals())
    db.execute("UPDATE geometry_columns SET f_table_name = '%(dest_table)s' WHERE f_table_name = '%(next_table)s'" % locals())
    db.execute('COMMIT')
    
    #
    #
    #
    
    logging.info('Dumping %(dest_table)s to routes.json.bz2' % locals())

    cmd = 'ogr2ogr -t_srs EPSG:4326 -lco ENCODING=UTF-8 -lco COORDINATE_PRECISION=6 -f GeoJSON /vsistdout <db> <table>'.split()
    cmd[-2:] = "PG:dbname='%s' host='%s' user='%s' password='%s'" % (db_name, opts.host, opts.user, opts.passwd), opts.table
    
    ogr2ogr = Popen(cmd, stdout=PIPE)
    file = open('routes.json.bz2', 'w')
    bz = Popen(['bzip2', '-v'], stdin=ogr2ogr.stdout, stderr=PIPE, stdout=file)
    
    ogr2ogr.wait()
    bz.wait()
    file.close()
    
    logging.info('Dumped %s' % bz.stderr.read().strip())
