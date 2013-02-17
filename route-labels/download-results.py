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
                         -select network,ref,modifier,zoomlevel,pixelwidth
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

optparser = OptionParser(usage="""%prog [options] <aws key> <aws secret> <s3 bucket/path> <db name> <db table>""")

defaults = dict(host='localhost', user='gis', passwd=None, table='routes_altogether', loglevel=logging.INFO)

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

    opts, (aws_key, aws_secret, aws_path, db_name, db_table) = optparser.parse_args()
    
    aws_bucket, aws_prefix = aws_path.split(sep, 1)
    
    logging.basicConfig(level=logging.INFO, format='%(levelname)08s - %(message)s')
    
    s3 = connect_s3(aws_key, aws_secret).get_bucket(aws_bucket)
    
    dbinfo = "PG:dbname='%s' host='%s' user='%s' password='%s'" % (db_name, opts.host, opts.user, opts.passwd)
    
    tables = []
    pool = Pool()
    
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
    
    dest_table = opts.table
    
    logging.info('Creating table %(dest_table)s' % locals())

    db = connect(host=opts.host, database=db_name, user=opts.user, password=opts.passwd).cursor()
    
    db.execute('SELECT srid(way) FROM %s LIMIT 1' % tables[0])
    (srid, ) = db.fetchone()
    
    db.execute('BEGIN')
    db.execute('DROP TABLE IF EXISTS %(dest_table)s' % locals())
    
    db.execute('''CREATE TABLE %(dest_table)s
                  (
                      network    TEXT,
                      ref        TEXT,
                      modifier   TEXT,
                      zoomlevel  INT,
                      pixelwidth INT
                  )''' % locals())
    
    db.execute("SELECT AddGeometryColumn('%(dest_table)s', 'way', %(srid)d, 'MULTILINESTRING', 2)" % locals())
    
    for tmp_table in tables:
        logging.debug('Inserting from %(tmp_table)s to %(dest_table)s' % locals())
    
        db.execute('''INSERT INTO %(dest_table)s
                      SELECT network, ref, modifier, zoomlevel, pixelwidth, way
                      FROM %(tmp_table)s''' % locals())
        
        db.execute('DROP TABLE %(tmp_table)s' % locals())
    
    logging.info('Indexing table %(dest_table)s' % locals())

    # Geohash idea from http://workshops.opengeo.org/postgis-intro/clusterindex.html
    db.execute('CREATE INDEX %(dest_table)s_geohash ON routes_altogether (ST_Geohash(Transform(way, 4326)))' % locals())
    db.execute('CREATE INDEX %(dest_table)s_networks ON routes_altogether (network)' % locals())
    db.execute('CLUSTER %(dest_table)s USING routes_altogether_geohash' % locals())

    db.execute('COMMIT')
    db.close()

