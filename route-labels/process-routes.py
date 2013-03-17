from sys import argv
from itertools import product
from multiprocessing import Pool
from os import write, close, remove
from subprocess import Popen, PIPE
from os.path import split, join, dirname
from StringIO import StringIO
from tempfile import mkstemp
from random import shuffle
from gzip import GzipFile
from bz2 import BZ2File
from re import compile
from time import sleep

import logging
import json

from boto import connect_s3

float_pat = compile(r'^-?\d+\.\d+(e-?\d+)?$')
charfloat_pat = compile(r'^[\[,\,]-?\d+\.\d+(e-?\d+)?$')

def download_input(input_key):
    '''
    '''
    handle, filename_input = mkstemp(dir='.', prefix='input-', suffix='.json.bz2')
    close(handle)
    
    input_key.get_contents_to_filename(filename_input)
    
    return filename_input

def generalize_input(filename_input, output_keyname, zoomlevel, pixelwidth):
    '''
    '''
    handle, filename_thruput = mkstemp(dir='.', prefix='through-', suffix='.json.gz')
    close(handle)
    
    handle, filename_stderr = mkstemp(dir='.', prefix='stderr-', suffix='.txt')
    close(handle)
    
    file_thruput = open(filename_thruput, 'w')
    file_stderr = open(filename_stderr, 'w')

    bzcat = Popen(('bzcat', filename_input), stdout=PIPE)
    generalize = 'skeletron-generalize.py -q -z %d -w %d /dev/stdin /dev/stdout' % (zoomlevel, pixelwidth)
    generalize = Popen(generalize.split(), stdin=bzcat.stdout, stdout=PIPE, stderr=file_stderr)
    gzip = Popen('gzip -c'.split(), stdin=generalize.stdout, stdout=file_thruput)
    
    while True:
        polled = generalize.poll()
        
        if polled is not None:
            # looks like we are done?
            break
        
        if s3.get_key(output_keyname) is not None:
            # the output now exists, kill and back out
            logging.info('Killing %s' % output_keyname)

            bzcat.kill()
            generalize.kill()
            gzip.kill()
            
            file_thruput.close()
            file_stderr.close()

            remove(filename_thruput)
            return None, filename_stderr

        sleep(15)
    
    bzcat.wait()
    generalize.wait()
    gzip.wait()
    
    file_thruput.close()
    file_stderr.close()

    return filename_thruput, filename_stderr

def modify_throughput(filename_thruput, zoomlevel, pixelwidth):
    '''
    '''
    geojson = json.load(GzipFile(filename_thruput))
    
    for feature in geojson['features']:
        feature['properties']['zoomlevel'] = zoomlevel
        feature['properties']['pixelwidth'] = pixelwidth
    
    return geojson

def encode_output(geojson):
    '''
    '''
    handle, filename_output = mkstemp(dir='.', prefix='output-', suffix='.json.bz2')
    close(handle)
    
    encoder = json.JSONEncoder(separators=(',', ':'))
    encoded = encoder.iterencode(geojson)
    format = '%.5f'
    
    output = BZ2File(filename_output, 'w')

    for token in encoded:
        if charfloat_pat.match(token):
            # in python 2.7, we see a character followed by a float literal
            output.write(token[0] + format % float(token[1:]))
            
        elif float_pat.match(token):
            # in python 2.6, we see a simple float literal
            output.write(format % float(token))
            
        else:
            output.write(token)

    output.close()
    
    return filename_output

def process_routes(bucketname, input_keyname, output_keyname, zoomlevel, pixelwidth):
    '''
    '''
    s3 = connect_s3().get_bucket(bucketname)
    
    if s3.get_key(output_keyname) is not None:
        logging.info('Skipping %s' % output_keyname)
        return
    
    logging.info('Starting %s' % output_keyname)

    garbage = set()

    try:
        input_key = s3.get_key(input_keyname)

        filename_input = download_input(input_key)
        garbage.add(filename_input)

        filename_thruput, filename_stderr = generalize_input(filename_input, output_keyname, zoomlevel, pixelwidth)
        garbage.add(filename_stderr)

        if filename_thruput is None:
            # Probably killed?
            return

        stderr_key = s3.new_key(output_keyname + '.stderr')
        stderr_key.set_contents_from_filename(filename_stderr, policy='public-read')
        
        garbage.add(filename_thruput)
        geojson = modify_throughput(filename_thruput, zoomlevel, 15)
        
        filename_output = encode_output(geojson)
        garbage.add(filename_output)

        output_key = s3.new_key(output_keyname)
        output_key.set_contents_from_filename(filename_output, policy='public-read')
        
    except Exception, e:
        logging.info('Errored %s: %s' % (output_keyname, str(e)))
    
    else:
        logging.info('Finished %s' % output_keyname)
    
    finally:
        for filename in garbage:
            logging.info('Deleting %s' % filename)
            remove(filename)

def get_tasks(s3, prefix, zooms):
    '''
    '''
    tasks = []
    
    for (zoomlevel, input_key) in product(zooms, s3.list(prefix=prefix)):
    
        dir, file = split(input_key.name)
        output_keyname = join(dirname(dir), join('output', '%d-%s' % (zoomlevel, file)))
        
        task = input_key.name, output_keyname, zoomlevel
        tasks.append(task)
    
    return tasks

if __name__ == '__main__':

    bucketname, prefix = argv[1:3]
    zooms = map(int, argv[3:])

    logging.basicConfig(format='%(levelname)s, %(process)d: %(message)s', level=logging.INFO)
    
    s3 = connect_s3().get_bucket(bucketname)
    pool = Pool()
    
    logging.info('Getting tasks')
    tasks = get_tasks(s3, prefix, zooms)

    shuffle(tasks)
    
    for (input_keyname, output_keyname, zoomlevel) in tasks:
        pool.apply_async(process_routes, (bucketname, input_keyname, output_keyname, zoomlevel, 15))
        
    pool.close()
    pool.join()
