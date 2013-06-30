from optparse import OptionParser
from itertools import product
from os.path import basename
from time import sleep, time
from os import sep
import logging

from boto import connect_s3, connect_cloudwatch
optparser = OptionParser(usage="""%prog [options] <s3 bucket/input path> <s3 bucket/output path>

Amazon S3 connection info is expected in ~/.boto, see:
    http://code.google.com/p/boto/wiki/BotoConfig""")

defaults = dict(loglevel=logging.INFO, namespace='OSM Streets and Routes')

optparser.set_defaults(**defaults)

optparser.add_option('-v', '--verbose', dest='loglevel',
                     action='store_const', const=logging.DEBUG,
                     help='Output extra progress information.')

optparser.add_option('-q', '--quiet', dest='loglevel',
                     action='store_const', const=logging.WARNING,
                     help='Output no progress information.')

optparser.add_option('-n', '--namespace', dest='namespace',
                     help='Cloudwatch namespace, default %s.' % repr(defaults['namespace']))

if __name__ == '__main__':

    opts, (input_path, output_path) = optparser.parse_args()
    
    input_bucket, input_prefix = input_path.split(sep, 1)
    output_bucket, output_prefix = output_path.split(sep, 1)
    
    logging.basicConfig(level=opts.loglevel, format='%(levelname)08s - %(message)s')
    
    input_s3 = connect_s3().get_bucket(input_bucket)
    output_s3 = connect_s3().get_bucket(output_bucket)
    cloudwatch = connect_cloudwatch()
    
    input_keys = (key for key in input_s3.list(input_prefix) if key.name.endswith('.json.bz2'))
    start_time = time()
    
    for (key, zoom) in product(input_keys, (12, 13, 14, 15)):
        output_keyname = '%s/%d-%s' % (output_prefix.rstrip('/'), zoom, basename(key.name))

        while True:
            logging.debug('Looking for %s...' % output_keyname)
            cloudwatch.put_metric_data(opts.namespace, 'Elapsed (Streets)', time() - start_time, unit='Seconds')

            if output_s3.get_key(output_keyname) is not None:
                logging.info('Found %s' % output_keyname)
                break

            sleep(5)
