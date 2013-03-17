from boto import connect_s3, connect_ec2

itype = 'c1.xlarge'
ami = 'ami-b6089bdf'
ec2 = connect_ec2()

# bid a half-penny over median
history = ec2.get_spot_price_history(instance_type=itype)
median = sorted([h.price for h in history])[len(history)/2]
bid = median + .005

#
#
#

bucket = 'osm-hadoop-data'
directory = '2013-03-11-routes'
key_name = 'whiteknight-id_rsa.pub'
access_key = ec2.access_key
secret_key = ec2.secret_key

#
#
#

s3 = connect_s3().get_bucket(bucket)

# group keys into hundreds
keys = [key for key in s3.list('%s/routes-geojson/routes-' % directory) if key.name.endswith('.json.bz2')]
prefixes = set([key.name[:-11] for key in keys])

ud_template = '''#!/bin/sh -ex

# This can't be $HOME, it has to be /root. Somehow,
# cloud-init has the wrong idea about root's home dir.
cd /root

apt-get update
apt-get upgrade -y
apt-get install -y bzip2 python-pip python-shapely python-numpy python-pyproj qhull-bin
pip install -U boto networkx Skeletron StreetNames

cat >.boto <<BOTO
[Credentials]
aws_access_key_id = %(access_key)s
aws_secret_access_key = %(secret_key)s

[Boto]
debug = 0
BOTO

curl -sOL http://169.254.169.254/latest/meta-data/instance-id
curl -sOL http://s3.amazonaws.com/%(bucket)s/%(directory)s/process-routes.py

python process-routes.py %(bucket)s %(prefix)s 12 13 14 15

python <<KILL

from boto import connect_ec2

instance = open('instance-id').read().strip()
connect_ec2().terminate_instances(instance)

KILL
'''

for prefix in sorted(prefixes):

    user_data = ud_template % locals()
    
    print 'bidding', bid, 'on', itype, 'for', '/'.join((bucket, prefix))
    
    # ec2.request_spot_instances(bid, ami, count=2, instance_type=itype, user_data=user_data, key_name=key_name)
