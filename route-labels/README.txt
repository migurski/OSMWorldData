1. ./local-run.sh

  Runs extract-routes.py, creates "YYYY-MM-DD-routes" directory, uploads to S3.

2. ./remote-run.sh <directory>

  Runs launch-routes.py to set up flotilla of EC2 spot instances,
  verify-routes.py to check for results, and download-routes.py to
  fetch output from S3, add to Postgres and create routes.json.bz2.
