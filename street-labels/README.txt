1. ./local-run.sh

  Runs extract-streets.py, creates "YYYY-MM-DD-streets" directory, uploads to S3.

2. ./remote-run.sh <directory>

  Runs launch-streets.py to set up flotilla of EC2 spot instances,
  verify-streets.py to check for results, and download-streets.py to
  fetch output from S3, add to Postgres and create streets.json.bz2.
