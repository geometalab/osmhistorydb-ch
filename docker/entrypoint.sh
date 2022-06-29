#!/bin/bash

set -e

python /code/initialize_data.py

# echo 'load initial changeset data'
# wget 
# https://osm-internal.download.geofabrik.de/europe/switzerland-internal.osh.pbf

exec "$@"
