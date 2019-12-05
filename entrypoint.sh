#!/bin/bash

set -x

# /opt/conda/bin/conda init bash
until redis-cli -h $REDIS_HOST -p $REDIS_PORT ping | grep "PONG" ; do
  echo >&2 "$(date +%Y-%m-%dT%H-%M-%S) Redis is unavailable - sleeping"
  sleep 1
done
echo >&2 "$(date +%Y-%m-%dT%H-%M-%S) Redis is up - starting server"

# Run extra commands
exec /usr/bin/start.sh