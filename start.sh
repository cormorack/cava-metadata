#!/bin/bash

set -x

source activate metadata-env
if [[ $DEVELOPMENT_MODE ]]; then
    export FLASK_APP=__init__.py
    export FLASK_ENV=development
    flask run -h 0.0.0.0
else
    cd ../
    gunicorn -b 0.0.0.0:5000 "app:create_app()"
fi;