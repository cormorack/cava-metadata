#!/bin/bash

set -x

source activate metadata-env
export FLASK_APP=__init__.py
export FLASK_ENV=development
flask run -h 0.0.0.0