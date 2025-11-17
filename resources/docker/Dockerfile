ARG CONDA_VERSION=4.9.2
ARG PYTHON_VERSION=3.8

FROM continuumio/miniconda3

ENV CONDA_DIR /opt/conda

RUN ${CONDA_DIR}/bin/conda install -c conda-forge \
    python=$PYTHON_VERSION \
    uvicorn \
    gunicorn \
    mamba \
    && ${CONDA_DIR}/bin/mamba clean -afy \
    && find ${CONDA_DIR} -follow -type f -name '*.a' -delete \
    && find ${CONDA_DIR} -follow -type f -name '*.pyc' -delete

COPY ./start.sh /start.sh
RUN chmod +x /start.sh

COPY ./gunicorn_conf.py /gunicorn_conf.py

COPY ./start-reload.sh /
RUN chmod +x /start-reload.sh

COPY ./app /app

WORKDIR /app/

ENV PYTHONPATH=/app

EXPOSE 80

# Run the start script, it will check for an /app/prestart.sh script (e.g. for migrations)
# And then will start Gunicorn with Uvicorn
CMD ["/start.sh"]

COPY . /tmp

RUN echo "Checking for 'environment.yaml' file" \
        ; if [ -f /tmp/environment.yaml ] ; then \
        echo "Updating conda environment" \
        && ${CONDA_DIR}/bin/mamba env update --file /tmp/environment.yaml --name base \
        ; else \
        echo "Using default environment" \
        ; fi

