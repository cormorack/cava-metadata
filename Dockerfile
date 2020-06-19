FROM cormorack/uvicorn-gunicorn:conda4.7.12-alpine

USER root
RUN rm -rf app

USER anaconda:anaconda
COPY ./app app

COPY ./environment.yml environment.yml

RUN /opt/conda/bin/conda env update -f ~/environment.yml