FROM continuumio/miniconda3:latest

RUN apt-get update -yqq \
  && apt-get install -yqq --no-install-recommends \
  software-properties-common

# -- Install Python dependencies
RUN apt-get update -yqq && DEBIAN_FRONTEND=noninteractive apt-get install -yqq --fix-missing --no-install-recommends \
  build-essential \
  python3.7 \
  python3.7-dev \
  python3-pip \
  git \
  wget \
  redis-tools \
  && apt-get -q clean

RUN conda install --yes --freeze-installed \
    -c conda-forge \
    tini \
    && conda clean -tipsy \
    && find /opt/conda/ -type f,l -name '*.a' -delete \
    && find /opt/conda/ -type f,l -name '*.pyc' -delete \
    && find /opt/conda/ -type f,l -name '*.js.map' -delete \
    && rm -rf /opt/conda/pkgs

RUN mkdir /opt/app

COPY environment.yml /opt/environment.yml
RUN /opt/conda/bin/conda env update -f /opt/environment.yml

COPY app/ /opt/app/

COPY entrypoint.sh /usr/bin/entrypoint.sh
COPY start.sh /usr/bin/start.sh

WORKDIR /opt/app

EXPOSE 5000

ENTRYPOINT ["tini", "-g", "--", "/usr/bin/entrypoint.sh"]