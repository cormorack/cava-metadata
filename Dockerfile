FROM continuumio/miniconda3:latest

RUN conda update --yes conda \
    && conda install --yes --freeze-installed \
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