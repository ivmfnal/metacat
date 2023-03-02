FROM conda/miniconda3-centos7

#RUN conda update -n base -c defaults conda
WORKDIR /tmp

RUN source /opt/conda/etc/profile.d/conda.sh && \
    conda activate && \
    pip install pythreader jinja2 webpie pyyaml pyjwt requests mod_wsgi \
    && rm -rf ~/.chache/pip \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/US/Central /etc/localtime 

RUN git clone https://github.com/ivmfnal/metacat.git 
RUN cd metacat; git checkout main
