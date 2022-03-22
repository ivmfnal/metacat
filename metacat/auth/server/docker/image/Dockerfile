FROM docker.io/ivmfnal/miniforge_centos_7

WORKDIR /tmp

#RUN yum install -y python3 \
#    && yum clean all \
#    && rm -rf /var/cache/yum

#RUN yum install -y epel-release.noarch gcc httpd httpd-devel mod_ssl openssl-devel.x86_64 \
#    && rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf \
#    && yum clean all \
#    && rm -rf /var/cache/yum

RUN yum install -y httpd httpd-devel mod_ssl \
    && rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y gridsite \
    && yum clean all \
    && rm -rf /var/cache/yum

#RUN conda update -n base -c defaults conda \
#	&& conda clean --all

#RUN pip install psycopg2-binary \
#    && rm -rf ~/.chache/pip

#RUN conda install -y -c conda-forge psycopg2 \
#    && yum clean all \
#    && rm -rf /var/cache/yum

RUN source /opt/conda/etc/profile.d/conda.sh && \
    conda activate && \
    pip install jinja2 webpie pyyaml pyjwt requests mod_wsgi \
    && rm -rf ~/.chache/pip \
    && mod_wsgi-express module-config > /etc/httpd/conf.modules.d/10-wsgi.conf \
    && yum clean all \
    && rm -rf /var/cache/yum

#RUN ln -s /metacat/metacat.conf /etc/httpd/conf.d/

WORKDIR /tmp
RUN git clone http://cdcvs.fnal.gov/projects/rwsi-wsdbtools
WORKDIR /tmp/rwsi-wsdbtools
RUN make HOME=/tmp

WORKDIR /tmp
RUN git clone https://github.com/ivmfnal/dm_common.git
RUN mkdir /tmp/auth_server
WORKDIR /tmp/dm_common
RUN make LIBDIR=/tmp/auth_server/dm_common

WORKDIR /tmp/auth_server
RUN ln -s /tmp/build/wsdbtools .
COPY auth_server.conf /etc/httpd/conf.d/
COPY start.sh /tmp/auth_server/

EXPOSE 8280
EXPOSE 8243

WORKDIR /tmp/auth_server



#ENTRYPOINT /metacat/run_server.sh

