# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Cedric Serfon, <cedric.serfon@cern.ch>, 2017
# - Thomas Beermann, <thomas.beermann@cern.ch>, 2018
FROM conda/miniconda3-centos7

WORKDIR /tmp

#RUN yum install -y python3 \
#    && yum clean all \
#    && rm -rf /var/cache/yum

RUN yum install -y epel-release.noarch git gcc httpd httpd-devel mod_ssl openssl-devel.x86_64 \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y gridsite less curl \
    && yum clean all \
    && rm -rf /var/cache/yum

#RUN conda update -n base -c defaults conda \
#	&& conda clean --all

#RUN pip install psycopg2-binary \
#    && rm -rf ~/.chache/pip

RUN conda install -y -c anaconda psycopg2

RUN pip install mod_wsgi webpie lark pyyaml pyjwt requests python-ldap \
    && rm -rf ~/.chache/pip

ADD metacat /metacat/
RUN ln -s /metacat/metacat.conf /etc/httpd/conf.d/
RUN rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf
RUN mod_wsgi-express module-config > /etc/httpd/conf.modules.d/10-wsgi.conf

RUN echo 4

WORKDIR /tmp
RUN git clone https://github.com/ivmfnal/metacat.git
WORKDIR /tmp/metacat
RUN make; mv ~/build/metacat /metacat/product

WORKDIR /tmp
RUN git clone http://cdcvs.fnal.gov/projects/rwsi-wsdbtools
WORKDIR /tmp/rwsi-wsdbtools
RUN make; mv ~/build/wsdbtools /metacat

WORKDIR /metacat

EXPOSE 8180
EXPOSE 8143

ENTRYPOINT /metacat/run_server.sh

