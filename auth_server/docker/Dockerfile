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

RUN yum install -y python3 \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y epel-release.noarch gcc httpd httpd-devel mod_ssl openssl-devel.x86_64 \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y gridsite less curl \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN pip install mod_wsgi webpie \
    && rm -rf ~/.chache/pip
   




#python-pip libaio gcc python-devel.x86_64 mod_ssl openssl-devel.x86_64
	


ADD metacat.conf /etc/httpd/conf.d/
ADD metacat /metacat/
ADD host_cert.pem host_key.pem /metacat/
RUN rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf
RUN mod_wsgi-express module-config > /etc/httpd/conf.modules.d/10-wsgi.conf

EXPOSE 8080
EXPOSE 8443

