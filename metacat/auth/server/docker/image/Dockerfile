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

#RUN conda update -n base -c defaults conda \
#	&& conda clean --all

#RUN pip install psycopg2-binary \
#    && rm -rf ~/.chache/pip

#RUN conda install -y -c conda-forge psycopg2 \
#    && yum clean all \
#    && rm -rf /var/cache/yum

#RUN yum install -y gridsite \
#    && yum clean all \
#    && rm -rf /var/cache/yum


RUN source /opt/conda/etc/profile.d/conda.sh && \
    conda activate && \
    pip install --upgrade pip && \
    pip install --upgrade pythreader jinja2 webpie pyyaml pyjwt requests mod_wsgi scitokens \
    && rm -rf ~/.chache/pip \
    && mod_wsgi-express module-config > /etc/httpd/conf.modules.d/10-wsgi.conf \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/US/Central /etc/localtime 

WORKDIR /tmp
RUN git clone https://github.com/ivmfnal/wsdbtools.git
WORKDIR /tmp/wsdbtools
RUN make HOME=/tmp

WORKDIR /tmp
RUN echo 10 >/dev/null
RUN git clone https://github.com/ivmfnal/metacat.git 
RUN cd metacat; git checkout main

RUN mkdir /tmp/auth_server 
WORKDIR /tmp/auth_server
RUN cp -R /tmp/metacat/metacat/auth/server .

RUN ln -s /tmp/build/wsdbtools/wsdbtools /tmp/metacat/metacat .
COPY auth_server.conf /etc/httpd/conf.d/
COPY start.sh env.py /tmp/auth_server/

EXPOSE 8443

WORKDIR /tmp/auth_server

RUN rm -rf /root/.cache && \
	chmod go+rwx /root

ENTRYPOINT /tmp/auth_server/start.sh

