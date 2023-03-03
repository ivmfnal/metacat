FROM conda/miniconda3-centos7

RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/US/Central /etc/localtime 

RUN conda update -n base -c defaults conda \
    && conda clean --all --yes

RUN yum upgrade -y \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum group install -y "Development Tools" \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y git make httpd httpd-devel mod_ssl \
    && rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN mkdir -p /metacat

WORKDIR /metacat

RUN pip install python-ldap pyyaml jinja2 pythreader webpie pyjwt requests lark mod_wsgi \
    && mod_wsgi-express module-config > /etc/httpd/conf.modules.d/10-wsgi.conf \
    && rm -rf ~/.chache/pip 

RUN conda install -c conda-forge psycopg2

WORKDIR /tmp
RUN git clone https://github.com/ivmfnal/wsdbtools
RUN cd wsdbtools; make HOME=/tmp
RUN cp -R /tmp/build/wsdbtools/wsdbtools /metacat
RUN rm -rf /tmp/wsdbtools /tmp/build

WORKDIR /metacat
RUN git clone https://github.com/ivmfnal/metacat.git product

RUN ln -s /metacat/product/metacat /metacat
RUN ln -s /metacat/product/webserver /metacat/server
RUN ln -s /metacat/product/metacat/auth/server /metacat/auth_server

COPY env.py /metacat/auth_server

COPY entrypoint.sh /metacat
CMD /metacat/entrypoint.sh
