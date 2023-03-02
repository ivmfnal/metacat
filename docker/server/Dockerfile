FROM conda/miniconda3-centos7

RUN yum upgrade -y \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum install -y git make \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN yum group install -y "Development Tools" \
    && yum clean all \
    && rm -rf /var/cache/yum

#RUN conda update -n base -c defaults conda
WORKDIR /tmp

RUN \ 
    pip install pythreader jinja2 webpie pyyaml pyjwt requests \
    && rm -rf ~/.chache/pip \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/US/Central /etc/localtime 

RUN mkdir /tmp/git
WORKDIR /tmp/git

RUN git clone https://github.com/ivmfnal/metacat.git 
RUN cd metacat; make HOME=/tmp generic
RUN git clone https://github.com/ivmfnal/wsdbtools
RUN cd wsdbtools; make HOME=/tmp

RUN ln -s /tmp/build/wsdbtools/wsdbtools /tmp
RUN ln -s /tmp/build/metacat /tmp/product
RUN ln -s /tmp/product/lib/metacat /tmp
RUN ln -s /tmp/product/server /tmp


#
# Token issuer
# 

RUN yum install -y httpd httpd-devel mod_ssl \
    && rm -f /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/ssl.conf \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN pip install mod_wsgi \
    && rm -rf ~/.chache/pip 

