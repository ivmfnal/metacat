FROM python:3.10

RUN apt update
RUN apt install -y git curl less vim make

RUN pip install --upgrade pip
RUN pip install --user pyyaml psycopg2-binary jinja2 pythreader

WORKDIR /tmp
RUN git clone https://github.com/webpie/webpie
RUN cd webpie; python setup.py install --user

RUN pip install --user metacat
#RUN pip install --user 'webpie>=5.6.10'

RUN git clone http://cdcvs.fnal.gov/projects/rwsi-wsdbtools wsdbtools
RUN git clone https://github.com/ivmfnal/metacat product && \
    ln -s product/metacat /tmp && \
    ln -s product/metacat/auth/server /tmp

ADD config /config
ADD run.sh env.py /tmp/

EXPOSE 443


