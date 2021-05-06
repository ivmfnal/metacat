#!/bin/bash

cd /metacat
export METACAT_SERVER_CFG=/metacat/config.yaml
export PYTHONPATH=/metacat/product/server:/metacat/product/lib:/metacat/wsdbtools
httpd
