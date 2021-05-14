#!/bin/bash

cd /metacat
export METACAT_SERVER_CFG=/config/config.yaml
export PYTHONPATH=/metacat/product/server:/metacat/product/lib:/metacat/wsdbtools
httpd

while true; do
	sleep 1
done

