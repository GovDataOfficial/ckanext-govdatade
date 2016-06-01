#!/bin/bash

export http_proxy="{{ http_proxy }}"
export https_proxy="{{ http_proxy }}"
export no_proxy="{{ no_proxy }}"

logger "Start GovData report generator"
/usr/lib/ckan/env/bin/paster --plugin=ckanext-govdatade report --config=/etc/ckan/default/production.ini
logger "Finished GovData report generator"

