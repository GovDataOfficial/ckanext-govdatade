#!/bin/bash

export http_proxy="{{ http_proxy }}"
export https_proxy="{{ http_proxy }}"
export no_proxy="{{ no_proxy }}"

logger "Start GovData report generator"
/usr/lib/ckan/env/bin/ckan --config=/etc/ckan/default/production.ini report
logger "Finished GovData report generator"

