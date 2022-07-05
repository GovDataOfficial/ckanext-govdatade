#!/bin/bash

export http_proxy="{{ http_proxy }}"
export https_proxy="{{ http_proxy }}"
export no_proxy="{{ no_proxy }}"

EXPORT_DIRECTORY="/var/lib/ckan/dumps/metadata"
EXPORT_FILE=$EXPORT_DIRECTORY/govdata.de-metadata-weekly.json.gz

[ -d $EXPORT_DIRECTORY ] || mkdir -p $EXPORT_DIRECTORY

logger "Testing jq availabilty"
command -v /usr/bin/jq >/dev/null 2>&1

if [ $? -eq 0 ]; then
  logger "Start GovData metadata export"
  . /usr/lib/ckan/env/bin/activate
  /usr/lib/ckan/env/bin/ckanapi dump datasets --all -q -p 3 -c /etc/ckan/default/production.ini | /usr/bin/jq --slurp . | gzip > $EXPORT_FILE
  logger "Finished GovData metadata export"
else
  logger "Required jq command not available"
fi