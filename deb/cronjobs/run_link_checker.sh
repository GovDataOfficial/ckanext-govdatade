#!/bin/bash

export http_proxy="{{ http_proxy }}"
export https_proxy="{{ http_proxy }}"
export no_proxy="{{ no_proxy }}"

ip=$(echo $IP_MASTER | tr -d '\r')
/sbin/ip -o -4 addr list scope global | awk '{print $4}' | cut -d/ -f1 | grep "$ip"

if [ $? -eq 0 ]; then
  logger "Start GovData linkchecker"
  /usr/lib/ckan/env/bin/paster --plugin=ckanext-govdatade linkchecker --config=/etc/ckan/default/production.ini
  if [ $? -eq 0 ]; then
    logger "Finished GovData linkchecker"
  else
    logger "Failed GovData linkchecker. Exited with Status Code $?."
  fi
else
  logger "Host isn't master host"
fi
