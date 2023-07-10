#!/bin/bash

ip=$(echo $IP_MASTER | tr -d '\r')
/sbin/ip -o -4 addr list scope global | awk '{print $4}' | cut -d/ -f1 | grep "$ip"

if [ $? -eq 0 ]; then
  logger "Start GovData clean up db"
  /usr/lib/ckan/env/bin/ckan --config=/etc/ckan/default/production.ini cleanupdb activities
  logger "Finished GovData clean up db"
else
  logger "Host isn't master host"
fi
