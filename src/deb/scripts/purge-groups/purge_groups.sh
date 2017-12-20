#!/bin/bash
filename="$1"
if [ $# -ne 3 ]
    then
        echo "Erwarte drei Parameter 1: Datei mit den zu loeschenden IDs, 2: Die Datei fuer die erfolgreichen IDs, 3: Die Datei fuer die fehlerhaften IDs"
        exit 1
fi

while read -r line || [ -n "$line" ]
do
    group_id=$(echo $line)
    echo "Purging group with ID $group_id ..."

    /usr/lib/ckan/env/bin/ckanapi action group_purge id=$group_id --config=/etc/ckan/default/production.ini
    exitcode=$?

    if [ $exitcode -ne 0 ]
        then
            echo $group_id >> $3
        else
            echo $group_id >> $2
    fi

    echo "Purge of group with ID $group_id ended with exit code=$exitcode."
done < "$filename"
exit 0
