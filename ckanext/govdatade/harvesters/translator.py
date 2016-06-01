#!/bin/env python

import json
import urllib2
import logging

from ckanext.govdatade.config import config

log = logging.getLogger(__name__)


def translate_groups(groups, source_name):
    log.debug('GROUPS: ')
    log.debug(groups)

    log.debug('SOURCE_NAME: ')
    log.debug(source_name)

    categories_path = config.get('ckanext.govdata.urls.categories')
    categories_file = categories_path + '/' + source_name + '2deutschland.json'

    log.debug('CATEGORY_FILE: ' + categories_file)

    json_string = urllib2.urlopen(categories_file).read()
    group_dict = json.loads(json_string)

    result = []
    for group in groups:
        if group in group_dict:
            result = result + group_dict[group]
            log.debug('FOUND_GROUP: ')
            log.debug(group_dict[group])
    return result
