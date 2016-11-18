#!/bin/env python

import json
import urllib2
import logging

from ckanext.govdatade.config import config

LOGGER = logging.getLogger(__name__)


def extract_groups_from_dicts(groups):
    '''
    Extract group names in dicts to
    a flat list.
    '''
    extracted_groups = []

    if isinstance(groups, list) and len(groups) == 0:
        return extracted_groups

    if isinstance(groups[0], dict):
        for group_dict in groups:
            if 'name' in group_dict:
                extracted_groups.append(
                    group_dict.get('name')
                )

    return extracted_groups


def translate_groups(groups, source_name):
    LOGGER.debug('GROUPS: ')
    LOGGER.debug(groups)

    LOGGER.debug('SOURCE_NAME: ')
    LOGGER.debug(source_name)

    categories_path = config.get('ckanext.govdata.urls.categories')
    categories_file = categories_path + '/' + source_name + '2deutschland.json'

    LOGGER.debug('CATEGORY_FILE: ' + categories_file)

    json_string = urllib2.urlopen(categories_file).read()
    group_dict = json.loads(json_string)

    extracted_groups = extract_groups_from_dicts(groups)
    if len(extracted_groups) > 0:
        groups = extracted_groups

    result = []
    for group in groups:
        if group in group_dict:
            local_group_name = group_dict[group][0]
            temp_single_group_dict = {'id': local_group_name, 'name': local_group_name}
            result.append(temp_single_group_dict)
            LOGGER.debug('FOUND_GROUP: ')
            LOGGER.debug(group_dict[group])
    return result
