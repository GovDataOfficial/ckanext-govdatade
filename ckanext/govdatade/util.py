import json
import logging
import os
import ast
import ckan.logic as logic
import ckanapi
import distutils.dir_util

from ckanext.govdatade.validators import link_checker
from ckanext.govdatade.validators import schema_checker
from datetime import datetime
from collections import defaultdict
from math import ceil

from ckanext.govdatade.config import config

LOGGER = logging.getLogger(__name__)


def iterate_remote_datasets(endpoint, max_rows=1000):
    '''
    Iterates over a set of remote datasets
    '''
    ckan_api_client = ckanapi.RemoteCKAN(endpoint)

    LOGGER.info('Retrieve total number of datasets')

    total = ckan_api_client.action.package_search(rows=1)['count']

    steps = int(ceil(total / float(max_rows)))
    rows = max_rows

    for i in range(0, steps):
        if i == steps - 1:
            rows = total - (i * rows)

        min_range = (i * 1000) + 1
        max_range = min_range + rows - 1
        info_message = 'Retrieve datasets {min} - {max}'.format(
            min=min_range,
            max=max_range
        )
        LOGGER.info(info_message)

        records = ckan_api_client.action.package_search(
            rows=rows,
            start=rows * i
        )
        records = records['results']
        for record in records:
            yield record


def iterate_local_datasets(context):
    '''
    Iterates over the local datasets
    '''
    for dataset_name in logic.get_action('package_list')(context, {}):
        try:
            dataset = logic.get_action('package_show')(
                context,
                {'id': dataset_name}
            )
            yield dataset
        except logic.NotFound:
            print dataset_name


def normalize_api_dataset(dataset):
    '''
    Normalizes the given API version 1 dataset
    to a API version 3 dataset.
    '''
    if 'groups' in dataset:
        groups = []

        for group in dataset['groups']:
            groups.append({'id': group, 'name': group})

        dataset['groups'] = groups

    if 'tags' in dataset:
        tags = []
        for tag in dataset['tags']:
            tags.append({'name': tag})

        dataset['tags'] = tags

    if 'extras' in dataset and isinstance(dataset['extras'], dict):
        extras = []
        for key, value in dataset['extras'].iteritems():
            value_string = value
            if not isinstance(value, basestring):
                value_string = json.dumps(value)
            extras.append({'key': key, 'value': value_string})

        dataset['extras'] = extras


def normalize_action_dataset(dataset):
    '''
    Normalizes the given dataset
    '''
    dataset['groups'] = [group['name'] for group in dataset['groups']]
    dataset['tags'] = [tag['name'] for tag in dataset['tags']]

    extras = {}
    if 'extras' in dataset:
        for entry in dataset['extras']:
            if entry['key'] == 'temporal_granularity_factor':
                if entry['value'].isdigit():
                    entry['value'] = int(entry['value'])
            extras[entry['key']] = entry['value']

    dataset['extras'] = normalize_extras(extras)


def normalize_extras(source):
    '''
    Normalizes the extras key, values
    '''
    if isinstance(source, dict):
        result = {}
        for key, value in source.iteritems():
            result[key] = normalize_extras(value)
        return result
    elif isinstance(source, list):
        return [normalize_extras(item) for item in source]
    elif isinstance(source, basestring) and is_valid(source):
        return normalize_extras(json.loads(source))
    else:
        return source

def get_group_dict(group_name):
    '''
    Creates a group dict with the given name and returns this.
    '''
    group_dict = {}
    if group_name:
        group_dict = {'id': group_name, 'name': group_name}

    return group_dict


def remove_group_dict(group_dict_list, group_name):
    '''
    Removes group dict objects with the given name from the given group dict list
    and returns the group dict list without the group dicts with the given name.
    '''
    result = []
    for group_dict in group_dict_list:
        if 'name' not in group_dict:
            result.append(group_dict)
        elif group_dict['name'] != group_name:
            result.append(group_dict)

    return result


def fix_group_dict_list(group_dict_list):
    '''
    Fixes group dict objects by setting id to the name of the group, because ckanext-harvest is using
    id to resolve local groups.
    '''
    for group_dict in group_dict_list:
        if 'name' in group_dict:
            # Set id to name, because ckanext-harvest is using id to resolve local groups
            group_dict['id'] = group_dict['name']


def copy_report_vendor_files():
    '''
    Copies the report vendor files to the
    configured report directory.
    '''

    target_dir = config.get('ckanext.govdata.validators.report.dir')
    target_dir = os.path.join(target_dir, 'assets')
    target_dir = os.path.abspath(target_dir)

    vendor_dir = os.path.dirname(__file__)
    vendor_dir = os.path.join(
        vendor_dir,
        os.path.dirname(os.path.abspath(__file__)),
        'report_assets/vendor'
    )
    vendor_dir = os.path.abspath(vendor_dir)

    distutils.dir_util.copy_tree(vendor_dir, target_dir, update=1)


def boolize_config_value(value):
    '''
    Turns a config value into a boolean
    '''
    true_values = ["true", "on", 1, "1", True]
    value = str(value)
    if value.lower() in true_values:
        return True

    false_values = ["false", "off", 0, "0", False]
    if value.lower() in false_values:
        return False


def copy_report_asset_files():
    '''
    Copies the report asset files to the
    configured report directory.
    '''

    target_dir = config.get('ckanext.govdata.validators.report.dir')
    target_dir = os.path.join(target_dir, 'assets')
    target_dir = os.path.abspath(target_dir)

    vendor_dir = os.path.dirname(__file__)
    vendor_dir = os.path.join(
        vendor_dir,
        os.path.dirname(os.path.abspath(__file__)),
        'report_assets/assets'
    )
    vendor_dir = os.path.abspath(vendor_dir)

    distutils.dir_util.copy_tree(vendor_dir, target_dir, update=1)


def is_valid(source):
    '''
    Validates the given source.
    '''

    try:
        value = json.loads(source)
        if isinstance(value, dict):
            return True
        if isinstance(value, list):
            return True
        if isinstance(value, basestring):
            return True
    except ValueError:
        return False


def generate_link_checker_data(data):
    '''
    Generates the link validation data that
    goes into the Redis datasets.
    '''

    checker = link_checker.LinkChecker(config)
    redis = checker.redis_client

    if redis.get('general') is None:
        error_message = "Redis key '{redis_key}' not set".format(
            redis_key='general'
        )
        LOGGER.error(error_message)
        raise LookupError(error_message)

    try:
        num_metadata = ast.literal_eval(redis.get('general'))['num_datasets']
    except ValueError as err:
        error_message = 'Error retrieving number of datasets'
        error_message = error_message + ' from Redis.'
        LOGGER.error(error_message)
        raise err

    data['linkchecker'] = {}
    data['portals'] = defaultdict(int)
    data['entries'] = defaultdict(list)

    for record in checker.get_records():
        if checker.SCHEMA_RECORD_KEY not in record or not record[checker.SCHEMA_RECORD_KEY]:
            continue

        for dummy_url, entry in record[checker.SCHEMA_RECORD_KEY].iteritems():
            if isinstance(entry['status'], int):
                entry['status'] = 'HTTP %s' % entry['status']

        # legacy
        if 'metadata_original_portal' in record:
            portal = record['metadata_original_portal']
            data['portals'][portal] += 1
            data['entries'][portal].append(record)

    lc_stats = data['linkchecker']
    lc_stats['broken'] = sum(data['portals'].values())
    lc_stats['working'] = num_metadata - lc_stats['broken']

    LOGGER.info('Link checker data: %s', data)


def generate_schema_checker_data(data):
    '''
    Generates the schema validation data that
    goes into the Redis datasets.
    '''

    validator = schema_checker.SchemaChecker(config, schema=None)
    redis = validator.redis_client

    if redis.get('general') is None:
        error_message = "Redis key '{redis_key}' not set".format(
            redis_key='general'
        )
        LOGGER.error(error_message)
        raise LookupError(error_message)

    try:
        num_metadata = ast.literal_eval(redis.get('general'))['num_datasets']
    except ValueError as err:
        error_message = 'Error retrieving number of datasets'
        error_message = error_message + ' from Redis.'
        LOGGER.error(error_message)
        raise err

    if validator.SCHEMA_RECORD_KEY not in data:
        data[validator.SCHEMA_RECORD_KEY] = {}

    if 'schemachecker' not in data:
        data['schemachecker'] = {}

    data[validator.SCHEMA_RECORD_KEY]['portal_statistic'] = defaultdict(int)
    data[validator.SCHEMA_RECORD_KEY]['rule_statistic'] = defaultdict(int)
    data[validator.SCHEMA_RECORD_KEY]['broken_rules'] = defaultdict(defaultdict)

    portals = data[validator.SCHEMA_RECORD_KEY]['portal_statistic']
    rules = data[validator.SCHEMA_RECORD_KEY]['rule_statistic']
    broken_rules = data[validator.SCHEMA_RECORD_KEY]['broken_rules']

    broken = 0

    for record in validator.get_records():
        dataset_id = record['id']
        portal = record['metadata_original_portal']

        if validator.SCHEMA_RECORD_KEY not in record or not record[validator.SCHEMA_RECORD_KEY]:
            continue

        portals[portal] += 1

        if record[validator.SCHEMA_RECORD_KEY]:
            broken += 1

        broken_rules[portal][dataset_id] = {
            'name': record['name'],
            'maintainer': record['maintainer'],
            'broken_rules': record[validator.SCHEMA_RECORD_KEY]
        }

        for broken_rule in record[validator.SCHEMA_RECORD_KEY]:
            rules[broken_rule[0]] += 1

    sc_stats = data['schemachecker']
    sc_stats['broken'] = broken
    sc_stats['working'] = num_metadata - sc_stats['broken']

    LOGGER.info('Schema checker data: %s', data)


def generate_general_data(data):
    '''
    Generates the general data that
    goes into the Redis storage.
    '''

    validator = schema_checker.SchemaChecker(config, schema=None)
    redis = validator.redis_client

    if redis.get('general') is None:
        error_message = "Redis key '{redis_key}' not set".format(
            redis_key='general'
        )
        LOGGER.error(error_message)
        raise LookupError(error_message)

    try:
        data['num_datasets'] = ast.literal_eval(redis.get('general'))['num_datasets']
        data['timestamp'] = datetime.today().strftime("%Y-%m-%d um %H:%M")
    except ValueError as err:
        error_message = 'Error retrieving number of datasets'
        error_message = error_message + ' from Redis.'
        LOGGER.error(error_message)
        raise err


def amend_portal(portal):
    '''
    Amend the given portal string.
    '''
    portal = str(portal)

    mapping = [(':', '-'), ('/', '-'), ('.', '-'),
               ('&', '-'), ('?', '-'), ('=', '-')]

    for key, value in mapping:
        portal = portal.replace(key, value)

    return portal
