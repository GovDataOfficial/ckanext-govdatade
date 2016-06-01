#!/usr/bin/env python
# -*- coding: utf-8 -*-
from math import ceil
from collections import defaultdict

import json
import urllib2
import logging
import ast
import os
import ckanapi

from ckan import model
from ckan.model import Session
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckan.logic.schema import default_resource_schema

from ckanext.govdatade.util import normalize_action_dataset
from ckanext.govdatade.util import iterate_local_datasets
from ckanext.govdatade.util import generate_schema_checker_data
from ckanext.govdatade.validators import schema_checker
from ckanext.govdatade.validators import link_checker
from ckanext.govdatade.config import config

from jinja2 import Environment, FileSystemLoader
from jsonschema.validators import Draft3Validator

class SchemaChecker(CkanCommand):

    '''Validates datasets against the GovData.de JSON schema

    report                         Creates a validity report for all datasets
    specific <dataset-name>        Checks validity for a specific dataset
    remote <host-name>             Checks validity for datasets of a given remote host
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__
    options = None

    def __init__(self, name):
        super(SchemaChecker, self).__init__(name)
        self.logger = logging.getLogger('ckanext.govdatade.reports')

    def get_dataset_count(self, ckan_api_client):
        '''
        Return the amout of datasets
        '''
        self.logger.debug('Retrieving total number of datasets')

        return ckan_api_client.action.package_search(rows=1)['count']

    def get_datasets(self, ckan_api_client, rows, i):
        datasets = (i * 1000) + 1
        self.logger.debug(
            'Retrieving datasets %s - %s', datasets, datasets + rows - 1
        )

        records = ckan_api_client.action.package_search(
            rows=rows,
            start=rows * i
        )

        return records['results']

    @classmethod
    def render_template(cls, template_file, data):
        '''
        Renders the report template
        '''
        template_dir = os.path.dirname(__file__)
        template_dir = os.path.join(
            template_dir,
            '../',
            'report_assets/templates'
        )
        template_dir = os.path.abspath(template_dir)

        environment = Environment(loader=FileSystemLoader(template_dir))
        template = environment.get_template(template_file)

        return template.render(data)

    @classmethod
    def write_validation_result(cls, rendered_template, template_file):
        '''
        Writes the report to the filesystem
        '''
        target_file = template_file.rstrip('.jinja2')

        target_dir = config.get('ckanext.govdata.validators.report.dir')
        target_dir = os.path.join(target_dir, target_file)
        target_dir = os.path.abspath(target_dir)

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        file_handle = open(target_dir, 'w')
        file_handle.write(rendered_template.encode('UTF-8'))
        file_handle.close()

    def validate_dataset(self, dataset, data):
        '''
        Validates a given dataset
        '''
        normalize_action_dataset(dataset)

        identifier = dataset['id']
        portal = dataset['extras'].get('metadata_original_portal', 'null')

        data['broken_rules'][portal][identifier] = []
        broken_rules = data['broken_rules'][portal][identifier]

        data['datasets_per_portal'][portal].add(identifier)
        errors = Draft3Validator(self.schema).iter_errors(dataset)

        if Draft3Validator(self.schema).is_valid(dataset):
            data['valid_datasets'] += 1
        else:
            data['invalid_datasets'] += 1
            errors = Draft3Validator(self.schema).iter_errors(dataset)

            for error in errors:
                path = [e for e in error.path if isinstance(e, basestring)]
                path = str(' -> '.join(map((lambda e: str(e)), path)))

                data['field_paths'][path] += 1
                field_path_message = [path, error.message]
                broken_rules.append(field_path_message)

    @classmethod
    def create_context(cls):
        return {'model': model,
                'session': Session,
                'user': u'harvest',
                'schema': default_resource_schema,
                'validate': False}

    def generate_report(self):
        '''
        Generates the report
        '''
        self.logger.debug('Generating schema validation report')

        context = {'model': model,
                   'session': model.Session,
                   'ignore_auth': True}

        validator = schema_checker.SchemaChecker(
            config,
            schema=self.schema
        )

        num_datasets = 0
        active_datasets = []
        for i, dataset in enumerate(iterate_local_datasets(context)):
            self.logger.debug('Processing dataset %s', i)
            normalize_action_dataset(dataset)
            validator.process_record(dataset)
            num_datasets += 1
            active_datasets.append(dataset['id'])

        self.delete_deprecated_violations(active_datasets)
        general = {'num_datasets': num_datasets}
        validator.redis_client.set('general', general)

    def command(self):
        '''
        Entry method for the command
        '''
        super(SchemaChecker, self)._load_config()
        schema_url = config.get('ckanext.govdata.urls.schema')
        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        context = self.create_context()

        data = {'linkchecker': {},
                'schemachecker': {},
                'field_paths': defaultdict(int),
                'broken_rules': defaultdict(dict),
                'datasets_per_portal': defaultdict(set),
                'invalid_datasets': 0,
                'valid_datasets': 0}

        if len(self.args) > 0:
            sub_command = self.args[0]

            if len(self.args) > 2:
                error_message = 'Too many command arguments'
                self.logger.error(error_message)
                raise SystemError(error_message)
            elif sub_command == 'remote':
                if len(self.args) == 2:
                    remote_host = self.args[1]

                    ckan_api_client = ckanapi.RemoteCKAN(remote_host)

                    rows = 1000
                    total = self.get_dataset_count(ckan_api_client)
                    steps = int(ceil(total / float(rows)))

                    for i in range(0, steps):
                        if i == steps - 1:
                            rows = total - (i * rows)

                        datasets = self.get_datasets(ckan_api_client, rows, i)
                        for dataset in datasets:
                            self.validate_dataset(dataset, data)

                    generate_schema_checker_data(data)

                    self.write_validation_result(
                        self.render_template(
                            'schemachecker.html.jinja2',
                            data
                        )
                    )
                else:
                    error_message = 'No remote host provided'
                    self.logger.error(error_message)
                    raise SystemError(error_message)
            elif sub_command == 'specific':
                if len(self.args) == 2:
                    context = {'model': model,
                               'session': model.Session,
                               'ignore_auth': True}

                    package_show = get_action('package_show')
                    dataset_name = self.args[1]
                    dataset = package_show(
                        context,
                        {'id': dataset_name}
                    )

                    self.logger.debug('Processing dataset %s', dataset)

                    normalize_action_dataset(dataset)
                    validator = schema_checker.SchemaChecker(config)
                    validator.process_record(dataset)
                else:
                    error_message = 'No specific dataset name provided'
                    self.logger.error(error_message)
                    raise SystemError(error_message)
            elif sub_command == 'report':
                self.generate_report()
                self.logger.info('Generated schema check report data.')
            else:
                error_message = 'Invalid command call'
                self.logger.error(error_message)
                raise SystemError(error_message)
        else:
            error_message = 'Missing sub command'
            self.logger.error(error_message)
            raise SystemError(error_message)

    def delete_deprecated_violations(self, active_datasets):
        '''
        Deletes deprecated datasets from Redis
        '''
        validator = schema_checker.SchemaChecker(
            config,
            schema=self.schema
        )
        redis_client = validator.redis_client
        redis_dataset_ids = redis_client.keys()
        for redis_id in redis_dataset_ids:
            if redis_id not in active_datasets:
                record = redis_client.get(redis_id)
                if record is not None:
                    record = self.evaluate_record(record)
                    if link_checker.LinkChecker.SCHEMA_RECORD_KEY not in record:
                        redis_client.delete(redis_id)
                    else:
                        record.pop(validator.SCHEMA_RECORD_KEY, None)

                    info_message = 'Deleted deprecated schema violation for dataset'
                    info_message = info_message + ' %s from Redis' % str(redis_id)
                    self.logger.info(info_message)

    def evaluate_record(self, record):
        try:
            record = ast.literal_eval(unicode(record))
        except ValueError:
            self.logger.error('Redis dataset record evaluation error: %s', record)
        return record
