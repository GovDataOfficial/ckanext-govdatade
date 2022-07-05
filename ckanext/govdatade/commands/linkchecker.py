#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Check if dataset links are available
'''
import json
import logging
import os

from pylons import config
from jinja2 import Environment, FileSystemLoader

from ckan import model
from ckan.model import Session
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckan.logic.schema import default_resource_schema
from ckanext.govdatade.util import normalize_action_dataset
from ckanext.govdatade.util import iterate_local_datasets
from ckanext.govdatade.util import iterate_remote_datasets
from ckanext.govdatade.util import generate_link_checker_data
from ckanext.govdatade.validators import link_checker


class LinkChecker(CkanCommand):

    '''Checks the availability of the dataset's URLs

    report                         Creates a report for all datasets
    specific <dataset-name>        Checks links for a specific dataset
    remote <host-name>             Checks links for datasets of a given remote host
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def __init__(self, name):
        super(LinkChecker, self).__init__(name)
        self.logger = logging.getLogger('ckanext.govdatade.reports.commands.linkchecker')

    def check_remote_host(self, endpoint):
        '''
        check if remote host is available
        '''
        checker = link_checker.LinkChecker(config)

        num_urls = 0
        num_success = 0
        for i, dataset in enumerate(iterate_remote_datasets(endpoint)):
            process_info = 'Process {id}'.format(id=i)
            self.logger.info(process_info)

            for resource in dataset['resources']:
                num_urls += 1
                url = resource['url'].encode('utf-8')
                response_code = checker.validate(url)

                if checker.is_available(response_code):
                    num_success += 1

    def generate_report(self):
        '''
        Generates the report
        '''
        self.logger.info('Generating dead link report.')
        data = {}
        generate_link_checker_data(data)

        self.logger.info('Report data %s', data)
        self.write_report(self.render_template(data))

    @classmethod
    def render_template(cls, data):
        '''
        Renders the report template
        '''
        template_file = 'linkchecker.html.jinja2'

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
    def write_report(cls, rendered_template):
        '''
        Writes the report to the filesystem
        '''
        target_dir = config.get('ckanext.govdata.validators.report.dir')
        target_dir = os.path.abspath(target_dir)

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        output = os.path.join(target_dir, 'linkchecker.html')

        file_handle = open(output, 'w')
        file_handle.write(rendered_template.encode('UTF-8'))
        file_handle.close()

    @classmethod
    def create_context(cls):
        '''
        Create context
        '''
        return {'model': model,
                'session': Session,
                'user': u'harvest',
                'schema': default_resource_schema,
                'validate': False}

    def command(self):
        '''
        Entry method for the command
        '''
        super(LinkChecker, self)._load_config()
        active_datasets = set()

        if len(self.args) == 0:

            context = {'model': model,
                       'session': model.Session,
                       'ignore_auth': True}

            validator = link_checker.LinkChecker(config)

            num_datasets = 0
            for dummy_index, dataset in enumerate(iterate_local_datasets(context)):
                normalize_action_dataset(dataset)
                try:
                    validator.process_record(dataset)
                    num_datasets += 1
                    active_datasets.add(dataset['id'])
                except Exception as ex:
                    self.logger.info('LinkChecker: Error while processing dataset %s. Details: %s',
                                     str(dataset['id']), ex.message)

            self.delete_deprecated_datasets(active_datasets)
            general = {'num_datasets': num_datasets}
            validator.redis_client.set('general', json.dumps(general))
            self.logger.info('Generated link check report data.')
        if len(self.args) > 0:
            subcommand = self.args[0]
            if subcommand == 'remote':
                self.check_remote_host(self.args[1])
            elif subcommand == 'report':
                self.generate_report()
            elif len(self.args) == 2 and self.args[0] == 'specific':
                dataset_name = self.args[1]

                context = {'model':       model,
                           'session':     model.Session,
                           'ignore_auth': True}

                package_show = get_action('package_show')
                validator = link_checker.LinkChecker(config)

                dataset = package_show(context, {'id': dataset_name})

                self.logger.info('Processing dataset %s', dataset)
                normalize_action_dataset(dataset)
                validator.process_record(dataset)

    def delete_deprecated_datasets(self, dataset_ids):
        '''
        Deletes deprecated datasets from Redis
        '''
        validator = link_checker.LinkChecker(config)
        redis_client = validator.redis_client
        redis_ids = redis_client.keys()
        for redis_id in redis_ids:
            if redis_id not in dataset_ids:
                record = redis_client.get(redis_id)
                if (record is not None) and (redis_id != 'general'):
                    redis_client.delete(redis_id)
                    self.logger.info('Deleted deprecated broken links information for dataset %s from Redis',
                                     str(redis_id))
