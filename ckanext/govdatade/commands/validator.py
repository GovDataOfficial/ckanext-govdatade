#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan import model
from ckan.model import Session
from ckan.lib.cli import CkanCommand
from ckan.logic.schema import default_package_schema

from ckanext.govdatade import CONFIG
from ckanext.govdatade.util import copy_report_vendor_files
from ckanext.govdatade.util import copy_report_asset_files
from ckanext.govdatade.util import normalize_action_dataset
from ckanext.govdatade.util import generate_link_checker_data
from ckanext.govdatade.util import iterate_local_datasets

from collections import defaultdict
from jinja2 import Environment, FileSystemLoader
from jsonschema.validators import Draft3Validator
from math import ceil

import ckanclient
import json
import os
import urllib2


class Validator(CkanCommand):
    '''Validates datasets against the GovData.de JSON schema'''

    summary = __doc__.split('\n')[0]

    SCHEMA_URL = 'https://raw.github.com/fraunhoferfokus/ogd-metadata/master/OGPD_JSON_Schema.json'  # NOQA

    def __init__(self, name):
        super(Validator, self).__init__(name)
        self.schema = json.loads(urllib2.urlopen(self.SCHEMA_URL).read())

    def get_dataset_count(self, ckan):
        print 'Retrieve total number of datasets'
        return ckan.action('package_search', rows=1)['count']

    def get_datasets(self, ckan, rows, i):
        datasets = (i * 1000) + 1
        print 'Retrieve datasets %s - %s' % (datasets, datasets + rows - 1)

        records = ckan.action('package_search', rows=rows, start=rows * i)
        return records['results']

    def render_template(self, template_file, data):

        template_dir = os.path.dirname(__file__)
        template_dir = os.path.join(template_dir, '../../..', 'lib/templates')
        template_dir = os.path.abspath(template_dir)

        environment = Environment(loader=FileSystemLoader(template_dir))
        template = environment.get_template(template_file)
        return template.render(data)

    def write_validation_result(self, rendered_template, template_file):
        target_file = template_file.rstrip(".jinja2")

        target_dir = CONFIG.get('validators', 'report_dir')
        target_dir = os.path.join(target_dir, target_file)
        target_dir = os.path.abspath(target_dir)

        fd = open(target_dir, 'w')
        fd.write(rendered_template.encode('UTF-8'))
        fd.close()

    def validate_datasets(self, dataset, data):
        normalize_action_dataset(dataset)

        identifier = dataset['id']
        portal = dataset['extras'].get('metadata_original_portal', 'null')
        portal = portal.replace('http://', '')
        portal = portal.replace('/', '')

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

    def create_context(self):
        return {'model':       model,
                'session':     Session,
                'user':        u'harvest',
                'schema':      default_package_schema,
                'validate':    False}

    def command(self):
        super(Validator, self)._load_config()
        context = self.create_context()

        data = {'field_paths':         defaultdict(int),
                'broken_rules':        defaultdict(dict),
                'datasets_per_portal': defaultdict(set),
                'invalid_datasets':    0,
                'valid_datasets':      0}

        if len(self.args) == 0:

            context = {'model':       model,
                       'session':     model.Session,
                       'ignore_auth': True}

            for i, dataset in enumerate(iterate_local_datasets(context)):
                print 'Processing dataset %s' % i
                self.validate_datasets(dataset, data)
                break

            copy_report_asset_files()
            copy_report_vendor_files()

            generate_link_checker_data(data)

            template_file = 'schema-validation.html.jinja2'
            rendered_template = self.render_template(template_file, data)
            self.write_validation_result(rendered_template, template_file)

            template_file = 'index.html.jinja2'
            rendered_template = self.render_template(template_file, data)
            self.write_validation_result(rendered_template, template_file)

        elif len(self.args) == 2 and self.args[0] == 'remote':
            endpoint = self.args[1]
            ckan = ckanclient.CkanClient(base_location=endpoint)

            rows = 1000
            total = self.get_dataset_count(ckan)
            steps = int(ceil(total / float(rows)))

            for i in range(0, steps):
                if i == steps - 1:
                    rows = total - (i * rows)

                datasets = self.get_datasets(ckan, rows, i)
                self.validate_datasets(datasets, data)

            self.write_validation_result(self.render_template(data))
