#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Module for reporting the quality (dead links)
of data in the CKAN instance.
'''
import logging
import os
from collections import defaultdict

from ckan.lib.cli import CkanCommand
from jinja2 import Environment, FileSystemLoader
from pylons import config

from ckanext.govdatade.util import amend_portal
from ckanext.govdatade.util import copy_report_asset_files
from ckanext.govdatade.util import copy_report_vendor_files
from ckanext.govdatade.util import generate_general_data
from ckanext.govdatade.util import generate_link_checker_data


class Report(CkanCommand):

    '''Generates metadata quality report based on Redis data.'''

    summary = __doc__.split('\n')[0]

    NOTIFICATION_TYPE_BROKEN_LINKS = 'broken-link-notification'

    def __init__(self, name):
        super(Report, self).__init__(name)
        self.logger = logging.getLogger('ckanext.govdatade.reports')

    def generate_report(self):
        '''
        Generates the report
        '''
        data = defaultdict(defaultdict)

        generate_general_data(data)
        generate_link_checker_data(data)

        copy_report_asset_files()
        copy_report_vendor_files()

        templates = ['index.html', 'linkchecker.html']
        templates = [name + '.jinja2' for name in templates]

        for template_file in templates:
            rendered_template = self.render_template(template_file, data)
            self.write_validation_result(rendered_template, template_file)

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
        environment.globals.update(amend_portal=amend_portal)

        data['ckan_api_url'] = config.get('ckan.api.url.portal')
        data['govdata_detail_url'] = config.get(
            'ckanext.govdata.validators.report.detail.url'
        )

        template = environment.get_template(template_file)
        return template.render(data)

    @classmethod
    def write_validation_result(cls, rendered_template, template_file):
        '''
        Writes the report to the filesystem
        '''
        target_template = template_file.rstrip('.jinja2')

        target_dir = config.get('ckanext.govdata.validators.report.dir')

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        target_file = os.path.join(target_dir, target_template)
        target_file = os.path.abspath(target_file)

        file_handler = open(target_file, 'w')
        file_handler.write(rendered_template.encode('UTF-8'))
        file_handler.close()

    def command(self):
        '''
        Entry method for the command
        '''
        super(Report, self)._load_config()
        self.generate_report()
        report_path = os.path.normpath(
            config.get('ckanext.govdata.validators.report.dir')
        )
        info_message = "Wrote validation report to '%s'." % report_path
        self.logger.info(info_message)
