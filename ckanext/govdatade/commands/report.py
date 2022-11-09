#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Module for reporting the quality (dead links)
of data in the CKAN instance.
'''
import logging
import os

from ckan.plugins.toolkit import CkanCommand
from ckan.plugins import toolkit as tk

from ckanext.govdatade.commands import command_util


class Report(CkanCommand):
    # pylint: disable=too-few-public-methods

    '''Generates metadata quality report based on Redis data.'''

    summary = __doc__.split('\n', maxsplit=1)[0]

    def __init__(self, name):
        super(Report, self).__init__(name)
        self.logger = logging.getLogger('ckanext.govdatade.reports')

    def command(self):
        '''
        Entry method for the command
        '''
        super(Report, self)._load_config()
        command_util.generate_report()
        report_path = os.path.normpath(
            tk.config.get('ckanext.govdata.validators.report.dir')
        )
        info_message = "Wrote validation report to '%s'." % report_path
        self.logger.info(info_message)
