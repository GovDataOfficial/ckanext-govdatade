#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for purging datasets.
'''
import logging
import sys
from ckan import model
from ckan.plugins.toolkit import CkanCommand
from ckan.plugins import toolkit as tk
from ckanext.govdatade.commands import command_util

LOGGER = logging.getLogger(__name__)


class Purge(CkanCommand):
    # pylint: disable=too-few-public-methods
    '''Purges datasets.'''

    summary = __doc__.split('\n', maxsplit=1)[0]

    def __init__(self, name):
        super(Purge, self).__init__(name)
        self.admin_user = None
        self.path_to_logfile = None

    def command(self):
        '''
        Check command
        '''
        super(Purge, self)._load_config()

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]

        # Getting/Setting default site user
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        self.admin_user = tk.get_action('get_site_user')(context, {})

        # Getting/Setting path to log file for auto deleted/purged packages
        self.path_to_logfile = tk.config.get('ckanext.govdata.delete_deprecated_packages.logfile')
        if self.path_to_logfile is not None:
            print("INFO: Logging to file %s." % self.path_to_logfile)
        else:
            print("WARN: Could not get log file path for purged datasets from configuration!")

        if cmd == 'deleted':
            command_util.purge_deleted_datasets(self.path_to_logfile, self.admin_user)
        else:
            print('Command %s not recognized' % cmd)
