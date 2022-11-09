#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for cleaning the CKAN dataset, e.g. dataset activities.
'''
import sys
import six
import ckan.plugins.toolkit as tk
from ckan import model
from ckan.plugins.toolkit import CkanCommand
import ckanext.govdatade.commands.command_util as util

ROWS = 100


class Delete(CkanCommand):
    # pylint: disable=too-few-public-methods
    '''Deletes objects in the CKAN database, e.g. datasets.

    Usage:

      datasets {filter-query-params} [--dry-run]
        - Deletes all datasets matching the given {filter-query-params}.

        '''

    summary = __doc__.split('\n', maxsplit=1)[0]
    usage = __doc__

    def __init__(self, name):
        super(Delete, self).__init__(name)
        self.parser.add_option('--dry-run', dest='dry_run', default='True',
                               help='With dry-run True the deletion will be not executed. '
                               'The default is True.')

        self.admin_user = None
        self.package_search_filter_params = None
        self.dry_run = None

    def command(self):
        '''
        Check command
        '''
        super(Delete, self)._load_config()

        if len(self.args) < 2:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]

        # Getting/Setting default site user
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        self.admin_user = tk.get_action('get_site_user')(context, {})

        if cmd == 'datasets':
            self.package_search_filter_params = util.check_package_search_params(self.args[1:])
            self._check_options()
            util.delete_datasets(self.dry_run, self.package_search_filter_params, self.admin_user)
        else:
            print('Command %s not recognized' % cmd)

    def _check_options(self):
        ''' Check if options are valid '''
        self.dry_run = True
        if self.options.dry_run:
            if self.options.dry_run.lower() not in ('yes', 'true', 'no', 'false'):
                self.parser.error('Value \'%s\' for dry-run is not a boolean!' \
                                  % six.text_type(self.options.dry_run))
            elif self.options.dry_run.lower() in ('no', 'false'):
                self.dry_run = False
