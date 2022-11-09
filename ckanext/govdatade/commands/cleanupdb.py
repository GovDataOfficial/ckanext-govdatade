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

DAYS_TO_SUBTRACT_DEFAULT = 30

class CleanUpDb(CkanCommand):
    '''Clean up the CKAN database, e.g. dataset activities.

    Usage:

      activities [--older-than-days={days}]
        - Deletes all activities older than the given {days}. Default is 30 days.

        '''

    summary = __doc__.split('\n', maxsplit=1)[0]
    usage = __doc__

    def __init__(self, name):
        super(CleanUpDb, self).__init__(name)

        self.parser.add_option('-o', '--older-than-days', dest='days', default=False,
                               help='Objects older than the defined days are deleted. '
                               'The default is %d days.' % DAYS_TO_SUBTRACT_DEFAULT)

        self.admin_user = None
        self.days_to_subtract = DAYS_TO_SUBTRACT_DEFAULT

    def command(self):
        '''
        Check command
        '''
        super(CleanUpDb, self)._load_config()

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]

        # Getting/Setting default site user
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        self.admin_user = tk.get_action('get_site_user')(context, {})

        if cmd == 'activities':
            self.check_option_days()
            util.delete_activities(self.days_to_subtract)
        else:
            print('Command %s not recognized' % cmd)

    def check_option_days(self):
        ''' Check value for option days '''
        if self.options.days:
            try:
                self.days_to_subtract = int(self.options.days)
            except ValueError:
                print('ERROR Value \'%s\' for days is not a number!' % six.text_type(self.options.days))
                sys.exit(1)
        else:
            print('INFO Using default of %d days.' % self.days_to_subtract)
