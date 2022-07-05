#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for purging datasets.
'''
import csv
import logging
import sys
import time

import ckan.plugins.toolkit as tk
from ckan.lib.base import model
from ckan.lib.cli import CkanCommand
from pylons import config

LOGGER = logging.getLogger(__name__)


class Purge(CkanCommand):
    '''Purges datasets.'''

    summary = __doc__.split('\n')[0]

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
        self.path_to_logfile = config.get('ckanext.govdata.delete_deprecated_packages.logfile')
        if self.path_to_logfile is not None:
            print "INFO: Logging to file %s." % self.path_to_logfile
        else:
            print "WARN: Could not get log file path for purged datasets from configuration!"

        if cmd == 'deleted':
            self.purge_deleted_datasets()
        else:
            print 'Command %s not recognized' % cmd

    def purge_deleted_datasets(self):
        '''Purges all deleted datasets.'''

        starttime = time.time()
        # Query all deleted packages except harvest packages
        query = model.Session.query(model.Package).\
            filter_by(state=model.State.DELETED).filter(model.Package.type != 'harvest')

        success_count = 0
        error_count = 0
        for package_object in query:
            try:
                package_id = package_object.id
                # Purging package
                checkpoint_start = time.time()
                self._purge(package_id)
                checkpoint_end = time.time()
                # Log to file and command line
                self.log_deleted_packages_in_file(package_object, checkpoint_end)
                print "DEBUG: Purged dataset with id %s and name %s. Time taken for purging: %s." % \
                         (package_id, package_object.name, str(checkpoint_end-checkpoint_start))
                success_count += 1
            except Exception as error:
                print 'ERROR: While purging dataset with id %s. Details: %s' % (package_id, error.message)
                error_count += 1

        endtime = time.time()
        print '============================================================='
        print "INFO: %s datasets successfully purged. %s datasets couldn't purged. Total time: %s." % \
                 (success_count, error_count, str(endtime-starttime))

    def _purge(self, dataset_ref):
        '''Purges the dataset with the given ID.'''

        context = {'user': self.admin_user['name']}
        tk.get_action('dataset_purge')(context, {'id': dataset_ref})

    def log_deleted_packages_in_file(self, package_object, time_in_seconds):
        '''Write the information about the deleted packages in a file.'''

        if self.path_to_logfile is not None:
            try:
                with open(self.path_to_logfile, 'a') as logfile:
                    line = ([package_object.id, package_object.name, 'purged',
                             self.format_date_string(time_in_seconds)])
                    csv.writer(logfile).writerow(line)
            except Exception as exception:
                LOGGER.warn(
                    'Could not write in automated deletion log file at %s: %s',
                    self.path_to_logfile, exception
                )

    @classmethod
    def format_date_string(cls, time_in_seconds):
        '''Converts a time stamp to a string according to a format specification.'''

        struct_time = time.localtime(time_in_seconds)
        return time.strftime("%Y-%m-%d %H:%M", struct_time)
