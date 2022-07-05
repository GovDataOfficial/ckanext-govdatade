#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for cleaning the CKAN dataset, e.g. dataset activities.
'''
import sys
import time

import ckan.plugins.toolkit as tk
from ckan.lib.base import model
from ckan.lib.cli import CkanCommand

ROWS = 100


class Delete(CkanCommand):
    '''Deletes objects in the CKAN database, e.g. datasets.

    Usage:

      datasets {filter-query-params} [--dry-run]
        - Deletes all datasets matching the given {filter-query-params}.

        '''

    summary = __doc__.split('\n')[0]
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
            self._check_package_search_params(self.args[1:])
            self._check_options()
            self._delete_datasets()
        else:
            print 'Command %s not recognized' % cmd

    def _check_package_search_params(self, psfp_args):
        ''' Check package search parameters '''
        if psfp_args:
            try:
                # Basic validation of (multiple) params
                for psfp_arg in psfp_args:
                    splitted = psfp_arg.split(':', 1)
                    if len(splitted) != 2:
                        raise ValueError
                self.package_search_filter_params = ' '.join(str(psfp_arg) for psfp_arg in psfp_args)
            except ValueError:
                print 'ERROR: One or more package search filter params are not of the required' \
                    ' form \'fieldname:value\' !'
                sys.exit(1)
        else:
            print 'ERROR: Missing required parameter with package search filter params!'
            sys.exit(1)

    def _check_options(self):
        ''' Check if options are valid '''
        self.dry_run = True
        if self.options.dry_run:
            if self.options.dry_run.lower() not in ('yes', 'true', 'no', 'false'):
                self.parser.error('Value \'%s\' for dry-run is not a boolean!' \
                                  % str(self.options.dry_run))
            elif self.options.dry_run.lower() in ('no', 'false'):
                self.dry_run = False

    def _gather_dataset_ids(self):
        '''Collects all dataset ids matching the filter params.'''
        package_ids_found = []
        offset = 0
        count = 0
        package_search = tk.get_action('package_search')

        while offset <= count:
            query_object = {
                "fq": self.package_search_filter_params,
                "rows": ROWS,
                "start": offset
                }
            result = package_search({}, query_object)
            datasets = result["results"]
            count += len(datasets)
            print "DEBUG: offset: %s, count: %s" % (str(offset), str(count))
            offset += ROWS

            if count != 0:
                for dataset in datasets:
                    package_ids_found.append(dataset['id'])

        return set(package_ids_found)

    def _delete_datasets(self):
        '''Deletes all datasets matching package search filter query.'''
        starttime = time.time()
        package_ids_to_delete = self._gather_dataset_ids()
        endtime = time.time()
        print "INFO: %s datasets found for deletion. Total time: %s." % \
                (len(package_ids_to_delete), str(endtime - starttime))

        if self.dry_run:
            print "INFO: DRY-RUN: The dataset deletion is disabled."
        elif len(package_ids_to_delete) > 0:
            success_count = error_count = 0
            starttime = time.time()
            for package_id in package_ids_to_delete:
                try:
                    # Deleting package
                    checkpoint_start = time.time()
                    self._delete(package_id)
                    checkpoint_end = time.time()
                    print "DEBUG: Deleted dataset with id %s. Time taken for deletion: %s." % \
                             (package_id, str(checkpoint_end - checkpoint_start))
                    success_count += 1
                except Exception as error:
                    print 'ERROR: While deleting dataset with id %s. Details: %s' % \
                        (package_id, error.message)
                    error_count += 1

            endtime = time.time()
            print '============================================================='
            print "INFO: %s datasets successfully deleted. %s datasets couldn't deleted. Total time: %s." % \
                    (success_count, error_count, str(endtime - starttime))

    def _delete(self, dataset_ref):
        '''Deletes the dataset with the given ID.'''
        context = {'user': self.admin_user['name']}
        tk.get_action('package_delete')(context, {'id': dataset_ref})
