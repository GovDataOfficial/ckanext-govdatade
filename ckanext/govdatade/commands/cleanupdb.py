#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for cleaning the CKAN dataset, e.g. dataset activities.
'''
import sys
import time
from datetime import datetime, timedelta

import ckan.plugins.toolkit as tk
from ckan.lib.base import model
from ckan.lib.cli import CkanCommand

DAYS_TO_SUBTRACT_DEFAULT = 30
DB_BLOCK_SIZE = 10000

class CleanUpDb(CkanCommand):
    '''Clean up the CKAN database, e.g. dataset activities.

    Usage:

      activities [--older-than-days={days}]
        - Deletes all activities older than the given {days}. Default is 30 days.

        '''

    summary = __doc__.split('\n')[0]
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
            self.delete_activities()
        elif cmd == 'revisions':
            self.delete_revisions()
        else:
            print 'Command %s not recognized' % cmd

    def delete_revisions(self):
        ''' Start delete revisions '''
        print 'INFO start deleting revisions...'
        try:
            # Query all resource revisions to delete
            query_rr = model.Session.query(model.ResourceRevision)\
                .filter_by(state=model.State.DELETED)

            rows_to_delete_count = query_rr.count()
            print "DEBUG resource revisions to delete count: %s " % rows_to_delete_count
            rows_deleted = query_rr.delete()
            print "INFO finished deleting resource revisions. count: %s " % rows_deleted

            # Query all package extra revisions to delete
            query_ex = model.Session.query(model.PackageExtraRevision)\
                .filter_by(state=model.State.DELETED)

            rows_to_delete_count = query_ex.count()
            print "DEBUG package extra revisions to delete count: %s " % rows_to_delete_count
            rows_deleted = query_ex.delete()
            print "INFO finished deleting package extra revisions. count: %s " % rows_deleted

            model.repo.commit()
        except Exception as error:
            model.Session.rollback()
            print 'ERROR while deleting revisions! Details: %s' % str(error)

    def delete_activities(self):
        '''Deletes all dataset activities.'''

        date_limit = datetime.today() - timedelta(days=self.days_to_subtract)
        date_limit_string = date_limit.strftime("%Y-%m-%d")
        print 'INFO Delete all activities older than %s.' % date_limit_string

        success_count = 0
        starttime = time.time()
        print 'INFO [%s]: START deleting activities...' % self.format_date_string(starttime)
        try:
            # Query all activities to delete
            query = model.Session.query(model.Activity)\
                .join(model.User, model.User.id == model.Activity.user_id)\
                .filter(model.User.name == 'harvest')\
                .filter(model.Activity.timestamp < date_limit_string)

            rows_to_delete_count = query.count()
            print "DEBUG Activity deleting count: %s " % rows_to_delete_count

            # Delete activities
            for table_row in self.page_query(query):
                table_row.delete()
                success_count += 1
                self.process_result_state(success_count, rows_to_delete_count, 'Activity')

            model.Session.flush()
            # delete obsolete activity_details
            activity_details_deleted_count = model.Session.query(model.ActivityDetail)\
                .filter(model.ActivityDetail.activity_id.is_(None))\
                .delete()

            print "DEBUG Activity_details deleted count: %d " % activity_details_deleted_count
            success_count += activity_details_deleted_count

            model.repo.commit()
        except Exception as error:
            model.Session.rollback()
            print 'ERROR while deleting activities! Details: %s' % str(error)

        endtime = time.time()
        print '============================================================='
        print "INFO [%s]: Totally deleted rows: %d. Total time: %s." % \
                 (self.format_date_string(endtime), success_count, str(endtime - starttime))

    def check_option_days(self):
        ''' Check value for option days '''
        if self.options.days:
            try:
                self.days_to_subtract = int(self.options.days)
            except ValueError:
                print 'ERROR Value \'%s\' for days is not a number!' % str(self.options.days)
                sys.exit(1)
        else:
            print 'INFO Using default of %d days.' % self.days_to_subtract

    @classmethod
    def page_query(cls, query):
        '''Iterates over the given query in blocks.'''

        while True:
            result = False
            for elem in query.limit(DB_BLOCK_SIZE):
                result = True
                yield elem
            if not result:
                break

    @classmethod
    def process_result_state(cls, success_count, rows_to_delete_count, object_type):
        '''Executes a commit at checkpoints and at the and of all results. Raises RuntimeError .
           if the maximum number of rows to delete was exceeded.
        '''

        if (success_count == rows_to_delete_count) or (success_count % DB_BLOCK_SIZE == 0):
            model.repo.commit()
            print 'DEBUG Deleted %d of %d objects of type %s' % \
                (success_count, rows_to_delete_count, object_type)
        if success_count > rows_to_delete_count:
            error_msg = '''
                Something went wrong! The maximum of %d rows to delete was exceeded! 
                Current value is %d.''' % (rows_to_delete_count, success_count)
            raise RuntimeError(error_msg)

    @classmethod
    def format_date_string(cls, time_in_seconds):
        '''Converts a time stamp to a string according to a format specification.'''

        struct_time = time.localtime(time_in_seconds)
        return time.strftime("%Y-%m-%d %H:%M:%S", struct_time)
