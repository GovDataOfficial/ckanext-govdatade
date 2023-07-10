'''
Commands util methods
'''
import csv
import io
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

import six
from jinja2 import Environment, FileSystemLoader

from ckan import model
from ckan.plugins import toolkit as tk
from ckanext.activity.model import Activity
from ckanext.govdatade import util
from ckanext.govdatade.validators import link_checker

DB_BLOCK_SIZE = 10000
ROWS = 100

LOGGER = logging.getLogger(__name__)

#######################################
###         delete utils            ###
#######################################

def check_package_search_params(psfp_args):
    ''' Check package search parameters '''
    if psfp_args:
        try:
            # Basic validation of (multiple) params
            for psfp_arg in psfp_args:
                splitted = psfp_arg.split(':', 1)
                if len(splitted) != 2:
                    raise ValueError
            package_search_filter_params = ' '.join(six.text_type(psfp_arg) for psfp_arg in psfp_args)
            return package_search_filter_params
        except ValueError:
            print('ERROR: One or more package search filter params are not of the required' \
                ' form \'fieldname:value\' !')
            sys.exit(1)
    else:
        print('ERROR: Missing required parameter with package search filter params!')
        sys.exit(1)

def delete_datasets(dry_run, package_search_filter_params, admin_user):
    '''Deletes all datasets matching package search filter query.'''
    starttime = time.time()
    package_ids_to_delete = _gather_dataset_ids(package_search_filter_params)
    endtime = time.time()
    print("INFO: %s datasets found for deletion. Total time: %s." % \
            (len(package_ids_to_delete), six.text_type(endtime - starttime)))

    if dry_run:
        print("INFO: DRY-RUN: The dataset deletion is disabled.")
    elif len(package_ids_to_delete) > 0:
        success_count = error_count = 0
        starttime = time.time()
        for package_id in package_ids_to_delete:
            try:
                # Deleting package
                checkpoint_start = time.time()
                _delete(package_id, admin_user)
                checkpoint_end = time.time()
                print("DEBUG: Deleted dataset with id %s. Time taken for deletion: %s." % \
                            (package_id, six.text_type(checkpoint_end - checkpoint_start)))
                success_count += 1
            except Exception as error:
                print('ERROR: While deleting dataset with id %s. Details: %s' % \
                    (package_id, six.text_type(error)))
                error_count += 1

        endtime = time.time()
        print('=============================================================')
        print("INFO: %s datasets successfully deleted. %s datasets couldn't deleted. Total time: %s." % \
                (success_count, error_count, six.text_type(endtime - starttime)))

def _delete(dataset_ref, admin_user):
    '''Deletes the dataset with the given ID.'''
    context = {'user': admin_user['name']}
    tk.get_action('package_delete')(context, {'id': dataset_ref})

def _gather_dataset_ids(package_search_filter_params):
    '''Collects all dataset ids matching the filter params.'''
    package_ids_found = []
    offset = 0
    count = 0
    package_search = tk.get_action('package_search')

    while offset <= count:
        query_object = {
            "fq": package_search_filter_params,
            "rows": ROWS,
            "start": offset
            }
        result = package_search({}, query_object)
        datasets = result["results"]
        count += len(datasets)
        print("DEBUG: offset: %s, count: %s" % (six.text_type(offset), six.text_type(count)))
        offset += ROWS

        if count != 0:
            for dataset in datasets:
                package_ids_found.append(dataset['id'])

    return set(package_ids_found)

#######################################
###         purge utils             ###
#######################################

def purge_deleted_datasets(path_to_logfile, admin_user):
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
            _purge(package_id, admin_user)
            checkpoint_end = time.time()
            # Log to file and command line
            _log_deleted_packages_in_file(package_object, checkpoint_end, path_to_logfile)
            print("DEBUG: Purged dataset with id %s and name %s. Time taken for purging: %s." % \
                        (package_id, package_object.name, six.text_type(checkpoint_end-checkpoint_start)))
            success_count += 1
        except Exception as error:
            print('ERROR: While purging dataset with id %s. Details: %s' % (package_id, six.text_type(error)))
            error_count += 1

    endtime = time.time()
    print('=============================================================')
    print("INFO: %s datasets successfully purged. %s datasets couldn't purged. Total time: %s." % \
                (success_count, error_count, six.text_type(endtime-starttime)))

def _purge(dataset_ref, admin_user):
    '''Purges the dataset with the given ID.'''

    context = {'user': admin_user['name']}
    tk.get_action('dataset_purge')(context, {'id': dataset_ref})

def _log_deleted_packages_in_file(package_object, time_in_seconds, path_to_logfile):
    '''Write the information about the deleted packages in a file.'''

    if path_to_logfile is not None:
        try:
            with io.open(path_to_logfile, 'a') as logfile:
                line = ([package_object.id, package_object.name, 'purged',
                         _format_date_string(time_in_seconds)])
                csv.writer(logfile).writerow(line)
        except Exception as exception:
            LOGGER.warning(
                'Could not write in automated deletion log file at %s: %s',
                path_to_logfile, exception
            )

def _format_date_string(time_in_seconds):
    '''Converts a time stamp to a string according to a format specification.'''

    struct_time = time.localtime(time_in_seconds)
    return time.strftime("%Y-%m-%d %H:%M", struct_time)


#######################################
###         linkchecker utils       ###
#######################################

def delete_deprecated_datasets(dataset_ids):
    '''
    Deletes deprecated datasets from Redis
    '''
    validator = link_checker.LinkChecker(tk.config)
    redis_client = validator.redis_client
    redis_ids = redis_client.keys()
    for redis_id in redis_ids:
        if redis_id not in dataset_ids:
            record = redis_client.get(redis_id)
            if (record is not None) and (redis_id != 'general'):
                redis_client.delete(redis_id)
                LOGGER.info('Deleted deprecated broken links information for dataset %s from Redis',
                            six.text_type(redis_id))

def check_remote_host(endpoint):
    '''
    check if remote host is available
    '''
    checker = link_checker.LinkChecker(tk.config)

    num_urls = 0
    num_success = 0
    for i, dataset in enumerate(util.iterate_remote_datasets(endpoint)):
        process_info = 'Process {id}'.format(id=i)
        LOGGER.info(process_info)

        for resource in dataset['resources']:
            num_urls += 1
            url = resource['url'].encode('utf-8')
            response_code = checker.validate(url)

            if checker.is_available(response_code):
                num_success += 1

#######################################
###         report utils            ###
#######################################

def generate_report():
    '''
    Generates the report
    '''
    data = defaultdict(defaultdict)

    util.generate_general_data(data)
    util.generate_link_checker_data(data)

    util.copy_report_asset_files()
    util.copy_report_vendor_files()

    templates = ['index.html', 'linkchecker.html']
    templates = [name + '.jinja2' for name in templates]

    for template_file in templates:
        rendered_template = _render_template(template_file, data)
        _write_validation_result(rendered_template, template_file)

def _render_template(template_file, data):
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
    environment.globals.update(amend_portal=util.amend_portal)
    environment.globals.update(six=six)

    data['ckan_api_url'] = tk.config.get('ckan.api.url.portal')
    data['govdata_detail_url'] = tk.config.get(
        'ckanext.govdata.validators.report.detail.url'
    )

    template = environment.get_template(template_file)
    return template.render(data)

def _write_validation_result(rendered_template, template_file):
    '''
    Writes the report to the filesystem
    '''
    target_template = template_file.rstrip('.jinja2')

    target_dir = tk.config.get('ckanext.govdata.validators.report.dir')

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    target_file = os.path.join(target_dir, target_template)
    target_file = os.path.abspath(target_file)

    file_handler = io.open(target_file, 'w')
    file_handler.write(rendered_template)
    file_handler.close()

#######################################
###         cleanupdb utils         ###
#######################################

def delete_activities(days_to_subtract):
    '''Deletes all dataset activities.'''

    date_limit = datetime.today() - timedelta(days=days_to_subtract)
    date_limit_string = date_limit.strftime("%Y-%m-%d")
    print('INFO Delete all activities older than %s.' % date_limit_string)

    success_count = 0
    starttime = time.time()
    print('INFO [%s]: START deleting activities...' % _format_date_string(starttime))
    try:
        # Query all activities to delete
        query = model.Session.query(Activity)\
            .join(model.User, model.User.id == Activity.user_id)\
            .filter(model.User.name == 'harvest')\
            .filter(Activity.timestamp < date_limit_string)

        rows_to_delete_count = query.count()
        print("DEBUG Activity deleting count: %s " % rows_to_delete_count)

        # Delete activities
        for table_row in _page_query(query):
            table_row.delete()
            success_count += 1
            _process_result_state(success_count, rows_to_delete_count, 'Activity')

        model.Session.flush()
        model.repo.commit()
    except Exception as error:
        model.Session.rollback()
        print('ERROR while deleting activities! Details: %s' % six.text_type(error))

    endtime = time.time()
    print('=============================================================')
    print("INFO [%s]: Totally deleted rows: %d. Total time: %s." % \
                (_format_date_string(endtime), success_count, six.text_type(endtime - starttime)))


def _process_result_state(success_count, rows_to_delete_count, object_type):
    '''Executes a commit at checkpoints and at the and of all results. Raises RuntimeError,
       if the maximum number of rows to delete was exceeded.
    '''

    if (success_count == rows_to_delete_count) or (success_count % DB_BLOCK_SIZE == 0):
        model.repo.commit()
        print('DEBUG Deleted %d of %d objects of type %s' % \
            (success_count, rows_to_delete_count, object_type))
    if success_count > rows_to_delete_count:
        error_msg = '''
            Something went wrong! The maximum of %d rows to delete was exceeded!
            Current value is %d.''' % (rows_to_delete_count, success_count)
        raise RuntimeError(error_msg)


def _page_query(query):
    '''Iterates over the given query in blocks.'''

    while True:
        result = False
        for elem in query.limit(DB_BLOCK_SIZE):
            result = True
            yield elem
        if not result:
            break
