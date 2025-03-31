# -*- coding: utf-8 -*-
'''
Click commands.
'''
import json
import os

import click
from ckan import model
from ckan.logic import get_action
from ckan.plugins import toolkit as tk
from ckanext.govdatade import util
from ckanext.govdatade.commands import command_util
from ckanext.govdatade.validators import link_checker

DAYS_TO_SUBTRACT_DEFAULT = 30

def get_commands():
    ''' Get available commands '''
    return [cleanupdb,
            delete,
            linkchecker,
            purge,
            report]

@click.command('cleanupdb')
@click.argument('delete_activities', required=False)
@click.option(
    '--older-than-days',
    default=False,
    help='Objects older than the defined days are deleted. '
    'The default is %d days.' % DAYS_TO_SUBTRACT_DEFAULT
)
def cleanupdb(delete_activities, older_than_days):
    '''Clean up the CKAN database, e.g. dataset activities.

    Usage:

      activities [--older-than-days={days}]
        - Deletes all activities older than the given {days}. Default is 30 days.

    '''

    if delete_activities:
        days_to_subtract = _check_option_days(older_than_days)
        command_util.delete_activities(days_to_subtract)
    else:
        tk.error_shout('Command not recognized')
        raise click.Abort()
    click.secho('Command successfully executed', fg='green')


@click.command('delete')
@click.argument('args', nargs=-1)
@click.option(
    '--dry-run',
    default='True',
    help='With dry-run True the deletion will be not executed. '
    'The default is True.'
)
def delete(args, dry_run):
    '''Deletes objects in the CKAN database, e.g. datasets.

    Usage:

        datasets {filter-query-params} [--dry-run]
        - Deletes all datasets matching the given {filter-query-params}.

    '''

    admin_user = None
    package_search_filter_params = None

    if len(args) < 2:
        tk.error_shout('Too few arguments')
        raise click.Abort()
    cmd = args[0]

    # Getting/Setting default site user
    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    admin_user = tk.get_action('get_site_user')(context, {})

    if cmd == 'datasets':
        package_search_filter_params = command_util.check_package_search_params(args[1:])
        dry_run_result = _check_options(dry_run)
        command_util.delete_datasets(dry_run_result, package_search_filter_params, admin_user)
    else:
        tk.error_shout(u'Command {} not recognized'.format(cmd))
        raise click.Abort()
    click.secho('Command successfully executed', fg='green')


@click.command('linkchecker')
@click.argument('args', nargs=-1)
def linkchecker(args):
    '''Checks the availability of the dataset's URLs

    report                         Creates a report for all datasets
    specific <dataset-name>        Checks links for a specific dataset
    remote <host-name>             Checks links for datasets of a given remote host
    '''

    active_datasets = set()
    if len(args) == 0:

        context = {'model': model,
                   'session': model.Session,
                   'ignore_auth': True}

        validator = link_checker.LinkChecker(tk.config)

        num_datasets = 0
        for dummy_index, dataset in enumerate(util.iterate_local_datasets(context)):
            util.normalize_action_dataset(dataset)
            try:
                validator.process_record(dataset)
                num_datasets += 1
                active_datasets.add(dataset['id'])
            except Exception as ex:
                click.echo(u'LinkChecker: Error while processing dataset {}. Details: {}'.format(
                    str(dataset['id']), str(ex)))

        command_util.delete_deprecated_datasets(active_datasets)
        general = {'num_datasets': num_datasets}
        validator.redis_client.set('general', json.dumps(general))
        click.secho('Generated link check report data.', fg='green')

    if len(args) > 0:
        subcommand = args[0]
        if subcommand == 'remote':
            command_util.check_remote_host(args[1])
        elif len(args) == 2 and args[0] == 'specific':
            dataset_name = args[1]

            context = {'model':       model,
                       'session':     model.Session,
                       'ignore_auth': True}

            package_show = get_action('package_show')
            validator = link_checker.LinkChecker(tk.config)

            dataset = package_show(context, {'id': dataset_name})

            click.echo(u'Processing dataset {}'.format(dataset))
            util.normalize_action_dataset(dataset)
            validator.process_record(dataset)


@click.command('purge')
@click.argument('args', nargs=-1)
def purge(args):
    '''Purges datasets.'''

    if len(args) == 0:
        tk.error_shout('Too few arguments')
        raise click.Abort()

    cmd = args[0]

    # Getting/Setting default site user
    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    admin_user = tk.get_action('get_site_user')(context, {})

    # Getting/Setting path to log file for auto deleted/purged packages
    path_to_logfile = tk.config.get('ckanext.govdata.delete_deprecated_packages.logfile')
    if path_to_logfile is not None:
        click.echo("INFO: Logging to file %s." % path_to_logfile)
    else:
        click.echo("WARN: Could not get log file path for purged datasets from configuration!")

    if cmd == 'deleted':
        command_util.purge_deleted_datasets(path_to_logfile, admin_user)
    else:
        tk.error_shout(u'Command {} not recognized'.format(cmd))
        raise click.Abort()
    click.secho('Command successfully executed', fg='green')


@click.command('report')
def report():
    '''Generates metadata quality report based on Redis data.'''

    command_util.generate_report()
    report_path = os.path.normpath(
        tk.config.get('ckanext.govdata.validators.report.dir')
    )
    info_message = "Wrote validation report to '%s'." % report_path
    click.secho(info_message, fg='green')


def _check_options(dry_run_option):
    ''' Check if options are valid '''
    dry_run = True
    if dry_run_option:
        if str(dry_run_option).lower() not in ('yes', 'true', 'no', 'false'):
            tk.error_shout(u'Value \'%s\' for dry-run is not a boolean!' \
                       % str(dry_run_option))
            raise click.Abort()
        elif str(dry_run_option).lower() in ('no', 'false'):
            dry_run = False
    return dry_run

def _check_option_days(older_than_days):
    ''' Check value for option days '''

    days_to_subtract = DAYS_TO_SUBTRACT_DEFAULT

    if older_than_days:
        try:
            days_to_subtract = int(older_than_days)
        except ValueError:
            tk.error_shout(u'ERROR Value \'{}\' for days is not a number!'.format(
                str(older_than_days)))
            raise click.Abort()
    else:
        click.echo('INFO Using default of {} days.'.format(days_to_subtract))

    return days_to_subtract
