#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Check if dataset links are available
'''
import json
import logging
import six

from ckan import model
from ckan.logic import get_action
from ckan.plugins.toolkit import CkanCommand
from ckan.plugins import toolkit as tk
from ckanext.govdatade.commands import command_util
from ckanext.govdatade import util
from ckanext.govdatade.validators import link_checker


class LinkChecker(CkanCommand):
    # pylint: disable=too-few-public-methods

    '''Checks the availability of the dataset's URLs

    report                         Creates a report for all datasets
    specific <dataset-name>        Checks links for a specific dataset
    remote <host-name>             Checks links for datasets of a given remote host
    '''

    summary = __doc__.split('\n', maxsplit=1)[0]
    usage = __doc__

    def __init__(self, name):
        super(LinkChecker, self).__init__(name)
        self.logger = logging.getLogger('ckanext.govdatade.reports.commands.linkchecker')

    def command(self):
        '''
        Entry method for the command
        '''
        super(LinkChecker, self)._load_config()
        active_datasets = set()

        if len(self.args) == 0:

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
                    self.logger.info('LinkChecker: Error while processing dataset %s. Details: %s',
                                     six.text_type(dataset['id']), six.text_type(ex))

            command_util.delete_deprecated_datasets(active_datasets)
            general = {'num_datasets': num_datasets}
            validator.redis_client.set('general', json.dumps(general))
            self.logger.info('Generated link check report data.')
        if len(self.args) > 0:
            subcommand = self.args[0]
            if subcommand == 'remote':
                command_util.check_remote_host(self.args[1])
            elif len(self.args) == 2 and self.args[0] == 'specific':
                dataset_name = self.args[1]

                context = {'model':       model,
                           'session':     model.Session,
                           'ignore_auth': True}

                package_show = get_action('package_show')
                validator = link_checker.LinkChecker(tk.config)

                dataset = package_show(context, {'id': dataset_name})

                self.logger.info('Processing dataset %s', dataset)
                util.normalize_action_dataset(dataset)
                validator.process_record(dataset)
