#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for adding a set of groups to the CKAN instance.
'''
import json
import logging
import urllib2
import ckanapi

from ckan import model
from ckan.model import Session
from ckan.lib.cli import CkanCommand
from ckan.logic.action.create import group_create
from ckan.logic.action.delete import group_purge
from ckan.logic import NotFound

from ckanext.govdatade.config import config

LOGGER = logging.getLogger(__name__)


class GroupAdder(CkanCommand):

    '''Adds a default set of groups to the current CKAN instance.'''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def __init__(self, name):
        super(GroupAdder, self).__init__(name)
        self.context = self.create_context()

    def create_context(self):
        return {
            'model': model,
            'session': Session,
            'user': u'harvest',
            'validate': False
        }

    def _create_and_purge_group(self, group_dict):
        '''Worker method for the actual group addition.
        For unpurged groups a purge happens prior.'''

        context = self.context
        context['allow_partial_update'] = True

        try:
            group_purge(context, group_dict)
        except NotFound:
            not_found_message = 'Group {group_name} not found, nothing to purge.'.format(
                group_name=group_dict['name']
            )
            print(not_found_message)
        finally:
            group_create(context, group_dict)

    def command(self):
        '''Worker command doing the actual group additions.'''

        super(GroupAdder, self)._load_config()
        ckan_api_client = ckanapi.LocalCKAN()

        present_groups_dict = ckan_api_client.action.group_list()

        present_groups_keys = []
        if len(present_groups_dict) > 0:
            for group_key in present_groups_dict:
                present_groups_keys.append(group_key)

        groups_file = config.get('ckanext.govdata.urls.groups')
        govdata_groups = json.loads(urllib2.urlopen(groups_file).read())

        for group_key in govdata_groups:
            if group_key not in present_groups_keys:
                add_message = 'Adding group {group_key}.'.format(
                    group_key=group_key
                )
                print(add_message)

                group_dict = {
                    'name': group_key,
                    'id': group_key,
                    'title': govdata_groups[group_key]
                }

                self._create_and_purge_group(
                    group_dict
                )
            else:
                skip_message = 'Skipping creation of group '
                skip_message = skip_message + "{group_key}, as it's already present."
                print(skip_message.format(group_key=group_key))
