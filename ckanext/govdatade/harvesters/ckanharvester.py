#!/usr/bin/python
# -*- coding: utf8 -*-
'''
Module for harvesting CKAN instances into GovData.
'''
import datetime
import json
import logging
import urllib2
import re
import ckanapi

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError
from ckanext.harvest.interfaces import IHarvester
from ckanext.govdatade.harvesters.translator import translate_groups
from ckanext.govdatade.config import config
from ckanext.govdatade.util import iterate_local_datasets
from simplejson.scanner import JSONDecodeError

from ckan import model
from ckan.logic import get_action
from ckan.logic.schema import default_create_package_schema
from ckan.model import Session
from ckan.plugins.core import implements

log = logging.getLogger(__name__)


def assert_author_fields(package_dict, author_alternative, author_email_alternative):
    '''Assert author field presence.'''

    if 'author' not in package_dict or not package_dict['author']:
        package_dict['author'] = author_alternative

    if 'author_email' not in package_dict or not package_dict['author_email']:
        package_dict['author_email'] = author_email_alternative

    if not package_dict['author']:
        value_error = 'There is no author for package {id}'.format(
            id=package_dict['id']
        )
        raise ValueError(value_error)


class GroupCKANHarvester(CKANHarvester):

    '''
    An extended CKAN harvester that also imports remote groups,
    for that api version 1 is enforced.
    '''

    api_version = 1
    '''Enforce API version 1 for enabling group import'''

    def __init__(self):
        schema_url = config.get('ckanext.govdata.urls.schema')
        groups_url = config.get('ckanext.govdata.urls.groups')

        if schema_url is None:
            error_message = 'Missing configuration value for {config_key}'.format(
                config_key='ckanext.govdata.urls.schema'
            )
            raise ValueError(error_message)
        if groups_url is None:
            error_message = 'Missing configuration value for {config_key}'.format(
                config_key='ckanext.govdata.urls.groups'
            )
            raise ValueError(error_message)

        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())

    def _set_config(self, config_str):
        '''Enforce API version 1 for enabling group import.'''

        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}

        self.api_version = 1
        self.config['api_version'] = 1
        self.config['force_all'] = True
        self.config['remote_groups'] = 'only_local'
        self.config['user'] = 'harvest'

        config_settings_log_message = 'config_settings: {config_settings}'.format(
            config_settings=json.dumps(self.config)
        )

        log.debug(config_settings_log_message)

    @classmethod
    def cleanse_tags(cls, tags):
        '''Cleans a set of given tags.'''

        log.debug(
            'Cleansing tags: %s.',
            json.dumps(tags)
        )

        if isinstance(tags, list):
            cleansed_tags = []

            for tag in tags:
                cleansed_tags.append(cls.cleanse_special_characters(tag))

            return cleansed_tags

        return cls.cleanse_special_characters(tags)


    @classmethod
    def cleanse_special_characters(cls, tag):
        '''Cleans a given tag of special characters.'''
        tag = tag.lower().strip()
        return re.sub(u'[^a-zA-ZÄÖÜäöü0-9 \-_\.]', '', tag).replace(' ', '-')


class GovDataHarvester(GroupCKANHarvester):

    '''The base harvester for GovData.de performing remote synchronization.'''

    implements(IHarvester)

    @classmethod
    def build_context(cls):
        return {
            'model': model,
            'session': Session,
            'user': u'harvest',
            'schema': default_create_package_schema(),
            'validate': False,
            'api_version': 1
        }

    @classmethod
    def portal_relevant(cls, portal):
        def condition_check(dataset):
            for extra in dataset['extras']:
                if extra['key'] == 'metadata_original_portal':
                    value = extra['value']
                    value = value.lstrip('"').rstrip('"')
                    return value == portal

            return False

        return condition_check

    def delete_deprecated_datasets(self, context, remote_dataset_names):
        '''
        Deletes deprecated datasets
        '''
        package_update = get_action('package_update')

        local_datasets = iterate_local_datasets(context)
        filtered = filter(self.portal_relevant(self.portal), local_datasets)
        local_dataset_names = map(lambda dataset: dataset['name'], filtered)

        deprecated = set(local_dataset_names) - set(remote_dataset_names)
        log.info('Found %s deprecated datasets.', len(deprecated))

        for local_dataset in filtered:
            if local_dataset['name'] in deprecated:
                local_dataset['state'] = 'deleted'
                local_dataset['tags'].append({'name': 'deprecated'})
                package_update(context, local_dataset)

    @classmethod
    def compare_metadata_modified(cls, remote_md_modified, local_md_modified):
        '''
        Compares the modified datetimes of the metadata
        '''
        dt_format = '%Y-%m-%dT%H:%M:%S.%f'
        remote_dt = datetime.datetime.strptime(remote_md_modified, dt_format)
        local_dt = datetime.datetime.strptime(local_md_modified, dt_format)
        if remote_dt < local_dt:
            log.debug('remote dataset precedes local dataset -> skipping.')
            return False
        elif remote_dt == local_dt:
            log.debug('remote dataset equals local dataset -> skipping.')
            return False
        else:
            log.debug('local dataset precedes remote dataset -> importing.')
            # TODO do I have to delete other dataset?
            return True

    def verify_transformer(self, remote_dataset):
        '''Based on metadata_transformer, this method checks, if a dataset should be imported.'''
        registry = ckanapi.RemoteCKAN(
            config.get('ckanext.govdata.harvester.ckan.api.base.url')
        )
        remote_dataset = json.loads(remote_dataset)
        remote_dataset_extras = remote_dataset['extras']
        if 'metadata_original_id' in remote_dataset_extras:
            orig_id = remote_dataset_extras['metadata_original_id']
            try:
                local_search_result = registry.action.package_search(
                    q='metadata_original_id:"' + orig_id + '"')
                if local_search_result['count'] == 0:
                    log.debug(
                        'Did not find this original id. Import accepted.')
                    return True
                if local_search_result['count'] == 1:
                    log.debug('Found duplicate entry')
                    local_dataset = local_search_result['results'][0]
                    local_dataset_extras = local_dataset['extras']
                    if 'metadata_transformer' in [entry['key'] for entry in local_dataset_extras]:
                        log.debug('Found metadata_transformer')
                        local_transformer = None
                        local_portal = None
                        for entry in local_dataset_extras:
                            if entry['key'] == 'metadata_transformer':
                                value = entry['value']
                                local_transformer = value.lstrip(
                                    '"').rstrip('"')
                                log.debug('Found local metadata transformer')
                            if entry['key'] == 'metadata_original_portal':
                                tmp_value = entry['value']
                                local_portal = tmp_value.lstrip(
                                    '"').rstrip('"')
                        if 'metadata_transformer' in remote_dataset_extras:
                            remote_transformer = remote_dataset_extras[
                                'metadata_transformer']
                            if remote_transformer == local_transformer or remote_transformer == 'harvester':
                                # TODO this is temporary for gdi-de
                                if local_portal == 'http://www.statistik.sachsen.de/':
                                    log.debug('Found sachsen, accept import.')
                                    return True
                                log.debug(
                                    'Remote metadata transformer equals local transformer -> check metadata_modified')
                                # TODO check md_modified
                                if 'metadata_modified' in remote_dataset:
                                    return self.compare_metadata_modified(remote_dataset['metadata_modified'],
                                                                          local_dataset['metadata_modified'])
                                else:
                                    log.debug(
                                        'Remote metadata transformer equals local transformer, but remote dataset does not contain metadata_modified -> skipping')
                                    return False
                            elif remote_transformer == 'author' and local_transformer == 'harvester':
                                log.debug(
                                    'Remote metadata transformer equals author and local equals harvester -> importing.')
                                return True
                            else:
                                log.debug(
                                    'unknown value for remote metadata_transformer -> skipping.')
                                return False
                        else:
                            log.debug(
                                'remote does not contain metadata_transformer, fallback on metadata_modified')
                            if 'metadata_modified' in remote_dataset:
                                return self.compare_metadata_modified(remote_dataset['metadata_modified'],
                                                                      local_dataset['metadata_modified'])
                            else:
                                log.debug(
                                    'Remote metadata transformer equals local transformer, but remote dataset does not contain metadata_modified -> skipping')
                                return False
                    else:
                        if 'metadata_modified' in remote_dataset:
                            return self.compare_metadata_modified(remote_dataset['metadata_modified'],
                                                                  local_dataset['metadata_modified'])
                        else:
                            log.debug(
                                'Found duplicate entry but remote dataset does not contain metadata_modified -> skipping.')
                            return False
            except Exception as exception:
                log.error(exception)
        else:
            log.debug('no metadata_original_id. Importing accepted.')
            return True

    def gather_stage(self, harvest_job):
        '''Retrieve local datasets for synchronization.'''
        try:
            self._set_config(harvest_job.source.config)
            content = self._get_content(harvest_job.source.url)

            base_url = harvest_job.source.url.rstrip('/')
            base_rest_url = base_url + self._get_rest_api_offset()
            url = base_rest_url + '/package'

            content = self._get_content(url)
        except JSONDecodeError as err:
            self._save_gather_error(err.message, harvest_job)
            return None
        except ContentFetchError as err:
            self._save_gather_error(err.message, harvest_job)
            return None
        except Exception as err:
            error = 'Unable to get content for URL: %s: %s' % (url, str(err))
            self._save_gather_error(error, harvest_job)
            return None

        return super(GovDataHarvester, self).gather_stage(harvest_job)

    def import_stage(self, harvest_object):
        to_import = self.verify_transformer(harvest_object.content)
        if to_import:
            super(GovDataHarvester, self).import_stage(harvest_object)


class RostockCKANHarvester(GovDataHarvester):

    '''A CKAN harvester for Rostock solving data compatibility problems.'''

    implements(IHarvester)

    def __init__(self, name='rostock_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'rostock',
            'title': 'Datenportal Rostock',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        package['extras']['metadata_original_portal'] = self.portal
        package['name'] = package['name'] + '-hro'
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        try:
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            log.error('Rostock: ' + str(error))
            return

        harvest_object.content = json.dumps(package)
        super(RostockCKANHarvester, self).import_stage(harvest_object)


class HamburgCKANHarvester(GroupCKANHarvester):

    '''A CKAN harvester for Hamburg solving data compatibility problems.'''

    implements(IHarvester)

    def __init__(self, name='hamburg_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'hamburg',
            'title': 'Datenportal Hamburg',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        extras = package['extras']

        is_latest_version = extras.get('latestVersion', None)

        if is_latest_version == 'true':
            log.debug(
                'received latestVersion == true. Continue with this dataset')

            remote_metadata_original_id = extras.get(
                'metadata_original_id', None)
            registry = ckanapi.RemoteCKAN(
                config.get('ckanext.govdata.harvester.ckan.api.base.url')
            )
            local_search_result = registry.action.package_search(
                q='metadata_original_id:"' + remote_metadata_original_id + '"'
            )

            if local_search_result['count'] == 0:
                log.debug(
                    'Did not find this metadata original id. Import accepted.')
            elif local_search_result['count'] == 1:
                log.debug(
                    'Found local dataset for particular metadata_original_id')
                local_dataset_from_action_api = local_search_result[
                    'results'][0]

                # copy name and id from local dataset to remote dataset
                log.debug('Copy id and name to remote dataset')
                log.debug(package['id'])
                log.debug(package['name'])
                package['id'] = local_dataset_from_action_api['id']
                package['name'] = local_dataset_from_action_api['name']
                log.debug(package['id'])
                log.debug(package['name'])
            else:

                log_message = 'Found more than one local dataset for particular '
                log_message = log_message + 'metadata_original_id. Offending '
                log_message = log_message + 'metadata_original_id is:'
                log.debug(log_message)
                log.debug(remote_metadata_original_id)
        elif is_latest_version == 'false':
            # do not import or update this particular remote dataset
            log.debug('received latestVersion == false. Skip this dataset')
            return False

        # check if import is desired
        if package['type'] == 'document':
            # check if tag 'govdata' exists
            if not [tag for tag in package['tags'] if tag.lower() == 'govdata']:
                log.debug('Found invalid package')
                return False
            package['type'] = 'dokument'
        # check if import is desired
        elif package['type'] == 'dokument':
            # check if tag 'govdata' exists
            if not [tag for tag in package['tags'] if tag.lower() == 'govdata']:
                log.debug('Found invalid package')
                return False
        elif package['type'] == 'dataset':
            package['type'] = 'datensatz'

        # fix groups
        log.debug('Before: ')
        log.debug(package['groups'])
        package['groups'] = translate_groups(package['groups'], 'hamburg')
        log.debug('After: ')
        log.debug(package['groups'])

        if not extras.get('metadata_original_portal'):
            extras['metadata_original_portal'] = self.portal

        assert_author_fields(
            package,
            package.get('maintainer'),
            package.get('maintainer_email')
        )

        return True

    def fetch_stage(self, harvest_object):
        log.debug('In CKANHarvester fetch_stage')

        self._set_config(harvest_object.job.source.config)

        # Get source URL
        url = harvest_object.source.url.rstrip('/')
        url = url + self._get_rest_api_offset() + '/package/' + \
            harvest_object.guid

        # Get contents
        try:
            content = self._get_content(url)
        except Exception, error:
            self._save_object_error('Unable to get content for package: %s: %r' %
                                    (url, error), harvest_object)
            import time
            log.debug('Going to sleep for 45s')
            time.sleep(45)
            log.debug('Wake up from sleep')
            return None

        # Save the fetched contents in the harvest object
        harvest_object.content = content
        harvest_object.save()
        return True

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        try:
            valid = self.amend_package(package)
            if not valid:
                return  # drop package
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            log.error('Hamburg: ' + str(error))
            return
        harvest_object.content = json.dumps(package)
        super(HamburgCKANHarvester, self).import_stage(harvest_object)


class BerlinCKANHarvester(GovDataHarvester):

    '''A CKAN harvester for Berlin solving data compatibility problems.'''

    implements(IHarvester)

    def __init__(self, name='berlin_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'berlin',
            'title': 'Datenportal Berlin',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        extras = package['extras']

        if 'license_id' not in package or package['license_id'] == '':
            package['license_id'] = 'notspecified'

        # if sector is not set, set it to 'oeffentlich' (default)
        if not extras.get('sector'):
            extras['sector'] = 'oeffentlich'

        if package['extras']['sector'] != 'oeffentlich':
            return False

        # avoid ValidationError when extra dict
        # key 'type' is also used by the internal CKAN validation,
        # see GOVDATA-651
        if 'type' in extras:
            package['extras'].pop('type', None)

        valid_types = ['datensatz', 'dokument', 'app']
        if not package.get('type') or package['type'] not in valid_types:
            package['type'] = 'datensatz'

        package['groups'] = translate_groups(package['groups'], 'berlin')

        if not extras.get('metadata_original_portal'):
            extras['metadata_original_portal'] = self.portal
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()
        return True

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        valid = self.amend_package(package)

        if not valid:
            return  # drop package

        harvest_object.content = json.dumps(package)
        super(BerlinCKANHarvester, self).import_stage(harvest_object)


class RlpCKANHarvester(GovDataHarvester):

    '''A CKAN harvester for Rhineland-Palatinate solving data compatibility problems.'''

    implements(IHarvester)

    def info(self):
        return {
            'name': 'rlp',
            'title': 'Datenportal Rheinland-Pfalz',
            'description': self.__doc__.split('\n')[0]
        }

    def __init__(self, name='rlp_harvester'):
        schema_url = config.get('ckanext.govdata.urls.schema')
        groups_url = config.get('ckanext.govdata.urls.groups')

        if schema_url is None:
            error_message = 'Missing configuration value for {config_key}'.format(
                config_key='ckanext.govdata.urls.schema'
            )
            raise ValueError(error_message)
        if groups_url is None:
            error_message = 'Missing configuration value for {config_key}'.format(
                config_key='ckanext.govdata.urls.groups'
            )
            raise ValueError(error_message)

        log.debug('schema_url: ' + schema_url)
        log.debug('groups_url: ' + groups_url)

        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())

        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    @classmethod
    def has_possible_contact_fields(cls, package_dict):
        '''
        Has the given dict possible contact fields?
        '''
        if 'point_of_contact' in package_dict and \
            'point_of_contact_address' in package_dict and \
            'email' in package_dict['point_of_contact_address']:
            return True
        return False

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        # manually set package type
        package['type'] = 'datensatz'
        if all([resource['format'].lower() == 'pdf' for resource in package['resources']]):
            package['type'] = 'dokument'

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

        if self.has_possible_contact_fields(package):
            assert_author_fields(
                package,
                package['point_of_contact'],
                package['point_of_contact_address']['email']
            )

        package['extras']['metadata_original_portal'] = self.portal
        package['extras']['sector'] = 'oeffentlich'

        # the extra fields are present as CKAN core fields in the remote
        # instance: copy all content from these fields into the extras field
        for extra_field in self.schema['properties']['extras']['properties'].keys():
            if extra_field in package:
                package['extras'][extra_field] = package[extra_field]
                del package[extra_field]

        # convert license cc-by-nc to cc-nc
        if package['extras']['terms_of_use']['license_id'] == 'cc-by-nc':
            package['extras']['terms_of_use']['license_id'] = 'cc-nc'

        package['license_id'] = package[
            'extras']['terms_of_use']['license_id']

        # GDI related patch
        if 'gdi-rp' in package['groups']:
            package['type'] = 'datensatz'

        # map these two group names to schema group names
        if 'justiz' in package['groups']:
            package['groups'].append('gesetze_justiz')
            package['groups'].remove('justiz')

        if 'transport' in package['groups']:
            package['groups'].append('transport_verkehr')
            package['groups'].remove('transport')

        # filter illegal group names
        package['groups'] = [
            group for group in package['groups'] if group in self.govdata_groups]

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        dataset = package['extras']['content_type'].lower() == 'datensatz'
        if not dataset and 'gdi-rp' not in package['groups']:
            return  # skip all non-datasets for the time being

        try:
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            log.exception(error)
            return

        harvest_object.content = json.dumps(package)
        super(RlpCKANHarvester, self).import_stage(harvest_object)


class DatahubCKANHarvester(GroupCKANHarvester):

    '''A CKAN harvester for Datahub.io importing a small set of packages.'''

    implements(IHarvester)

    valid_packages = [
        'hbz_unioncatalog',
        'lobid-resources',
        'deutsche-nationalbibliografie-dnb',
        'dnb-gemeinsame-normdatei'
    ]

    def __init__(self, name='datahub_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal_url = url_dict['portal_url']

    def info(self):
        return {
            'name': 'datahub',
            'title': 'Datahub.io',
            'description': self.__doc__.split('\n')[0]
        }

    def fetch_stage(self, harvest_object):
        log.debug('In CKANHarvester fetch_stage')
        self._set_config(harvest_object.job.source.config)

        if harvest_object.guid not in self.valid_packages:
            return None

        # Get source URL
        url = harvest_object.source.url.rstrip('/')
        url = url + self._get_rest_api_offset() + '/package/'
        url = url + harvest_object.guid

        # Get contents
        try:
            content = self._get_content(url)
        except Exception, error:
            self._save_object_error('Unable to get content for package:'
                                    '%s: %r' % (url, error), harvest_object)
            log.exception(error)
            return None

        # Save the fetched contents in the harvest object
        harvest_object.content = content
        harvest_object.save()
        return True

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        package['type'] = 'datensatz'

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

        package['extras']['metadata_original_portal'] = self.portal_url
        package['groups'].append('bildung_wissenschaft')

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(DatahubCKANHarvester, self).import_stage(harvest_object)


class OpenNrwCKANHarvester(GroupCKANHarvester):

    '''A CKAN Harvester for OpenNRW'''

    def __init__(self, name='opennrw_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal_url = url_dict['portal_url']

    def info(self):
        return {
            'name': 'opennrw',
            'title': 'Datenportal OpenNRW',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        package['extras']['metadata_original_portal'] = self.portal_url
        package['extras']['metadata_transformer'] = ''

        harvest_object.content = json.dumps(package)
        super(OpenNrwCKANHarvester, self).import_stage(harvest_object)
