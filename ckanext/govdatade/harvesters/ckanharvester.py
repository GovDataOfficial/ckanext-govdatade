#!/usr/bin/python
# -*- coding: utf8 -*-
'''
Module for harvesting CKAN instances into GovData.
'''
import csv
import datetime
import time
import json
import logging
import urllib2
import re
import uuid
import ckanapi

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError
from ckanext.harvest.interfaces import IHarvester
from ckanext.govdatade.harvesters.translator import translate_groups
from ckanext.govdatade.config import config
from simplejson.scanner import JSONDecodeError

from ckan import model
from ckan import plugins as p
from ckan.model import Session, PACKAGE_NAME_MAX_LENGTH
from ckan.plugins.core import implements

log = logging.getLogger(__name__)


NAME_RANDOM_STRING_LENGTH = 5
NAME_DELETED_SUFFIX = "-deleted"
NAME_MAX_LENGTH = PACKAGE_NAME_MAX_LENGTH-NAME_RANDOM_STRING_LENGTH-len(NAME_DELETED_SUFFIX)
DELETE_PACKAGES_LOGFILE_PATH_DEFAULT = '/var/log/ckan/auto_delete_deprecated_packages.csv'


class GovDataHarvester(CKANHarvester):

    '''The base harvester for GovData.de performing remote synchronization.'''

    implements(IHarvester)

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
    def assert_author_fields(cls, package_dict, author_alternative, author_email_alternative):
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

    @classmethod
    def build_context(cls):
        return {
            'model': model,
            'session': Session,
            'user': u'harvest',
            'api_version': 1,
            'ignore_auth': True
        }

    @classmethod
    def format_date_string(cls, time_in_seconds):
        '''Converts a time stamp to a string according to a format specification.'''

        struct_time = time.localtime(time_in_seconds)
        return time.strftime("%Y-%m-%d %H:%M", struct_time)

    @classmethod
    def create_new_name_for_deletion(cls, name):
        '''Creates new name by adding suffix "-deleted" and random string to the given name'''

        random_suffix = str(uuid.uuid4())[:NAME_RANDOM_STRING_LENGTH]
        new_name = name[:NAME_MAX_LENGTH]
        return new_name + NAME_DELETED_SUFFIX + random_suffix

    @classmethod
    def rename_datasets_before_delete(cls, deprecated_package_dicts):
        '''Renames the given packages to avoid name conflicts with deleted packages.'''

        package_update = p.toolkit.get_action('package_update')
        for package_dict in deprecated_package_dicts:
            context = cls.build_context()
            package_id = package_dict['id']
            context.update({'id':package_id})
            package_dict['name'] = cls.create_new_name_for_deletion(package_dict['name'])
            # Update package
            try:
                package_update(context, package_dict)
            except Exception as exception:
                log.error("Unable updating package %s: %s", package_id, exception)

    @classmethod
    def log_deleted_packages_in_file(cls, deprecated_package_dicts, time_in_seconds):
        '''Write the information about the deleted packages in a file.'''

        path_to_logfile = config.get('ckanext.govdata.delete_deprecated_packages.logfile',
                                     DELETE_PACKAGES_LOGFILE_PATH_DEFAULT)
        if path_to_logfile:
            log.debug("Logging to file %s.", path_to_logfile)
            with open(path_to_logfile, 'a') as logfile:
                for package_dict in deprecated_package_dicts:
                    line = ([package_dict['id'], package_dict['name'], 'deleted',
                             cls.format_date_string(time_in_seconds)])
                    csv.writer(logfile).writerow(line)
        else:
            log.error("Could not get log file path from configuration!")

    @classmethod
    def delete_packages(cls, package_ids):
        '''Deletes the packages belonging to the given package ids.'''

        package_delete = p.toolkit.get_action('package_delete')
        for to_delete_id in package_ids:
            context = cls.build_context()
            try:
                package_delete(context, {'id': to_delete_id})
            except Exception as exception:
                log.error("Unable deleting package with id %s: %s", package_id, exception)

    def delete_deprecated_datasets(self, remote_dataset_ids, harvest_job):
        '''
        Deletes deprecated datasets
        '''

        starttime = time.time()
        # load harvester configuration
        context = self.build_context()
        harvester_package = p.toolkit.get_action('package_show')(context, {'id': harvest_job.source_id})
        # Local harvest source organization
        organization_id = harvester_package.get('owner_org')
        log.info("delete_deprecated_datasets: Started at %s. Org-ID: %s. Portal: %s.",
                 self.format_date_string(starttime), str(organization_id), str(self.portal))

        # check if the information about the source portal is present
        if self.portal:
            # get all datasets of this organization step by step
            local_dataset_ids = []
            deprecated_ids_total = []
            offset = 0
            count = 0
            rows = 500
            package_search = p.toolkit.get_action('package_search')
            while offset <= count:
                query_object = {
                    "fq": '+owner_org:"' + organization_id + '" +metadata_original_portal:"' + self.portal
                          + '" -type:"harvest"',
                    "rows": rows,
                    "start": offset
                    }
                result = package_search({}, query_object)
                datasets = result["results"]
                count += len(datasets)
                log.debug("offset: %s, count: %s", str(offset), str(count))
                offset += rows

                if count != 0:
                    local_dataset_ids_sub = [x["id"] for x in datasets]
                    local_dataset_ids.extend(local_dataset_ids_sub)
                    deprecated_ids = set(local_dataset_ids_sub) - set(remote_dataset_ids)
                    log.debug('Found %s deprecated datasets.', len(deprecated_ids))
                    if deprecated_ids:
                        deprecated_ids_total.extend(deprecated_ids)
                        checkpoint_start = time.time()
                        # Rename datasets before deleting, because of possible name conflicts
                        deprecated_package_dicts = [x for x in datasets if x['id'] in deprecated_ids]
                        self.rename_datasets_before_delete(deprecated_package_dicts)
                        checkpoint_end = time.time()
                        log.debug("Time taken for renaming %s datasets: %s.",
                                  len(deprecated_ids), str(checkpoint_end-checkpoint_start))

                        # delete deprecated datasets
                        checkpoint_start = time.time()
                        self.delete_packages(deprecated_ids)
                        checkpoint_end = time.time()
                        log.debug("Deleted %s deprecated datasets. Time taken for deletion: %s.",
                                  len(deprecated_ids), str(checkpoint_end-checkpoint_start))
                        # Logging id, name and time to file system
                        deprecated_package_dicts = [x for x in datasets if x['id'] in deprecated_ids]
                        self.log_deleted_packages_in_file(deprecated_package_dicts, checkpoint_end)

            log.debug("Local datasets: %s, remote datasets: %s", len(local_dataset_ids),
                      len(remote_dataset_ids))
        else:
            log.warn("Could not get information about the source portal for harvester %s."
                     " -> SKIPPING deletion of deprecated datasets!", harvester_package['name'])

        endtime = time.time()
        log.info("delete_deprecated_datasets: Finished at %s. Deleted %s deprecated datasets. Duration: %s",
                 self.format_date_string(endtime), len(deprecated_ids_total), str(endtime-starttime))

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
        # FIXME : Replace Remote CKAN api call with CKAN internal get_action
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
        '''Retrieve remote dataset ids for synchronization.'''
        try:
            url = None
            self._set_config(harvest_job.source.config)

            base_url = harvest_job.source.url.rstrip('/')
            # save current api version
            api_version_original = self.api_version
            self.api_version = 2 # api version 2 is getting ids instead of names
            base_rest_url = base_url + self._get_rest_api_offset()
            # revert api version
            self.api_version = api_version_original
            url = base_rest_url + '/package'
            log.debug("gather_stage: package_url = " + url)

            content = self._get_content(url)
            package_ids = json.loads(content)
            self.delete_deprecated_datasets(package_ids, harvest_job)

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


class HamburgCKANHarvester(GovDataHarvester):

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
            # FIXME : Replace Remote CKAN api call with CKAN internal get_action
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

        extras['metadata_original_portal'] = self.portal

        self.assert_author_fields(
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
            self.assert_author_fields(
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


class DatahubCKANHarvester(GovDataHarvester):

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
        self.portal = url_dict['portal_url']

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

        package['extras']['metadata_original_portal'] = self.portal
        package['groups'].append('bildung_wissenschaft')

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(DatahubCKANHarvester, self).import_stage(harvest_object)


class OpenNrwCKANHarvester(GovDataHarvester):

    '''A CKAN Harvester for OpenNRW'''

    def __init__(self, name='opennrw_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

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

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

        package['extras']['metadata_original_portal'] = self.portal
        package['extras']['metadata_transformer'] = ''

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(OpenNrwCKANHarvester, self).import_stage(harvest_object)
