#!/usr/bin/python
# -*- coding: utf8 -*-
'''
Module for harvesting CKAN instances into GovData.
'''
from codecs import BOM_UTF8
import csv
import datetime
import json
import logging
import re
import time
import urllib2
import uuid

from ckan import model
from ckan import plugins as p
from ckan.logic import get_action
from ckan.model import Session, PACKAGE_NAME_MAX_LENGTH
from ckan.plugins.core import implements
from ckanext.dcatde.migration import migration_functions
from ckanext.govdatade.config import config
from ckanext.govdatade.extras import Extras
from ckanext.govdatade.harvesters.translator import translate_groups
from ckanext.govdatade.util import get_group_dict, remove_group_dict, fix_group_dict_list
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError
from ckanext.harvest.interfaces import IHarvester
from simplejson.scanner import JSONDecodeError


LOGGER = logging.getLogger(__name__)


NAME_RANDOM_STRING_LENGTH = 5
NAME_DELETED_SUFFIX = "-deleted"
NAME_MAX_LENGTH = PACKAGE_NAME_MAX_LENGTH - NAME_RANDOM_STRING_LENGTH - len(NAME_DELETED_SUFFIX)


class GovDataHarvester(CKANHarvester):

    '''The base harvester for GovData.de performing remote synchronization.'''

    implements(IHarvester)

    def __init__(self):
        '''Initializes the general necessary params from config.'''

        self.path_to_logfile = None
        schema_url = config.get('ckanext.govdata.urls.schema')
        groups_url = config.get('ckanext.govdata.urls.groups')
        path_to_logfile = config.get(
            'ckanext.govdata.delete_deprecated_packages.logfile'
        )

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
        if path_to_logfile is None:
            LOGGER.error(
                'Missing configuration value for %s',
                'ckanext.govdata.delete_deprecated_packages.logfile'
            )
        else:
            self.path_to_logfile = path_to_logfile

        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())

        self.migration_executor = migration_functions.MigrationFunctionExecutor(
            config.get('ckanext.dcatde.urls.license_mapping'),
            config.get('ckanext.dcatde.urls.category_mapping'))


    @classmethod
    def _get_rest_api_offset(cls, api_version):
        return '/api/%d/rest' % api_version

    @classmethod
    def lstrip_bom(cls, content, bom=BOM_UTF8):
        '''
        Strips the BOM if present
        '''
        if content.startswith(bom):
            return content[len(bom):]
        else:
            return content

    def amend_package(self, package):
        if 'extras' not in package:
            package['extras'] = []

        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])

        self.lowercase_resources_formats(package)

        if 'groups' in package:
            fix_group_dict_list(package['groups'])

        package.pop('relationships_as_subject', [])
        package.pop('relationships_as_object', [])

        extras = Extras(package['extras'])
        extras.update(
            'guid',
            package['id'],
            True
        )

        self.set_portal(package)

    @classmethod
    def lowercase_resources_formats(cls, package_dict):
        '''Lowercases the format values in resources.'''
        for resource in package_dict['resources']:
            resource['format'] = resource['format'].lower()

    def set_portal(self, package_dict):
        '''Set the portal into package extras.'''
        extras = Extras(package_dict['extras'])
        extras.update(
            'metadata_original_portal',
            self.portal,
            True
        )
        # Store as extra Field which will not be migrated, so we can do what we want here.
        # This field will also be set when implementing / adapting the RDF-Harvester for DCAT-AP.de
        extras.update(
            'metadata_harvested_portal',
            self.portal,
            True
        )
        package_dict['extras'] = extras.get()

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

        LOGGER.debug(config_settings_log_message)

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
    def has_tag(cls, tags_dict_list, tag):
        '''Checks if tag is present in tags dict list.'''
        if isinstance(tags_dict_list, list):
            for tags_dict in tags_dict_list:
                if 'name' in tags_dict and tags_dict.get('name').strip() == tag.strip():
                    return True

        return False

    @classmethod
    def cleanse_tags(cls, tags_dict_list):
        '''Cleans tags in given list of tags.'''

        LOGGER.debug(
            'Cleansing tags in list: %s.',
            json.dumps(tags_dict_list)
        )

        if isinstance(tags_dict_list, list):
            cleansed_tags = []
            for tags_dict in tags_dict_list:
                if 'name' in tags_dict:
                    tag = tags_dict.get('name')
                    tags_dict['name'] = cls.cleanse_special_characters(tag)
                    cleansed_tags.append(tag)

            if len(cleansed_tags) > 0:
                LOGGER.debug('Cleansed %d tags.', len(cleansed_tags))
                LOGGER.debug('Cleansed list: %s', json.dumps(tags_dict_list))

            return tags_dict_list

        cleansed_tag = cls.cleanse_special_characters(tags_dict_list)

        LOGGER.debug('Cleansed tag: %s', json.dumps(cleansed_tag))

        return cleansed_tag

    @classmethod
    def cleanse_special_characters(cls, tag):
        '''Cleans a given tag of special characters.'''
        tag = tag.lower().strip()
        return re.sub(u'[^a-zA-ZÄÖÜäöüß0-9 \-_\.]', '', tag).replace(' ', '-')

    @classmethod
    def build_context(cls):
        '''Builds a context dictionary.'''
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
        '''
        Creates new name by adding suffix "-deleted" and
        random string to the given name
        '''

        random_suffix = str(uuid.uuid4())[:NAME_RANDOM_STRING_LENGTH]
        new_name = name[:NAME_MAX_LENGTH]
        return new_name + NAME_DELETED_SUFFIX + random_suffix

    @classmethod
    def rename_datasets_before_delete(cls, deprecated_package_dicts):
        '''
        Renames the given packages to avoid name conflicts with
        deleted packages.
        '''

        renamed_package_ids = []
        package_update = p.toolkit.get_action('package_update')
        for package_dict in deprecated_package_dicts:
            context = cls.build_context()
            package_id = package_dict['id']
            context.update({'id':package_id})
            package_dict['name'] = cls.create_new_name_for_deletion(package_dict['name'])
            # Update package
            try:
                package_update(context, package_dict)
                renamed_package_ids.append(package_id)
            except Exception as exception:
                LOGGER.error("Unable updating package %s: %s", package_id, exception)

        return renamed_package_ids

    def log_deleted_packages_in_file(self, deprecated_package_dicts, time_in_seconds):
        '''Write the information about the deleted packages in a file.'''

        if self.path_to_logfile is not None:
            LOGGER.debug("Logging to file %s.", self.path_to_logfile)
            try:
                with open(self.path_to_logfile, 'a') as logfile:
                    for package_dict in deprecated_package_dicts:
                        line = ([package_dict['id'], package_dict['name'], 'deleted',
                                 self.format_date_string(time_in_seconds)])
                        csv.writer(logfile).writerow(line)
            except Exception as exception:
                LOGGER.warn(
                    'Could not write in automated deletion log file at %s: %s',
                    self.path_to_logfile,
                    exception
                )

    @classmethod
    def delete_packages(cls, package_ids):
        '''Deletes the packages belonging to the given package ids.'''

        deleted_package_ids = []
        package_delete = p.toolkit.get_action('package_delete')
        for to_delete_id in package_ids:
            context = cls.build_context()
            try:
                package_delete(context, {'id': to_delete_id})
                deleted_package_ids.append(to_delete_id)
            except Exception as exception:
                LOGGER.error(
                    "Unable to delete package with id %s: %s",
                    to_delete_id,
                    exception
                )
        return deleted_package_ids

    def delete_deprecated_datasets(self, remote_dataset_ids, harvest_job):
        '''Deletes deprecated datasets.'''

        starttime = time.time()
        # load harvester configuration
        context = self.build_context()
        harvester_package = p.toolkit.get_action('package_show')(context, {'id': harvest_job.source_id})
        # Local harvest source organization
        organization_id = harvester_package.get('owner_org')
        LOGGER.info(
            "delete_deprecated_datasets: Started at %s. Org-ID: %s. Portal: %s.",
            self.format_date_string(starttime),
            str(organization_id),
            str(self.portal)
        )

        deleted_package_ids = []
        if remote_dataset_ids:
            # check if the information about the source portal is present
            if self.portal:
                # get all datasets of this organization step by step
                local_dataset_ids = []
                deprecated_package_dicts_total = []
                offset = 0
                count = 0
                rows = 500
                package_search = p.toolkit.get_action('package_search')

                rename_log_message = 'Time taken for renaming %s datasets: %s.'
                delete_log_message = 'Deleted %s deprecated datasets. Time taken for deletion: %s.'

                while offset <= count:
                    query_object = {
                        "fq": '+owner_org:"' + organization_id + '" +metadata_harvested_portal:"' + self.portal
                              + '" -type:"harvest"',
                        "rows": rows,
                        "start": offset
                        }
                    result = package_search({}, query_object)
                    datasets = result["results"]
                    count += len(datasets)
                    LOGGER.debug("offset: %s, count: %s", str(offset), str(count))
                    offset += rows

                    if count != 0:
                        local_dataset_ids_sub = [x["id"] for x in datasets]
                        local_dataset_ids.extend(local_dataset_ids_sub)
                        deprecated_ids = set(local_dataset_ids_sub) - set(remote_dataset_ids)
                        for x in datasets:
                            if x['id'] in deprecated_ids:
                                deprecated_package_dicts_total.append(self.get_min_package_dict(x))

                if len(deprecated_package_dicts_total) > 0:
                    LOGGER.debug('Found %s deprecated datasets.', len(deprecated_package_dicts_total))
                    checkpoint_start = time.time()
                    # Rename datasets before deleting, because of possible name conflicts
                    renamed_package_ids = self.rename_datasets_before_delete(deprecated_package_dicts_total)
                    checkpoint_end = time.time()
                    LOGGER.debug(
                        rename_log_message,
                        len(renamed_package_ids),
                        str(checkpoint_end - checkpoint_start)
                    )
                    # delete deprecated datasets
                    checkpoint_start = time.time()
                    deleted_package_ids = self.delete_packages(renamed_package_ids)
                    checkpoint_end = time.time()
                    LOGGER.debug(
                        delete_log_message,
                        len(deleted_package_ids),
                        str(checkpoint_end - checkpoint_start)
                    )
                    # Logging id, name and time to file system
                    deleted_package_dicts = [
                        x for x in deprecated_package_dicts_total if x['id'] in deleted_package_ids]
                    self.log_deleted_packages_in_file(deleted_package_dicts, checkpoint_end)

                LOGGER.debug(
                    "Local datasets: %s, remote datasets: %s",
                    len(local_dataset_ids),
                    len(remote_dataset_ids)
                )
            else:
                log_message = 'Could not get information about the source portal for harvester %s.'
                log_message += ' -> SKIPPING deletion of deprecated datasets!'
                LOGGER.warn(log_message, harvester_package['name'])

        else:
            log_message = 'The list of remote dataset ids is not set or empty for harvester %s.'
            log_message += ' -> SKIPPING deletion of deprecated datasets!'
            LOGGER.warn(log_message, harvester_package['name'])

        endtime = time.time()
        log_message = 'delete_deprecated_datasets: Finished at %s. '
        log_message += 'Deleted %s deprecated datasets. Duration: %s.'
        LOGGER.info(
            log_message,
            self.format_date_string(endtime),
            len(deleted_package_ids),
            str(endtime - starttime)
        )

    @classmethod
    def get_min_package_dict(cls, package_dict):
        if package_dict:
            result = {
                      'id': package_dict['id'],
                      'name': package_dict['name'],
                      'metadata_modified': package_dict['metadata_modified']}
            return result
        else:
            return

    @classmethod
    def compare_metadata_modified(cls, remote_md_modified, local_md_modified):
        '''
        Compares the modified datetimes of the metadata
        '''
        dt_format = '%Y-%m-%dT%H:%M:%S.%f'
        remote_dt = datetime.datetime.strptime(remote_md_modified, dt_format)
        local_dt = datetime.datetime.strptime(local_md_modified, dt_format)
        if remote_dt < local_dt:
            LOGGER.debug('remote dataset precedes local dataset -> skipping.')
            return False
        elif remote_dt == local_dt:
            LOGGER.debug('remote dataset equals local dataset -> skipping.')
            return False
        else:
            LOGGER.debug('local dataset precedes remote dataset -> importing.')
            # TODO do I have to delete other dataset?
            return True

    def verify_transformer(self, harvest_object_content):
        '''Compares new dataset with existing and checks, if a dataset should be imported.'''
        context = self.build_context()

        remote_dataset = json.loads(harvest_object_content)
        remote_dataset_extras = Extras(remote_dataset['extras'])

        has_orig_id = remote_dataset_extras.key('identifier')

        if has_orig_id:
            orig_id = remote_dataset_extras.value('identifier')
            try:
                data_dict = {"q": 'identifier:"' + orig_id + '"'}
                local_search_result = get_action("package_search")(context, data_dict)
                if local_search_result['count'] == 0:
                    LOGGER.debug('Did not find this original id. Import accepted.')
                    return True
                if local_search_result['count'] == 1:
                    LOGGER.debug('Found duplicate entry')
                    local_dataset = local_search_result['results'][0]

                    if 'metadata_modified' in remote_dataset:
                        return self.compare_metadata_modified(
                            remote_dataset['metadata_modified'],
                            local_dataset['metadata_modified']
                        )
                    else:
                        LOGGER.debug(
                            'Found duplicate entry but remote dataset does not contain metadata_modified -> skipping.')
                        return False
            except Exception as exception:
                LOGGER.error(exception)
        else:
            LOGGER.debug('no metadata_original_id. Importing accepted.')
            return True

    def gather_stage(self, harvest_job):
        '''Retrieve remote dataset ids for synchronization.'''
        try:
            url = None
            self._set_config(harvest_job.source.config)

            base_url = harvest_job.source.url.rstrip('/')
            # api version 2 is getting ids instead of names
            base_rest_url = base_url + self._get_rest_api_offset(2)
            url = base_rest_url + '/package'
            LOGGER.debug("gather_stage: package_url = " + url)

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

    def migrate_dataset(self, harvest_object):
        ''' Migrate OGD to DCAT-AP.de'''
        package = json.loads(harvest_object.content)
        # Harvesters are skipped with a warning in the parent class, no migration needed
        if package.get('type') == 'harvest':
            return

        self.migration_executor.apply_to(package)
        # ensure that the package has the type 'dataset'.
        # This property is ignored for existing datasets, but as they were migrated,
        # the type is correct already.
        package['type'] = 'dataset'

        harvest_object.content = json.dumps(package)

    def import_stage(self, harvest_object):
        self.migrate_dataset(harvest_object)

        to_import = self.verify_transformer(harvest_object.content)
        if to_import:
            return super(GovDataHarvester, self).import_stage(harvest_object)
        else:
            return 'unchanged'


class RostockCKANHarvester(GovDataHarvester):

    '''A CKAN harvester for Rostock solving data compatibility problems.'''

    implements(IHarvester)

    def __init__(self, name='rostock_harvester'):
        GovDataHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['name'] = package['name'] + '-hro'

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Rostock: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(RostockCKANHarvester, self).import_stage(harvest_object)


class HamburgCKANHarvester(GovDataHarvester):

    '''A CKAN harvester for Hamburg solving data compatibility problems.'''


    implements(IHarvester)

    def __init__(self, name='hamburg_harvester'):
        GovDataHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        context = self.build_context()

        extras = Extras(package['extras'])

        is_latest_version = None
        if extras.key('latestVersion'):
            is_latest_version = extras.value('latestVersion')

        if is_latest_version == 'true':
            LOGGER.debug(
                'received latestVersion == true. Continue with this dataset')

            remote_metadata_original_id = extras.value(
                'metadata_original_id'
            )

            # compare harvested OGD-Dataset with local DCAT-AP.de-Dataset
            data_dict = {"q": 'identifier:"' + remote_metadata_original_id + '"'}
            local_search_result = get_action("package_search")(context, data_dict)

            if local_search_result['count'] == 0:
                LOGGER.debug(
                    'Did not find this metadata original id. Import accepted.')
            elif local_search_result['count'] == 1:
                LOGGER.debug(
                    'Found local dataset for particular metadata_original_id')
                local_dataset_from_action_api = local_search_result[
                    'results'][0]

                # copy name and id from local dataset to remote dataset
                LOGGER.debug('Copy id and name to remote dataset')
                LOGGER.debug(package['id'])
                LOGGER.debug(package['name'])
                package['id'] = local_dataset_from_action_api['id']
                package['name'] = local_dataset_from_action_api['name']
                LOGGER.debug(package['id'])
                LOGGER.debug(package['name'])
            else:

                log_message = 'Found more than one local dataset for '
                log_message = log_message + 'particular metadata_original_id. '
                log_message = log_message + 'Offending metadata_original_id '
                log_message = log_message + 'is:'
                LOGGER.debug(log_message)
                LOGGER.debug(remote_metadata_original_id)
        elif is_latest_version == 'false':
            # do not import or update this particular remote dataset
            LOGGER.debug('received latestVersion == false. Skip this dataset')
            return False

        # check if import is desired
        if package['type'] == 'document' or package['type'] == 'dokument':
            if not self.has_tag(package['tags'], 'govdata'):
                LOGGER.debug("Found invalid package with 'govdata' tag")
                return False
            package['type'] = 'dokument'
        elif package['type'] == 'dataset':
            package['type'] = 'datensatz'

        # fix groups
        LOGGER.debug('Before: ')
        LOGGER.debug(package['groups'])
        package['groups'] = translate_groups(package['groups'], 'hamburg')
        LOGGER.debug('After: ')
        LOGGER.debug(package['groups'])

        self.assert_author_fields(
            package,
            package.get('maintainer'),
            package.get('maintainer_email')
        )

        return True

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            valid = self.amend_package(package)
            if not valid:
                return  # drop package
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Hamburg: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(HamburgCKANHarvester, self).import_stage(harvest_object)


class BerlinCKANHarvester(GovDataHarvester):

    '''A CKAN harvester for Berlin solving data compatibility problems.'''

    implements(IHarvester)

    def __init__(self, name='berlin_harvester'):
        GovDataHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        if 'license_id' not in package or package['license_id'] == '':
            package['license_id'] = 'notspecified'

        extras = Extras(package['extras'])

        # if sector is not set, set it to 'oeffentlich' (default)
        if not extras.key('sector', disallow_empty=True):
            extras.update('sector', 'oeffentlich', True)

        if extras.value('sector') != 'oeffentlich':
            return False

        # avoid ValidationError when extra dict
        # key 'type' is also used by the internal CKAN validation,
        # see GOVDATA-651
        if extras.key('type'):
            extras.remove('type')

        package['extras'] = extras.get()

        valid_types = ['datensatz', 'dokument', 'app']
        if not package.get('type') or package['type'] not in valid_types:
            package['type'] = 'datensatz'

        package['groups'] = translate_groups(package['groups'], 'berlin')

        return True

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            valid = self.amend_package(package)

            if not valid:
                return 'unchanged'  # drop package

        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Berlin: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(BerlinCKANHarvester, self).import_stage(harvest_object)


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
        GovDataHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        extras = Extras(package['extras'])

        # manually set package type
        package['type'] = 'datensatz'
        if all([resource['format'].lower() == 'pdf' for resource in package['resources']]):
            package['type'] = 'dokument'

        if self.has_possible_contact_fields(package):
            self.assert_author_fields(
                package,
                package['point_of_contact'],
                package['point_of_contact_address']['email']
            )

        extras.update('sector', 'oeffentlich', True)

        # the extra fields are present as CKAN core fields in the remote
        # instance: copy all content from these fields into the extras field
        extra_fields = self.schema['properties']['extras']['properties'].keys()
        for extra_field in extra_fields:
            if extra_field in package:
                extras.update(
                    extra_field,
                    package[extra_field],
                    True
                )
                del package[extra_field]

        # convert license cc-by-nc to cc-nc
        if package['license_id'] == 'cc-by-nc':
            package['license_id'] = 'cc-nc'

        package['extras'] = extras.get()

        # GDI related patch
        if 'gdi-rp' in package['groups']:
            package['type'] = 'datensatz'

        # map these two group names to schema group names
        if 'justiz' in package['groups']:
            package['groups'].append(get_group_dict('gesetze_justiz'))
            package['groups'] = remove_group_dict(package['groups'], 'justiz')

        if 'transport' in package['groups']:
            package['groups'].append(get_group_dict('transport_verkehr'))
            package['groups'] = remove_group_dict(package['groups'], 'transport')

        # filter illegal group names
        package['groups'] = [
            group for group in package['groups'] if group['name'] in self.govdata_groups]

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Rlp: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(RlpCKANHarvester, self).import_stage(harvest_object)


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
        GovDataHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'datahub',
            'title': 'Datahub.io',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        GovDataHarvester.amend_package(self, package)

        package['type'] = 'datensatz'
        package['groups'].append(get_group_dict('bildung_wissenschaft'))

        return True

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            if package['name'] not in self.valid_packages:
                LOGGER.info('Datahub: Package %s is not within whitelist, skipping...', package['name'])
                return 'unchanged'
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Datahub: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(DatahubCKANHarvester, self).import_stage(harvest_object)


class OpenNrwCKANHarvester(GovDataHarvester):

    '''A CKAN Harvester for OpenNRW'''

    def __init__(self, name='opennrw_harvester'):
        GovDataHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        extras = Extras(package['extras'])
        extras.update('metadata_transformer', '', True)

        package['extras'] = extras.get()

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('OpenNrw: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(OpenNrwCKANHarvester, self).import_stage(harvest_object)
