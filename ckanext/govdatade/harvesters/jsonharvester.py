#!/usr/bin/python
# -*- coding: utf8 -*-
'''
Module for harvesting JSON based data into GovData.
'''
import urllib2
import json
import logging
import StringIO
import uuid
import zipfile

from urlparse import urlparse
from zipfile import BadZipfile
from codecs import BOM_UTF8

from ckanext.harvest.model import HarvestObject
from ckanext.govdatade.config import config
from ckanext.govdatade.harvesters.translator import translate_groups
from ckanext.govdatade.harvesters.ckanharvester import GovDataHarvester
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError

log = logging.getLogger(__name__)


class JSONDumpBaseCKANHarvester(GovDataHarvester):

    '''A base CKAN harvester for CKAN instances returning JSON dump files.'''

    def info(self):
        return {
            'name': 'json-base',
            'title': 'Base JSON dump harvester',
            'description': self.__doc__.split('\n')[0]
        }

    def gather_stage(self, harvest_job):
        super(GovDataHarvester, self)._set_config(
            harvest_job.source.config
        )

        try:
            content = self._get_content(harvest_job.source.url)
        except ContentFetchError as err:
            self._save_gather_error(err.message, harvest_job)
            return None
        except Exception, err:
            error_template = 'Unable to get content for URL: %s: %s'
            error = error_template % (harvest_job.source.url, str(err))
            self._save_gather_error(error, harvest_job)
            return None

        object_ids = []

        packages = json.loads(content)
        for package in packages:
            obj = HarvestObject(guid=package['name'], job=harvest_job)
            obj.content = json.dumps(package)
            obj.save()
            object_ids.append(obj.id)

        if object_ids:
            return object_ids
        else:
            self._save_gather_error('No packages received for URL: %s' % harvest_job.source.url,
                                    harvest_job)
            return None

    def fetch_stage(self, harvest_object):
        self._set_config(harvest_object.job.source.config)

        if harvest_object.content:
            return True
        return False


class BremenCKANHarvester(JSONDumpBaseCKANHarvester):

    '''A CKAN harvester for Bremen solving data compatibility problems.'''

    def __init__(self, name='bremen_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'bremen',
            'title': 'Datenportal Bremen',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''This function fixes some differences in the datasets
           retrieved from Bremen and our schema such as:
        - fix groups
        - set metadata_original_portal
        - fix terms_of_use
        - copy veroeffentlichende_stelle to maintainer
        - set spatial text
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        package['extras']['metadata_original_portal'] = self.portal

        # set correct groups
        if not package['groups']:
            package['groups'] = []

        groups_before_log_message = 'groups before translate: {groups}'.format(
            groups=json.dumps(package['groups'])
        )
        log.debug(groups_before_log_message)

        package['groups'] = translate_groups(package['groups'], 'bremen')

        groups_after_log_message = 'groups after translate: {groups}'.format(
            groups=json.dumps(package['groups'])
        )
        log.debug(groups_after_log_message)

        # copy veroeffentlichende_stelle to maintainer
        if 'contacts' in package['extras']:
            quelle = filter(
                lambda x: x['role'] == 'veroeffentlichende_stelle',
                package['extras']['contacts']
            )
            if quelle:
                package['maintainer'] = quelle[0]['name']
                package['maintainer_email'] = quelle[0]['email']
            else:
                log.info('Unable to resolve maintainer details')

        # fix typos in terms of use
        if 'terms_of_use' in package['extras']:
            self.fix_terms_of_use(package['extras']['terms_of_use'])
            package['license_id'] = package['extras'][
                'terms_of_use']['license_id']
        else:
            package['license_id'] = u'notspecified'

        if 'spatial-text' not in package['extras']:
            package['extras']['spatial-text'] = 'Bremen 04 0 11 000'

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(
            uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(BremenCKANHarvester, self).import_stage(harvest_object)

    @classmethod
    def fix_terms_of_use(cls, terms_of_use):
        '''
        Fixes the data structure for terms of use
        '''
        terms_of_use['license_id'] = terms_of_use['licence_id']
        del terms_of_use['licence_id']
        terms_of_use['license_url'] = terms_of_use['licence_url']
        del terms_of_use['licence_url']


class JSONZipBaseHarvester(JSONDumpBaseCKANHarvester):

    '''A base CKAN harvester for CKAN instances returning zipped JSON dump files.'''

    def info(self):
        return {
            'name': 'json-zip-base',
            'title': 'Base JSON zip harvester',
            'description': self.__doc__.split('\n')[0]
        }

    @classmethod
    def lstrip_bom(cls, content, bom=BOM_UTF8):
        '''
        Strips the BOM if present
        '''
        if content.startswith(bom):
            return content[len(bom):]
        else:
            return content

    def _get_content(self, url):
        http_request = urllib2.Request(url=url)
        # Set User-agent to a different value, because the BFJ-Harvester endpoint do not accept
        # the default urllib2 User-agent.
        http_request.add_header('User-agent', 'govdata-harvester')

        try:
            http_response = urllib2.urlopen(http_request)
            log.debug('http headers: ' + str(http_request.header_items()))
        except urllib2.HTTPError, e:
            if e.getcode() == 404:
                raise ContentNotFoundError('HTTP error: %s' % e.code)
            else:
                raise ContentFetchError('HTTP error: %s' % e.code)
        except urllib2.URLError, e:
            raise ContentFetchError('URL error: %s' % e.reason)
        except httplib.HTTPException, e:
            raise ContentFetchError('HTTP Exception: %s' % e)
        return http_response.read()

    def gather_stage(self, harvest_job, encoding=None):
        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            content = self._get_content(harvest_job.source.url)
            log.debug('Grabbing zip file: %s', harvest_job.source.url)

            object_ids = []
            packages = []

            file_content = StringIO.StringIO(content)
            archive = zipfile.ZipFile(file_content, 'r')
            for name in archive.namelist():
                if name.endswith('.json'):
                    archive_content = archive.read(name)
                    if encoding is not None:
                        archive_content = archive_content.decode(encoding)
                    else:
                        archive_content = self.lstrip_bom(archive_content)
                    package = json.loads(archive_content)
                    packages.append(package)
                    obj = HarvestObject(guid=package['name'], job=harvest_job)
                    obj.content = json.dumps(package)
                    obj.save()
                    object_ids.append(obj.id)

        except BadZipfile as err:
            self._save_gather_error(err.message, harvest_job)
            return None
        except ContentFetchError as err:
            self._save_gather_error(err.message, harvest_job)
            return None
        except Exception as err:
            error_template = 'Unable to get content for URL: %s: %s'
            error = error_template % (harvest_job.source.url, str(err))
            self._save_gather_error(error, harvest_job)
            return None

        if object_ids:
            return object_ids
        else:
            self._save_gather_error('No packages received for URL: %s' % harvest_job.source.url,
                                    harvest_job)
            return None


class GdiHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Geodateninfrastruktur Deutschland solving data compatibility problems.'''

    def __init__(self, name='gdi_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'gdi',
            'title': 'Geodateninfrastruktur',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(
            uuid.uuid5(uuid.NAMESPACE_OID, str(package['name']))
        )
        package['extras']['metadata_original_portal'] = self.portal
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class GenesisDestatisZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Genesis Destatis solving data compatibility problems.'''

    def __init__(self, name='genesis_destatis_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'genesis_destatis',
            'title': ' Genesis Destatis',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id

        package['id'] = str(
            uuid.uuid5(uuid.NAMESPACE_OID, str(package['name']))
        )
        package['extras']['metadata_original_portal'] = self.portal

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class RegionalstatistikZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Regionalstatistik solving data compatibility problems.'''

    def __init__(self, name='regionalstatistik_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'regionalstatistik',
            'title': 'Regionalstatistik',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(
            uuid.uuid5(uuid.NAMESPACE_OID, str(package['name']))
        )
        package['extras']['metadata_original_portal'] = self.portal
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        self.amend_package(package)
        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class DestatisZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Destatis solving data compatibility problems.'''

    def __init__(self, name='destatis_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'destatis',
            'title': 'Destatis',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id

        package['id'] = str(
            uuid.uuid5(uuid.NAMESPACE_OID, str(package['name']))
        )
        package['extras']['metadata_original_portal'] = self.portal

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class SachsenZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Sachsen solving data compatibility problems.'''

    def __init__(self, name='sachsen_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'sachsen',
            'title': 'Datenportal Sachsen',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(
            uuid.uuid5(uuid.NAMESPACE_OID, str(package['name']))
        )
        package['extras']['metadata_original_portal'] = self.portal

        # resource format to lower case
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class BmbfZipHarvester(JSONDumpBaseCKANHarvester):

    '''A CKAN harvester for BMBF solving data compatibility problems.'''

    def __init__(self, name='bmbf_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'bmbf',
            'title': u'Datenportal Bundesministerium für Bildung und Forschung',
            'description': self.__doc__.split('\n')[0]
        }

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
        self.api_version = 1
        self.config['api_version'] = 1
        self.config['force_all'] = True
        self.config['remote_groups'] = 'only_local'
        self.config['user'] = 'bmbf-datenportal'

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))

        package['id'] = str(uuid.uuid5(
            uuid.NAMESPACE_OID, str(package['name'])
        ))
        package['extras']['metadata_original_portal'] = self.portal

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):

        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(BmbfZipHarvester, self).import_stage(harvest_object)


class BfjHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for BfJ solving data compatibility problems.'''

    def __init__(self, name='bfj_harvester'):
        url_dict = config.get_harvester_urls(name)
        log.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        return {
            'name': 'bfj',
            'title': u'Datenportal Bundesamt für Justiz',
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        if 'tags' in package:
            package['tags'] = self.cleanse_tags(package['tags'])
            log.debug('Cleansed tags: %s', json.dumps(package['tags']))
        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(
            uuid.uuid5(uuid.NAMESPACE_OID, str(package['name']))
        )
        package['extras']['metadata_original_portal'] = self.portal
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def gather_stage(self, harvest_job):
        return super(BfjHarvester, self).gather_stage(harvest_job, 'latin9')

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)
