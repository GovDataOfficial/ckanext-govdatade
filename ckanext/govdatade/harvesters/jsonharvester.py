#!/usr/bin/python
# -*- coding: utf8 -*-
'''
Module for harvesting JSON based data into GovData.
'''
import StringIO
import json
import logging
import urllib2
import uuid
import zipfile

from ckanext.govdatade.config import config
from ckanext.govdatade.extras import Extras
from ckanext.govdatade.harvesters.ckanharvester import GovDataHarvester
from ckanext.govdatade.harvesters.translator import translate_groups
from ckanext.govdatade.util import normalize_api_dataset
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, ContentNotFoundError
from ckanext.harvest.model import HarvestObject


LOGGER = logging.getLogger(__name__)


class JSONDumpBaseCKANHarvester(GovDataHarvester):

    '''A base CKAN harvester for CKAN instances returning JSON dump files.'''

    def __init__(self):
        '''Initializes the general necessary params from config.'''

        GovDataHarvester.__init__(self)

    def info(self):
        '''
        Returns a self describing dictionary.
        '''
        return {
            'name': 'json-base',
            'title': 'Base JSON dump harvester',
            'description': self.__doc__.split('\n')[0]
        }

    def generate_id_from_name(self, package_name):
        '''
        Generates the id based on OID namespace and package name,
        this ensures that packages with the same name get the same id.
        '''
        return str(uuid.uuid5(uuid.NAMESPACE_OID, str(package_name)))

    def delete_deprecated_datasets(self, packages, harvest_job):
        '''
        Wrapper for deleting deprecated packages.
        '''

        remote_dataset_ids = [self.generate_id_from_name(x['name']) for x in packages]
        super(JSONDumpBaseCKANHarvester, self).delete_deprecated_datasets(
            remote_dataset_ids,
            harvest_job
        )

    def gather_stage(self, harvest_job):
        super(JSONDumpBaseCKANHarvester, self)._set_config(
            harvest_job.source.config
        )

        try:
            content = self._get_content(harvest_job.source.url)
        except ContentFetchError as err:
            self._save_gather_error(err.message, harvest_job)
            return None
        except Exception as err:
            error_template = 'Unable to get content for URL: %s: %s'
            error = error_template % (harvest_job.source.url, str(err))
            self._save_gather_error(error, harvest_job)
            return None

        object_ids = []

        packages = json.loads(content)

        for package in packages:
            normalize_api_dataset(package)
            obj = HarvestObject(guid=package['name'], job=harvest_job)
            obj.content = json.dumps(package)
            obj.save()
            object_ids.append(obj.id)

        if object_ids:
            # delete obsolete packages
            self.delete_deprecated_datasets(packages, harvest_job)
            return object_ids
        else:
            self._save_gather_error(
                'No packages received for URL: %s' % harvest_job.source.url,
                harvest_job
            )
            return None

    def fetch_stage(self, harvest_object):
        self._set_config(harvest_object.job.source.config)

        if harvest_object.content:
            return True
        return False


class JSONZipBaseHarvester(JSONDumpBaseCKANHarvester):

    '''A base CKAN harvester for CKAN instances returning zipped JSON dump files.'''

    def __init__(self):
        '''Initializes the general necessary params from config.'''

        JSONDumpBaseCKANHarvester.__init__(self)

    def info(self):
        return {
            'name': 'json-zip-base',
            'title': 'Base JSON zip harvester',
            'description': self.__doc__.split('\n')[0]
        }

    def _get_content(self, url):
        http_request = urllib2.Request(url=url)
        '''
        Set User-agent to a different value, because the BFJ-Harvester endpoint
        does not accept the default urllib2 User-agent.
        '''
        http_request.add_header('User-agent', 'govdata-harvester')

        try:
            http_response = urllib2.urlopen(http_request)
            LOGGER.debug('http headers: ' + str(http_request.header_items()))
        except urllib2.HTTPError as error:
            if error.getcode() == 404:
                raise ContentNotFoundError('HTTP error: %s' % error.code)
            else:
                raise ContentFetchError('HTTP error: %s' % error.code)
        except urllib2.URLError as error:
            raise ContentFetchError('URL error: %s' % error.reason)
        except httplib.HTTPException as error:
            raise ContentFetchError('HTTP Exception: %s' % error)
        return http_response.read()

    def gather_stage(self, harvest_job, encoding=None):
        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            content = self._get_content(harvest_job.source.url)
            LOGGER.debug('Grabbing zip file: %s', harvest_job.source.url)

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
                    normalize_api_dataset(package)
                    packages.append(package)
                    obj = HarvestObject(guid=package['name'], job=harvest_job)
                    obj.content = json.dumps(package)
                    obj.save()
                    object_ids.append(obj.id)

        except zipfile.BadZipfile as err:
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
            # delete obsolete packages
            super(JSONZipBaseHarvester, self).delete_deprecated_datasets(
                packages,
                harvest_job
            )

            return object_ids
        else:
            self._save_gather_error(
                'No packages received for URL: %s' % harvest_job.source.url,
                harvest_job
            )

            return None


class BremenCKANHarvester(JSONDumpBaseCKANHarvester):

    '''A CKAN harvester for Bremen solving data compatibility problems.'''

    def __init__(self, name='bremen_harvester'):
        JSONDumpBaseCKANHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

        # set correct groups
        if not package['groups']:
            package['groups'] = []

        groups_before_log_message = 'groups before translate: {groups}'.format(
            groups=json.dumps(package['groups'])
        )
        LOGGER.debug(groups_before_log_message)

        package['groups'] = translate_groups(package['groups'], 'bremen')

        groups_after_log_message = 'groups after translate: {groups}'.format(
            groups=json.dumps(package['groups'])
        )
        LOGGER.debug(groups_after_log_message)

        # copy veroeffentlichende_stelle to maintainer
        extras = Extras(package['extras'])

        if extras.key('contacts'):
            contacts_dict = json.loads(extras.value('contacts'))
            quelle = filter(
                lambda x: x['role'] == 'veroeffentlichende_stelle',
                contacts_dict
            )
            if quelle:
                package['maintainer'] = quelle[0]['name']
                package['maintainer_email'] = quelle[0]['email']
            else:
                LOGGER.info('Unable to resolve maintainer details')

        # fix typos in terms of use
        package['license_id'] = u'notspecified'

        if extras.key('terms_of_use'):
            self.fix_terms_of_use(extras)
            terms_of_use_dict = json.loads(extras.value('terms_of_use'))
            package['license_id'] = terms_of_use_dict['license_id']

        if not extras.key('spatial-text'):
            extras.update('spatial-text', 'Bremen 04 0 11 000', True)

        package['extras'] = extras.get()

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Bremen: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(BremenCKANHarvester, self).import_stage(harvest_object)

    @classmethod
    def fix_terms_of_use(cls, extras):
        '''
        Fixes the data structure for terms of use.
        '''
        if extras.key('terms_of_use'):
            terms_of_use = json.loads(extras.value('terms_of_use'))

            if 'licence_id' in terms_of_use:
                terms_of_use['license_id'] = terms_of_use['licence_id']
                del terms_of_use['licence_id']
            if 'licence_url' in terms_of_use:
                terms_of_use['license_url'] = terms_of_use['licence_url']
                del terms_of_use['licence_url']

            extras.update(
                'terms_of_use',
                json.dumps(terms_of_use),
                True
            )


class GdiHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Geodateninfrastruktur Deutschland solving data compatibility problems.'''

    def __init__(self, name='gdi_harvester'):
        JSONZipBaseHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Gdi: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(GdiHarvester, self).import_stage(harvest_object)


class GenesisDestatisZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Genesis Destatis solving data compatibility problems.'''

    def __init__(self, name='genesis_destatis_harvester'):
        JSONZipBaseHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('GenesisDestatis: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(GenesisDestatisZipHarvester, self).import_stage(harvest_object)


class RegionalstatistikZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Regionalstatistik solving data compatibility problems.'''

    def __init__(self, name='regionalstatistik_harvester'):
        JSONZipBaseHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Regionalstatistik: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(RegionalstatistikZipHarvester, self).import_stage(harvest_object)


class DestatisZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Destatis solving data compatibility problems.'''

    def __init__(self, name='destatis_harvester'):
        JSONZipBaseHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('DestatisZip: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(DestatisZipHarvester, self).import_stage(harvest_object)


class SachsenZipHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for Sachsen solving data compatibility problems.'''

    def __init__(self, name='sachsen_harvester'):
        JSONZipBaseHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('SachsenZip: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(SachsenZipHarvester, self).import_stage(harvest_object)


class BmbfZipHarvester(JSONDumpBaseCKANHarvester):

    '''A CKAN harvester for BMBF solving data compatibility problems.'''

    def __init__(self, name='bmbf_harvester'):
        JSONDumpBaseCKANHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
        self.portal = url_dict['portal_url']

    def info(self):
        title = u'Datenportal Bundesministerium für Bildung und Forschung'
        return {
            'name': 'bmbf',
            'title': title,
            'description': self.__doc__.split('\n')[0]
        }

    def amend_package(self, package):
        '''
        Amends the package data
        '''
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('BmbfZip: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(BmbfZipHarvester, self).import_stage(harvest_object)


class BfjHarvester(JSONZipBaseHarvester):

    '''A CKAN harvester for BfJ solving data compatibility problems.'''

    def __init__(self, name='bfj_harvester'):
        JSONZipBaseHarvester.__init__(self)
        url_dict = config.get_harvester_urls(name)
        LOGGER.debug('url_dict: %s', json.dumps(url_dict))
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
        GovDataHarvester.amend_package(self, package)

        package['id'] = self.generate_id_from_name(package['name'])

    def gather_stage(self, harvest_job):
        return super(BfjHarvester, self).gather_stage(harvest_job, 'latin9')

    def import_stage(self, harvest_object):
        try:
            package = json.loads(harvest_object.content)
            self.amend_package(package)
        except ValueError, error:
            self._save_object_error(str(error), harvest_object)
            LOGGER.error('Bfj: ' + str(error))
            return False

        harvest_object.content = json.dumps(package)
        return super(BfjHarvester, self).import_stage(harvest_object)
