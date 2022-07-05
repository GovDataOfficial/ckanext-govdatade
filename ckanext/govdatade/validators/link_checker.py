#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Module for checking link availability of CKAN resources.
'''
from datetime import datetime

import socket
import logging
import ast
import redis
import requests


class LinkChecker(object):

    '''
    Class providing the actual link check logic.
    '''

    HEADERS = {'User-Agent': 'govdata-linkchecker'}
    SCHEMA_RECORD_KEY = 'urls'
    default_timeout = 15.0

    def __init__(self, config):

        self.logger = logging.getLogger(
            'ckanext.govdatade.reports.validators.linkchecker'
        )

        self.redis_client = redis.StrictRedis(
            host=config.get('ckanext.govdata.validators.redis.host'),
            port=int(config.get('ckanext.govdata.validators.redis.port')),
            db=int(config.get('ckanext.govdata.validators.redis.database'))
        )

        timeout_config_string = config.get('ckanext.govdata.validators.linkchecker.timeout')
        if timeout_config_string is not None:
            try:
                timeout_config = int(timeout_config_string)
                self.default_timeout = timeout_config
                self.logger.debug('Using timeout (s): %s', self.default_timeout)
            except ValueError:
                self.logger.debug('Timeout in configuration is not an integer: %s', timeout_config_string)
                self.logger.debug('Using default timeout (s): %s', self.default_timeout)

    def process_record(self, dataset):
        '''
        Checking a single datasets URLs for availability
        '''
        dataset_id = dataset['id']
        self.logger.debug('Dataset id: %s', dataset_id)
        delete = False
        active_urls = []

        for resource in dataset['resources']:
            url = resource['url']
            self.logger.debug(u'Resource URL: %s', url)
            active_urls.append(url)
            try:
                code = self.validate(url)
                self.logger.debug(u'HTTP status code for %s: %s', url, code)
                if self.is_available(code):
                    self.record_success(dataset_id, url)
                else:
                    delete = delete or self.record_failure(
                        dataset, url, code
                    )
            except requests.exceptions.Timeout:
                delete = delete or self.record_failure(
                    dataset, url, 'Timeout'
                )
            except requests.exceptions.TooManyRedirects:
                delete = delete or self.record_failure(
                    dataset, url, 'Redirect Loop'
                )
            except requests.exceptions.SSLError:
                delete = delete or self.record_failure(
                    dataset, url, 'SSL Error'
                )
            except requests.exceptions.RequestException as request_error:
                if request_error is None:
                    delete = delete or self.record_failure(
                        dataset, url, 'Unknown Request Error'
                    )
                else:
                    delete = delete or self.record_failure(
                        dataset, url, str(request_error)
                    )
            except socket.timeout:
                delete = delete or self.record_failure(
                    dataset, url, 'Timeout'
                )
            except ValueError as value_error:
                self.logger.debug('Value error: %s', value_error)
                self.record_success(dataset_id, url)
            except Exception as exception:
                self.logger.debug('Unknown Error: %s', exception)
                #In case of an unknown exception, change nothing
                delete = delete or self.record_failure(
                    dataset, url, 'Unknown Error'
                )
        # Delete no more existent urls in dataset
        self.delete_deprecated_urls(dataset_id, active_urls)
        return delete

    def check_dataset(self, dataset):
        '''
        Checks the URLs (resources) of a given dataset
        '''
        datasets = []
        for resource in dataset['resources']:
            url = resource['url']
            datasets.append(self.validate(url))
        return datasets

    def validate(self, url):
        '''
        Validates a given URL by making a request against it
        and returning it's HTTP status code.
        '''
        self.logger.debug(u'URL: %s', url)

        self.logger.debug(u'Calling with HEAD method...')
        response = requests.head(
            url,
            allow_redirects=True,
            timeout=self.default_timeout,
            headers=self.HEADERS,
            verify=False
        )

        if self.has_redirection_to_404_page(response):
            self.logger.debug(
                'Redirect ends in HTTP status code %s', str(requests.codes.not_found)
            )
            return requests.codes.not_found
        # if method HEAD is not allowed try again with http method GET
        elif self.is_method_not_allowed(response.status_code):
            self.logger.debug(u'HEAD method seems is not supported. Calling with GET method...')
            response = requests.get(
                url,
                allow_redirects=True,
                timeout=self.default_timeout,
                headers=self.HEADERS,
                verify=False
            )

        self.logger.debug(
            'HTTP status code: %s', str(response.status_code)
        )
        return response.status_code

    @classmethod
    def has_redirection_to_404_page(cls, response):
        '''
        Utility method for determining if there's a redirection
        to a 404 page in the response.
        '''
        if len(response.history) > 0:
            for redirect_response in response.history:
                if 'location' in redirect_response.headers:
                    if '404' in redirect_response.headers['location']:
                        return True

        return False

    @classmethod
    def is_method_not_allowed(cls, status_code):
        '''
        Utility method for determining if the http method HEAD is not allowed by the server.
        Some server returns the http status 405 "method not allowed" (correct answer), but
        some server answer with the status code 400 "bad request".
        '''
        return (status_code == requests.codes.method_not_allowed) \
            | (status_code == requests.codes.bad_request)

    @classmethod
    def is_available(cls, response_code):
        '''
        Utility method for determining the availability from
        a HTTP status code
        '''
        return response_code >= 200 and response_code < 300

    def record_failure(self, dataset, url, status, date=datetime.now().date()):
        '''
        Adds a non available URL to the Redis dataset
        '''

        self.logger.debug('Record failure with error (code) %s.', str(status))

        portal = None
        if 'extras' in dataset and \
           'metadata_harvested_portal' in dataset['extras']:
            portal = dataset['extras']['metadata_harvested_portal']

        dataset_id = dataset['id']
        dataset_name = dataset['name']
        dataset_maintainer_email = dataset['maintainer_email'] if 'maintainer_email' in dataset else ''
        dataset_maintainer = dataset['maintainer'] if 'maintainer' in dataset else ''
        record = unicode(self.redis_client.get(dataset_id))
        record = self.evaluate_record(record)

        initial_url_record = {
            'status': status,
            'date': date.strftime("%Y-%m-%d"),
            'strikes': 1
        }

        if record is not None:
            record['name'] = dataset_name
            record['maintainer'] = dataset_maintainer
            record['maintainer_email'] = dataset_maintainer_email
            record['metadata_original_portal'] = portal
            self.redis_client.set(dataset_id, record)

        # Record is not known yet
        if record is None:
            record = {
                'id': dataset_id,
                'name': dataset_name,
                'maintainer': dataset_maintainer,
                'maintainer_email': dataset_maintainer_email,
                self.SCHEMA_RECORD_KEY: {}
            }

            record[self.SCHEMA_RECORD_KEY][url] = initial_url_record
            record['metadata_original_portal'] = portal
            self.redis_client.set(dataset_id, record)

        # Record is known, but only with schema errors
        elif self.SCHEMA_RECORD_KEY not in record:
            record[self.SCHEMA_RECORD_KEY] = {}
            record[self.SCHEMA_RECORD_KEY][url] = initial_url_record
            self.redis_client.set(dataset_id, record)
        # Record is known, but not that particular URL (Resource)
        elif url not in record[self.SCHEMA_RECORD_KEY]:
            record[self.SCHEMA_RECORD_KEY][url] = initial_url_record
            self.redis_client.set(dataset_id, record)

        # Record and URL are known, increment Strike counter if 1+ day(s) have
        # passed since the last check
        else:
            url_entry = record[self.SCHEMA_RECORD_KEY][url]
            last_updated = datetime.strptime(url_entry['date'], "%Y-%m-%d")

            try:
                last_updated = datetime.combine(
                    last_updated.date(), datetime.min.time())

                if last_updated < date:
                    url_entry['status'] = status
                    url_entry['strikes'] += 1
                    url_entry['date'] = date.strftime("%Y-%m-%d")
                    self.redis_client.set(dataset_id, record)
            except TypeError:

                last_updated = last_updated.date()

                if last_updated < date:
                    url_entry['status'] = status
                    url_entry['strikes'] += 1
                    url_entry['date'] = date.strftime("%Y-%m-%d")
                    self.redis_client.set(dataset_id, record)

        delete = record[self.SCHEMA_RECORD_KEY][url]['strikes'] >= 100

        return delete

    def record_success(self, dataset_id, url):
        '''
        Deletes or adds URL's from Redis dataset records
        '''
        record = self.redis_client.get(dataset_id)

        if record is not None:
            record = self.evaluate_record(record)

            # Remove URL entry due to a valid URL
            if record.get(self.SCHEMA_RECORD_KEY):
                record[self.SCHEMA_RECORD_KEY].pop(url, None)
                self.redis_client.set(dataset_id, record)

    def delete_deprecated_urls(self, dataset_id, active_urls):
        '''
        Deletes deprecated URL's from Redis dataset record
        '''
        record = self.redis_client.get(dataset_id)

        if record is not None:
            record = self.evaluate_record(record)

            if self.SCHEMA_RECORD_KEY in record:
                deprecated_urls = []
                for candidate in record[self.SCHEMA_RECORD_KEY]:
                    if candidate not in active_urls:
                        deprecated_urls.append(candidate)

                # Remove deprecated URL entries
                for to_remove in deprecated_urls:
                    self.logger.debug(
                        'Delete deprecated url %s in dataset %s', to_remove, dataset_id)
                    record[self.SCHEMA_RECORD_KEY].pop(to_remove, None)

                self.redis_client.set(dataset_id, record)

    def get_records(self):
        '''
        Returns the dataset records from Redis
        '''
        records = []
        for dataset_id in self.redis_client.keys('*'):
            if dataset_id == 'general' or dataset_id.startswith('harvest_object_id', 0):
                continue
            try:
                records.append(
                    ast.literal_eval(self.redis_client.get(dataset_id))
                )
            except ValueError:
                self.logger.error('Data set error: %s', dataset_id)

        return records

    def evaluate_record(self, record):
        '''
        Evaluate the record
        '''
        try:
            record = ast.literal_eval(unicode(record))
        except ValueError:
            self.logger.error('Redis dataset record evaluation error: %s', record)
        return record
