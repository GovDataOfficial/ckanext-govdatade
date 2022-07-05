#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import json
import unittest

from pylons import config

from ckanext.govdatade.extras import Extras
from ckanext.govdatade.util import amend_portal
from ckanext.govdatade.util import fix_group_dict_list
from ckanext.govdatade.util import generate_link_checker_data
from ckanext.govdatade.util import get_group_dict
from ckanext.govdatade.util import normalize_action_dataset
from ckanext.govdatade.util import normalize_api_dataset
from ckanext.govdatade.util import remove_group_dict
from ckanext.govdatade.validators.link_checker import LinkChecker


class UtilTest(unittest.TestCase):

    def setUp(self):
        self.link_checker = LinkChecker(config)
        self.link_checker.redis_client.flushdb()

    def tearDown(self):
        self.link_checker.redis_client.flushdb()

    def test_amend_portal_works_as_expected(self):
        self.assertEqual('abc', amend_portal('abc'))
        self.assertEqual('A------Z', amend_portal('A:/.&?=Z'))

    def test_get_group_dict_name_not_empty(self):
        # prepare
        group_name = 'group'

        # execute
        result = get_group_dict(group_name)

        # verify
        self.assertDictEqual(result, {'id': group_name, 'name': group_name})

    def test_get_group_dict_name_empty(self):
        # prepare
        group_name = ''

        # execute
        result = get_group_dict(group_name)

        # verify
        self.assertDictEqual(result, {})

    def test_remove_group_dict(self):
        # prepare
        group_name = 'group1-name'
        group_dict_list = [{'id': 'group1', 'name': 'group1-name'}, {'id': 'group2', 'name': 'group2-name'}]

        # execute
        result = remove_group_dict(group_dict_list, group_name)

        # verify
        self.assertListEqual(result, [{'id': 'group2', 'name': 'group2-name'}])

    def test_remove_group_dict_name_empty(self):
        # prepare
        group_name = ''
        group_dict_list = [{'id': 'group1', 'name': 'group1-name'}, {'id': 'group2', 'name': 'group2-name'}]

        # execute
        result = remove_group_dict(group_dict_list, group_name)

        # verify
        self.assertListEqual(result,
            [{'id': 'group1', 'name': 'group1-name'}, {'id': 'group2', 'name': 'group2-name'}])

    def test_remove_group_dict_name_check_only_name_attribut(self):
        # prepare
        group_name = 'group1'
        group_dict_list = [{'id': 'group1', 'name': 'group1-name'}, {'id': 'group2', 'name': 'group2-name'}]

        # execute
        result = remove_group_dict(group_dict_list, group_name)

        # verify
        self.assertListEqual(result,
            [{'id': 'group1', 'name': 'group1-name'}, {'id': 'group2', 'name': 'group2-name'}])

    def test_fix_group_dict_list(self):
        # prepare
        group_dict_list = [{'id': 'group1', 'name': 'group1-name'}, {'id': 'group2', 'version': '12'}]

        # execute
        fix_group_dict_list(group_dict_list)

        # verify
        self.assertListEqual(group_dict_list,
            [{'id': 'group1-name', 'name': 'group1-name'}, {'id': 'group2', 'version': '12'}])

    def test_normalize_action_dataset(self):
        # prepare
        dataset_id = 1
        dataset_name = 'example'
        dataset = {
            'id': dataset_id,
            'name': dataset_name,
            'groups': [{'name': 'group1'}, {'name': 'group2'}],
            'tags': [{'name': 'tag1'}, {'name': 'tag2'}],
            'extras': [
                        {'key': 'temporal_granularity_factor', 'value': '1'},
                        {'key': 'anotherKey', 'value': 'anotherValue'}
                      ]
        }

        # execute
        normalize_action_dataset(dataset)

        # verify
        extras_expected = {
                    'temporal_granularity_factor': 1,
                    'anotherKey': 'anotherValue'
                }
        self.assertDictEqual(dataset['extras'], extras_expected)
        self.assertListEqual(dataset['groups'], ['group1', 'group2'])
        self.assertListEqual(dataset['tags'], ['tag1', 'tag2'])

    def test_normalize_api_dataset(self):
        # API version 1 dataset
        dataset = {
            'id': 1,
            'name': 'some_name',
            'groups': ['wirtschaft_arbeit', 'bevoelkerung'],
            'tags': ['tag1', 'tag2', 'tag3'],
            'extras': {
                'contacts': [{
                    'url': '',
                    'address': '',
                    'role': 'autor',
                    'email': 'https://www.destatis.de/kontakt',
                    'name': 'Statistisches Bundesamt'
                }],
                'dates': [{
                    'date': '2016-08-19',
                    'role': 'veroeffentlicht'
                }],
                'terms_of_use': {
                    'license_id': 'dl-de-by-2.0',
                    'license_url': 'some-url'
                },
                'metadata_modified': '2016-08-19T08:20:01.501641 ',
                'spatial_reference': {'nuts': 'DE'},
                'geographical_granularity': 'bund',
                'temporal_granularity': 'monat',
            }
        }

        normalize_api_dataset(dataset)

        expected_dataset = {
            'id': 1,
            'name': 'some_name',
            'groups': [{'id': 'wirtschaft_arbeit', 'name': 'wirtschaft_arbeit'},
                       {'id': 'bevoelkerung', 'name': 'bevoelkerung'}],
            'tags': [{'name': 'tag1'}, {'name': 'tag2'}, {'name': 'tag3'}]
        }

        self.assertListEqual(
            expected_dataset['groups'],
            dataset['groups']
        )
        self.assertListEqual(
            expected_dataset['tags'],
            dataset['tags']
        )

        # Use extras to avoid dict key value order issues
        # when asserting
        extras = Extras(dataset['extras'])
        self.assertEquals("{\"nuts\": \"DE\"}", extras.value('spatial_reference'))

        expected_terms_of_use = json.dumps({
            "license_id": "dl-de-by-2.0",
            "license_url": "some-url"
        })

        self.assertEquals(expected_terms_of_use, extras.value('terms_of_use'))

        expected_contacts = json.dumps([{
            "url": "",
            "address": "",
            "role": "autor",
            "email": "https://www.destatis.de/kontakt",
            "name": "Statistisches Bundesamt",
        }])

        self.assertEquals(expected_contacts, extras.value('contacts'))
        
        self.assertEquals('2016-08-19T08:20:01.501641 ', extras.value('metadata_modified'))
        
        self.assertEquals("bund", extras.value('geographical_granularity'))

    def test_generate_link_checker_data_empty_urls_dict(self):
        # prepare
        general = {'num_datasets': 0}
        self.link_checker.redis_client.set('general', json.dumps(general))

        dataset_id = 1
        dataset_name = 'example'
        portal = 'portal1'
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'urls': {},
            'metadata_original_portal': portal
        }
        self.link_checker.redis_client.set(dataset_id, json.dumps(initial_record))
        data = {}

        # execute
        generate_link_checker_data(data)

        # verify
        self.assertDictEqual(data['linkchecker'], {'broken': 0, 'working': 0})
        self.assertDictEqual(data['portals'], {})
        self.assertDictEqual(data['entries'], {})

    def _run_test_generate_data(self, serializer_function):
        """
        Generalized helper to check data generation, supports a generic function to serialize
        dicts into redis as an argument.
        """
        # prepare
        general = {'num_datasets': 1}
        self.link_checker.redis_client.set('general', serializer_function(general))

        dataset_id = 1
        dataset_name = 'example'
        portal = 'portal1'
        url = 'http://example.com/dataset/1'
        status = 404
        date = datetime.datetime(2014, 1, 1)
        date_string = date.strftime("%Y-%m-%d")
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'urls': {
                url: {
                    'status': status,
                    'date': date_string,
                    'strikes': 1
                }
            },
           'metadata_original_portal': portal
        }
        self.link_checker.redis_client.set(dataset_id, serializer_function(initial_record))
        data = {}

        # execute
        generate_link_checker_data(data)

        # verify
        self.assertDictEqual(data['linkchecker'], {'broken': 1, 'working': 0})
        self.assertEqual(data['portals'][portal], 1)
        self.assertListEqual(
            data['entries'][portal],
            [{
                'metadata_original_portal': portal,
                'id': dataset_id,
                'urls': {
                         url: {
                               'status': 'HTTP 404',
                               'date': '2014-01-01',
                               'strikes': 1}
                         },
                'name': 'example'
             }]
        )

    def test_generate_link_checker_data(self):
        # use JSON-serialized data by default
        self._run_test_generate_data(json.dumps)

    def test_link_checker_data_legacy(self):
        # older entries are just direct strings of dicts, they should be supported as well
        self._run_test_generate_data(str)