#!/usr/bin/python
# -*- coding: utf8 -*-

import unittest
import datetime
from ckanext.govdatade.validators.link_checker import LinkChecker
from ckanext.govdatade.validators.schema_checker import SchemaChecker
from ckanext.govdatade.util import amend_portal
from ckanext.govdatade.util import boolize_config_value
from ckanext.govdatade.util import generate_link_checker_data
from ckanext.govdatade.util import generate_schema_checker_data
from ckanext.govdatade.util import normalize_action_dataset
from ckanext.govdatade.config import config

class UtilTest(unittest.TestCase):

    def setUp(self):
        self.schema_checker = SchemaChecker(config)
        self.schema_checker.redis_client.flushdb()
        self.link_checker = LinkChecker(config)
        self.link_checker.redis_client.flushdb()

    def tearDown(self):
        self.schema_checker.redis_client.flushdb()
        self.link_checker.redis_client.flushdb()

    def test_amend_portal_works_as_expected(self):
        self.assertEqual('abc', amend_portal('abc'))
        self.assertEqual('A------Z', amend_portal('A:/.&?=Z'))

    def test_boolize(self):
        true_values = ["true", "on", 1, "1", "ON", "True", True]

        for value in true_values:
            self.assertTrue(boolize_config_value(value))

        false_values = ["false", "off", 0, "0", "Off", "FALSE", False]


        for value in false_values:
            self.assertFalse(boolize_config_value(value))

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

    def test_generate_link_checker_data_empty_urls_dict(self):
        # prepare
        general = {'num_datasets': 0}
        self.link_checker.redis_client.set('general', general)

        dataset_id = 1
        dataset_name = 'example'
        portal = 'portal1'
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'urls': {},
            'metadata_original_portal': portal
        }
        self.link_checker.redis_client.set(dataset_id, initial_record)
        data = {}

        # execute
        generate_link_checker_data(data)

        # verify
        self.assertDictEqual(data['linkchecker'], {'broken': 0, 'working': 0})
        self.assertDictEqual(data['portals'], {})
        self.assertDictEqual(data['entries'], {})

    def test_generate_link_checker_data(self):
        # prepare
        general = {'num_datasets': 1}
        self.link_checker.redis_client.set('general', general)

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
        self.link_checker.redis_client.set(dataset_id, initial_record)
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

    def test_generate_schema_checker_data_empty_schema_dict(self):
        # prepare
        general = {'num_datasets': 0}
        self.schema_checker.redis_client.set('general', general)

        dataset_id = 1
        dataset_name = 'example'
        dataset_maintainer = 'maintainer1'
        portal = 'portal1'
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'maintainer': dataset_maintainer,
            'schema': {},
            'metadata_original_portal': portal
        }
        self.schema_checker.redis_client.set(dataset_id, initial_record)
        data = {}

        # execute
        generate_schema_checker_data(data)

        # verify
        self.assertDictEqual(data['schemachecker'], {'broken': 0, 'working': 0})
        self.assertDictEqual(data['schema']['portal_statistic'], {})
        self.assertDictEqual(data['schema']['rule_statistic'], {})
        self.assertDictEqual(data['schema']['broken_rules'], {})

    def test_generate_schema_checker_data(self):
        # prepare
        general = {'num_datasets': 1}
        self.schema_checker.redis_client.set('general', general)

        dataset_id = 1
        dataset_name = 'example'
        dataset_maintainer = 'maintainer1'
        portal = 'portal1'
        url = 'http://example.com/dataset/1'
        status = 404
        date = datetime.datetime(2014, 1, 1)
        date_string = date.strftime("%Y-%m-%d")
        broken_rules = [
                ['rule1', 'message1'],
                ['rule2', 'message2'],
                ['rule3', 'message3']
            ]
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'maintainer': dataset_maintainer,
            'schema': broken_rules,
            'metadata_original_portal': portal
        }
        self.schema_checker.redis_client.set(dataset_id, initial_record)
        data = {}

        # execute
        generate_schema_checker_data(data)

        # verify
        self.assertDictEqual(data['schemachecker'], {'broken': 1, 'working': 0})
        self.assertEqual(data['schema']['portal_statistic'][portal], 1)
        self.assertEqual(data['schema']['rule_statistic'][broken_rules[0][0]], 1)
        self.assertEqual(data['schema']['rule_statistic'][broken_rules[1][0]], 1)
        self.assertEqual(data['schema']['rule_statistic'][broken_rules[2][0]], 1)
        self.assertDictEqual(
            data['schema']['broken_rules'][portal][dataset_id],
            {
             'name': dataset_name,
             'maintainer': dataset_maintainer,
             'broken_rules': broken_rules
            }
        )
