import datetime
import json
import unittest

import httpretty
from ckan.plugins import toolkit as tk
from ckanext.govdatade.validators.link_checker import LinkChecker
from mock import Mock, patch

class TestLinkChecker(unittest.TestCase):

    def setUp(self):
        self.link_checker = LinkChecker(tk.config)
        self.link_checker.redis_client.flushdb()

    def tearDown(self):
        self.link_checker.redis_client.flushdb()

    def test_redis(self):
        assert self.link_checker.redis_client.ping()

    def test_is_available_200(self):
        assert self.link_checker.is_available(200)

    def test_is_available_404(self):
        assert not self.link_checker.is_available(400)

    @patch.dict("ckan.plugins.toolkit.config", {'ckanext.govdata.validators.linkchecker.timeout': '68'})
    def test_default_timeout_valid_integer(self):
        link_checker = LinkChecker(tk.config)
        self.assertTrue(link_checker.default_timeout == 68)

    @patch.dict("ckan.plugins.toolkit.config", {'ckanext.govdata.validators.linkchecker.timeout': 'InvalidInteger'})
    def test_default_timeout_invalid_integer(self):
        link_checker = LinkChecker(tk.config)
        # Default value of timeout
        self.assertTrue(link_checker.default_timeout == 15)

    @patch.dict("ckan.plugins.toolkit.config", {})
    def test_default_timeout_None(self):
        link_checker = LinkChecker(tk.config)
        # Default value of timeout
        self.assertTrue(link_checker.default_timeout == 15)

    def test_legacy_data_loading(self):
        testdict = {
            'a test': 'dict',
            'value': 5
        }
        parsed_json = self.link_checker.load_redis_data(json.dumps(testdict))
        parsed_str = self.link_checker.load_redis_data(str(testdict))

        self.assertDictEqual(testdict, parsed_json)
        self.assertDictEqual(testdict, parsed_str)

        # invalid data should still lead to failures
        with self.assertRaises(Exception):
            self.link_checker.load_redis_data("?!invalid{")
        # python code should also not be accepted
        with self.assertRaises(Exception):
            self.link_checker.load_redis_data("content = str(3)")

    def test_record_success(self):
        dataset_id = '1'
        url = 'https://www.example.com'

        self.link_checker.record_success(dataset_id, url)

        entry = self.link_checker.redis_client.get(dataset_id)
        assert entry is None

    def test_dataset_beginning_with_harvest_object_id_is_filtered(self):
        self.link_checker.redis_client.set('harvest_object_id:b6d207e2-8e28-472a-95b0-2c79405ecc1f', '2015-12-02 14:15:34.793933')

        records = self.link_checker.get_records()

        self.assertEqual(records, [])

        self.link_checker.redis_client.set('key_for_json_structure', '{"abc": "def"}')

        records = self.link_checker.get_records()
        self.assertTrue(len(records) == 1)

    @httpretty.activate
    def test_process_record(self):
        url1 = 'http://example.com/dataset/1'
        url2 = 'http://example.com/dataset/2'

        httpretty.register_uri(httpretty.HEAD, url1, status=200)
        httpretty.register_uri(httpretty.HEAD, url2, status=404)

        dataset_id = '1'
        dataset = {
            'id': dataset_id,
            'resources': [{'url': url1}, {'url': url2}],
            'name': 'example'
        }

        self.link_checker.process_record(dataset)
        record = json.loads(self.link_checker.redis_client.get(dataset_id))

        self.assertNotIn(url1, record['urls'])
        self.assertEqual(record['urls'][url2]['strikes'], 1)

    @httpretty.activate
    def test_process_record_deprecated_urls(self):
        # prepare (1)
        url1 = 'http://example.com/dataset/1'
        url2 = 'http://example.com/dataset/2'

        httpretty.register_uri(httpretty.HEAD, url1, status=404)
        httpretty.register_uri(httpretty.HEAD, url2, status=404)

        dataset_id = '1'
        dataset = {
            'id': dataset_id,
            'resources': [{'url': url1}, {'url': url2}],
            'name': 'example'
        }

        # execute (1)
        self.link_checker.process_record(dataset)
        
        # verify (1)
        record = json.loads(self.link_checker.redis_client.get(dataset_id))
        self.assertEqual(record['urls'][url1]['strikes'], 1)
        self.assertEqual(record['urls'][url1]['status'], 404)
        self.assertEqual(record['urls'][url2]['strikes'], 1)
        self.assertEqual(record['urls'][url2]['status'], 404)
        
        # prepare (2)
        dataset.get('resources').pop(0) # removes entry with url1

        # execute (1)
        self.link_checker.process_record(dataset)
        
        # verify (1)
        record = json.loads(self.link_checker.redis_client.get(dataset_id))
        self.assertNotIn(url1, record['urls'])
        # Comment within method record_failure in link_checker.py:
        # Record and URL are known, increment Strike counter if 1+ day(s) have
        # passed since the last check
        self.assertEqual(record['urls'][url2]['strikes'], 1) # normally expected 2
        self.assertEqual(record['urls'][url2]['status'], 404)

    @httpretty.activate
    def test_process_record_deprecated_urls_all_active(self):
        # prepare
        url1 = 'http://example.com/dataset/1'

        httpretty.register_uri(httpretty.HEAD, url1, status=200)

        dataset_id = '1'
        dataset = {
            'id': dataset_id,
            'resources': [{'url': url1}],
            'name': 'example'
        }

        # execute
        self.link_checker.process_record(dataset)
        
        # verify
        record = self.link_checker.redis_client.get(dataset_id)
        self.assertIsNone(record)

    @httpretty.activate
    def test_process_record_deprecated_urls_all_active_with_existent_record(self):
        # prepare
        url1 = 'http://example.com/dataset/1'

        httpretty.register_uri(httpretty.HEAD, url1, status=200)

        dataset_id = '1'
        dataset_name = 'example'
        dataset = {
            'id': dataset_id,
            'resources': [{'url': url1}],
            'name': dataset_name
        }
        
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'schema': {}
        }
        self.link_checker.redis_client.set(dataset_id, json.dumps(initial_record))

        # execute
        self.link_checker.process_record(dataset)
        
        # verify
        record_actual = json.loads(self.link_checker.redis_client.get(dataset_id))
        self.assertDictEqual(record_actual, initial_record)

    @httpretty.activate
    def test_check_url_200(self):
        httpretty.HTTPretty.allow_net_connect = False
        url = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url, status=200)

        expectation = 200
        assert self.link_checker.validate(url) == expectation
        self.assertTrue(httpretty.has_request())

    @httpretty.activate
    def test_check_url_404(self):
        httpretty.HTTPretty.allow_net_connect = False
        url = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url, status=404)

        expectation = 404
        assert self.link_checker.validate(url) == expectation
        self.assertTrue(httpretty.has_request())

    @httpretty.activate
    def test_check_url_301(self):
        httpretty.HTTPretty.allow_net_connect = False
        url = 'http://example.com/dataset/1'
        target = 'http://www.example.com/dataset/1'

        httpretty.register_uri(httpretty.HEAD, target, status=200)
        httpretty.register_uri(httpretty.HEAD, url,
                               status=301, location=target)

        expectation = 200
        assert self.link_checker.validate(url) == expectation
        self.assertTrue(httpretty.has_request())

    @httpretty.activate
    def test_check_url_405(self):
        httpretty.HTTPretty.allow_net_connect = False
        url = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url, status=405)
        httpretty.register_uri(httpretty.GET, url, status=200)

        expectation = 200
        assert self.link_checker.validate(url) == expectation
        self.assertTrue(httpretty.has_request())

    @httpretty.activate
    def test_check_url_400(self):
        httpretty.HTTPretty.allow_net_connect = False
        url = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url, status=400)
        httpretty.register_uri(httpretty.GET, url, status=200)

        expectation = 200
        assert self.link_checker.validate(url) == expectation
        self.assertTrue(httpretty.has_request())

    @httpretty.activate
    def test_check_url_statistik_sachsen(self):
        httpretty.HTTPretty.allow_net_connect = False
        url = 'http://statistik.sachsen.de/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url, status=200)

        expectation = 200
        assert self.link_checker.validate(url) == expectation
        self.assertTrue(httpretty.has_request())

    @httpretty.activate
    def test_check_dataset(self):
        url1 = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url1, status=200)

        url2 = 'http://example.com/dataset/2'
        httpretty.register_uri(httpretty.HEAD, url2, status=404)

        url3 = 'http://example.com/dataset/3'
        httpretty.register_uri(httpretty.HEAD, url3, status=200)

        dataset = {'id': '1',
                   'name': 'example',
                   'resources': [{'url': url1}, {'url': url2}, {'url': url3}]}

        assert self.link_checker.check_dataset(dataset) == [200, 404, 200]

    def test_record_failure(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404
        portal = 'example.com'
        dataset = {'id': dataset_id, 'name': 'example', 'extras': {'metadata_harvested_portal': portal}}

        date = datetime.datetime(2014, 1, 1)
        date_string = date.strftime("%Y-%m-%d")

        self.link_checker.record_failure(dataset, url, status, date=date)
        actual_record = json.loads(self.link_checker.redis_client.get(dataset_id))

        expected_record = {
            'id': dataset_id,
            'name': 'example',
            'maintainer': '',
            'maintainer_email': '',
            'urls': {
                url: {
                    'status': 404,
                    'date': date_string,
                    'strikes': 1
            }},
           'metadata_original_portal': portal
        }

        assert actual_record == expected_record

    def test_record_failure_second_time_same_date(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404
        dataset = {'id': dataset_id, 'name': 'example'}

        date = datetime.datetime(2014, 1, 1)
        date_string = date.strftime("%Y-%m-%d")

        self.link_checker.record_failure(dataset, url, status, date=date)

        # Second time to test that the strikes counter has not incremented
        self.link_checker.record_failure(dataset, url, status, date=date)

        actual_record = json.loads(self.link_checker.redis_client.get(dataset_id))

        expected_record = {
            'metadata_original_portal': None, 'id': dataset_id, 'maintainer': '', 'maintainer_email': '',
            'urls': {
                url: {'status': status, 'date': date_string, 'strikes': 1}
            },
            'name': 'example'}

        assert actual_record == expected_record

    def test_record_failure_second_time_different_date(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404
        portal = 'example.com'
        dataset = {'id': dataset_id, 'name': 'example', 'extras': {'metadata_harvested_portal': portal}}

        date = datetime.datetime(2014, 1, 1)
        self.link_checker.record_failure(dataset, url, status, date=date)

        date = datetime.datetime(2014, 1, 2)
        date_string = date.strftime("%Y-%m-%d")

        self.link_checker.record_failure(dataset, url, status, date=date)

        actual_record = json.loads(self.link_checker.redis_client.get(dataset_id))

        expected_record = {
            'metadata_original_portal': portal,
            'id': dataset_id,
            'maintainer': '',
            'maintainer_email': '',
            'urls': {
                url: {
                    'status': status,
                    'date': date_string,
                    'strikes': 2
                }
            },
            'name': 'example'
        }

        self.assertEqual(actual_record, expected_record)

    def test_record_success_after_failure(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404
        dataset = {'id': dataset_id, 'name': 'example'}

        date = datetime.datetime(2014, 1, 1)
        date_string = date.strftime("%Y-%m-%d")

        self.link_checker.record_failure(dataset, url, status, date=date)

        actual_record = json.loads(self.link_checker.redis_client.get(dataset_id))

        expected_record = {
            'metadata_original_portal': None,
            'id': dataset_id,
            'maintainer': '',
            'maintainer_email': '',
            'urls': {
                url: {
                    'status': status,
                    'date': date_string,
                    'strikes': 1
                }
            },
            'name': 'example'
        }

        self.assertEqual(actual_record, expected_record)

        self.link_checker.record_success(dataset_id, url)
        # Expected after record success
        expected_record.get('urls').pop(url, None)
        actual_record = json.loads(self.link_checker.redis_client.get(dataset_id))
        self.assertEqual(actual_record, expected_record)

    def test_url_success_after_failure(self):
        dataset_id = '1'
        url1 = 'https://www.example.com/dataset/1'
        url2 = 'https://www.example.com/dataset/2'
        portal = 'example.com'
        dataset = {'id': dataset_id, 'name': 'example', 'extras': {'metadata_harvested_portal': portal}}

        date = datetime.datetime(2014, 1, 1)
        date_string = date.strftime("%Y-%m-%d")

        self.link_checker.record_failure(dataset, url1, 404, date=date)
        self.link_checker.record_failure(dataset, url2, 404, date=date)

        actual_record = json.loads(self.link_checker.redis_client.get(dataset_id))

        expected_record = {
            'metadata_original_portal': portal,
            'id': dataset_id,
            'maintainer': '',
            'maintainer_email': '',
            'urls':  {
                url1: {
                    'status': 404,
                    'date': date_string,
                    'strikes': 1
                },
                url2: {
                    'status': 404,
                    'date': date_string,
                    'strikes': 1
                }
            },
            'name': 'example'
        }

        self.assertEqual(actual_record, expected_record)
        self.link_checker.record_success(dataset_id, url1)

        actual_record = json.loads(self.link_checker.redis_client.get(dataset_id))

        expected_record = {
            'metadata_original_portal': portal,
            'id': dataset_id,
            'maintainer': '',
            'maintainer_email': '',
            'urls':  {
                url2: {
                    'status': 404,
                    'date': date_string,
                    'strikes': 1
                }
            },
            'name': 'example'
        }

        self.assertEqual(actual_record, expected_record)

    def test_get_records_works_as_expected(self):
        # (1)
        self.assertEqual(self.link_checker.get_records(), [])

        # (2)
        self.link_checker.redis_client.keys = Mock(return_value=['general'])
        self.assertEqual(self.link_checker.get_records(), [])
        self.link_checker.redis_client.keys.assert_called_once_with('*')

        # (3)
        self.link_checker.redis_client.keys = Mock(return_value=['general', 'abc'])
        test_dict = {'metadata_original_portal': u'http://suche.transparenz.hamburg.de/'}
        self.link_checker.redis_client.get = Mock(
            return_value=json.dumps(test_dict)
        )

        self.assertEqual(self.link_checker.get_records(), [test_dict])
        self.link_checker.redis_client.keys.assert_called_once_with('*')
        self.link_checker.redis_client.get.assert_called_once_with('abc')

        # (4)
        self.link_checker.redis_client.keys = Mock(return_value=['general'])
        self.link_checker.redis_client.get.reset_mock()

        self.assertEqual(self.link_checker.get_records(), [])
        self.link_checker.redis_client.keys.assert_called_once_with('*')
        self.link_checker.redis_client.get.assert_not_called()
