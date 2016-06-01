from ckanext.govdatade.validators.schema_checker import SchemaChecker
from mock import Mock
from ckanext.govdatade.config import config

import unittest


class TestSchemaChecker(unittest.TestCase):

    def setUp(self):
        self.schema_checker = SchemaChecker(config, schema = None)
        self.schema_checker.redis_client.flushdb()

    def tearDown(self):
        self.schema_checker.redis_client.flushdb()

    def test_get_records_works_as_expected(self):
        self.assertEqual(self.schema_checker.get_records(), [])

        self.schema_checker.redis_client.keys = Mock(return_value=['general'])
        self.assertEqual(self.schema_checker.get_records(), [])
        self.schema_checker.redis_client.keys.assert_called_once_with('*')

        self.schema_checker.redis_client.keys = Mock(return_value=['general', 'abc'])
        self.schema_checker.redis_client.get = Mock(
            return_value="{'metadata_original_portal': u'http://suche.transparenz.hamburg.de/'}"
        )

        expected_records = [
            {'metadata_original_portal': u'http://suche.transparenz.hamburg.de/'}
        ]

        self.assertEqual(self.schema_checker.get_records(), expected_records)
        self.schema_checker.redis_client.keys.assert_called_once_with('*')
        self.schema_checker.redis_client.get.assert_called_once_with('abc')
