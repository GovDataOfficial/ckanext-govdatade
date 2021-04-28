import unittest

from pylons import config

from ckanext.govdatade.commands.schemachecker import SchemaChecker as SchemaCheckerCommand
from ckanext.govdatade.validators.schema_checker import SchemaChecker


class TestSchemaChecker(unittest.TestCase):

    def setUp(self):
        self.schemachecker = SchemaCheckerCommand(name = 'SchemaCheckerTest')
        self.schemachecker.schema = None
        self.schema_checker = SchemaChecker(config, schema = None)
        self.schema_checker.redis_client.flushdb()

    def tearDown(self):
        self.schema_checker.redis_client.flushdb()

    def test_delete_deprecated_violations_no_more_active_without_urls_dict(self):
        # prepare
        dataset_id = '1'
        dataset_name = 'example'
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'schema': {}
        }
        self.schema_checker.redis_client.set(dataset_id, initial_record)

        active_datasets = ['2', '3']

        # execute
        self.schemachecker.delete_deprecated_violations(active_datasets)

        # verify
        record_actual = self.schema_checker.redis_client.get(dataset_id)
        self.assertIsNone(record_actual)

    def test_delete_deprecated_violations_no_more_active_with_urls_dict(self):
        # prepare
        dataset_id = '1'
        dataset_name = 'example'
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'schema': {},
            'urls': {}
        }
        self.schema_checker.redis_client.set(dataset_id, initial_record)

        active_datasets = ['2', '3']

        # execute
        self.schemachecker.delete_deprecated_violations(active_datasets)

        # verify
        record_actual = eval(self.schema_checker.redis_client.get(dataset_id))
        expected_record = initial_record
        expected_record.pop('schema')
        self.assertDictEqual(record_actual, expected_record)
