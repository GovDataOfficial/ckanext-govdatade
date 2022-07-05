import json
import unittest

from ckan.plugins import toolkit as tk
import ckanext.govdatade.commands.command_util as util
from ckanext.govdatade.validators.link_checker import LinkChecker


class TestLinkChecker(unittest.TestCase):

    def setUp(self):
        self.link_checker = LinkChecker(tk.config)
        self.link_checker.redis_client.flushdb()

    def tearDown(self):
        self.link_checker.redis_client.flushdb()

    def test_delete_deprecated_datasets_no_more_active(self):
        # prepare
        dataset_id = '1'
        dataset_name = 'example'
        initial_record = {
            'id': dataset_id,
            'name': dataset_name,
            'urls': {}
        }
        self.link_checker.redis_client.set(dataset_id, json.dumps(initial_record))

        active_datasets = ['2', '3']

        # execute
        util.delete_deprecated_datasets(active_datasets)

        # verify
        record_actual = self.link_checker.redis_client.get(dataset_id)
        self.assertIsNone(record_actual)
