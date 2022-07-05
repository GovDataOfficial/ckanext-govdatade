import unittest

from pylons import config

from ckanext.govdatade.commands.linkchecker import LinkChecker as LinkCheckerCommand
from ckanext.govdatade.validators.link_checker import LinkChecker


class TestLinkChecker(unittest.TestCase):

    def setUp(self):
        self.linkchecker = LinkCheckerCommand(name = 'LinkCheckerTest')
        self.link_checker = LinkChecker(config)
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
        self.link_checker.redis_client.set(dataset_id, initial_record)

        active_datasets = ['2', '3']

        # execute
        self.linkchecker.delete_deprecated_datasets(active_datasets)

        # verify
        record_actual = self.link_checker.redis_client.get(dataset_id)
        self.assertIsNone(record_actual)
