import unittest

from mock import patch, Mock, ANY, call

from ckan import model
from ckanext.govdatade.commands.purge import Purge as PurgeCommand


class DummyClass:
    pass

class TestPurgeCommand(unittest.TestCase):

    def setUp(self):
        self.target = PurgeCommand(name = 'PurgeTest')

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_purge_deleted_datasets_missing_command(self, mock_super_load_config):
        # prepare
        self.target.args = []
        self.target.purge_deleted_datasets = Mock()

        # execute
        with self.assertRaises(SystemExit) as cm:
            self.target.command()

        # verify
        self.assertEqual(cm.exception.code, 1)
        mock_super_load_config.assert_called_once_with()
        self.target.purge_deleted_datasets.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_purge_deleted_datasets_unknown_command(self, mock_super_load_config):
        # prepare
        self.target.args = ['foo']
        self.target.purge_deleted_datasets = Mock()

        # execute
        self.target.command()

        # verify
        mock_super_load_config.assert_called_once_with()
        self.target.purge_deleted_datasets.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_purge_deleted_datasets_valid_command(self, mock_super_load_config):
        # prepare
        self.target.args = ['deleted', 'ignored']
        self.target.purge_deleted_datasets = Mock()

        # execute
        self.target.command()

        # verify
        mock_super_load_config.assert_called_once_with()
        self.target.purge_deleted_datasets.assert_called_once_with()

    @patch("ckan.model.meta.Session.query")
    @patch('ckan.plugins.toolkit.get_action')
    def test_purge_deleted_datasets(self, mock_get_action, mock_session_query):
        # prepare
        package1 = DummyClass()
        package1.id = 'abc'
        package1.name = 'package1_name'
        package2 = DummyClass()
        package2.id = 'xyz'
        package2.name = 'package2_name'
        mock_session_query.return_value.filter_by.return_value.filter.return_value = [package1, package2]

        self.target.log_deleted_packages_in_file = Mock()

        mock_action_methods = Mock("action-methods")
        mock_get_action.return_value = mock_action_methods

        admin_user = 'default'
        self.target.admin_user = {'name': admin_user}

        # execute
        self.target.purge_deleted_datasets()

        # verify
        mock_session_query.assert_called_once_with(model.package.Package)
        mock_session_query.return_value.filter_by.assert_called_once_with(state=model.State.DELETED)
        mock_session_query.return_value.filter_by.return_value.filter.assert_called_once_with(ANY)
        expected_logging_calls = [call(package1, ANY), call(package2, ANY)]
        self.target.log_deleted_packages_in_file.assert_has_calls(expected_logging_calls)
        self.assertEqual(2, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("dataset_purge"), call("dataset_purge")])
        expected_purge_calls = [call({'user': admin_user}, {'id': package1.id}),
                                  call({'user': admin_user}, {'id': package2.id})]
        mock_action_methods.assert_has_calls(expected_purge_calls)
