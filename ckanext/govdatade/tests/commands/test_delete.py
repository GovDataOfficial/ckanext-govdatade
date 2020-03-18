from ckanext.govdatade.commands.delete import Delete as DeleteCommand
from mock import patch, Mock, call
from ckan import model

import unittest


class DummyClass:
    pass


class TestDeleteCommand(unittest.TestCase):

    def setUp(self):
        self.target = DeleteCommand(name='DeleteTest')
        self.target.options = DummyClass()
        self.target.options.dry_run = 'True'

    def tearDown(self):
        # Remove option to avoid OptionConflictError
        self.target.parser.remove_option('--dry-run')

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_missing_command(self, mock_super_load_config):
        # prepare
        self.target.args = []
        self.target._delete_datasets = Mock()
        self.target._gather_dataset_ids = Mock()

        # execute
        with self.assertRaises(SystemExit) as cm:
            self.target.command()

        # verify
        self.assertEqual(cm.exception.code, 1)
        mock_super_load_config.assert_called_once_with()
        self.target._delete_datasets.assert_not_called()
        self.target._gather_dataset_ids.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_unknown_command_missing_second_argument(self, mock_super_load_config):
        # prepare
        self.target.args = ['foo']
        self.target._delete_datasets = Mock()
        self.target._gather_dataset_ids = Mock()

        # execute
        with self.assertRaises(SystemExit) as cm:
            self.target.command()

        # verify
        self.assertEqual(cm.exception.code, 1)
        mock_super_load_config.assert_called_once_with()
        self.target._delete_datasets.assert_not_called()
        self.target._gather_dataset_ids.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_unknown_command(self, mock_super_load_config):
        # prepare
        self.target.args = ['foo', 'title:waterfall']
        self.target._delete_datasets = Mock()
        self.target._gather_dataset_ids = Mock()

        # execute
        self.target.command()

        # verify
        mock_super_load_config.assert_called_once_with()
        self.target._delete_datasets.assert_not_called()
        self.target._gather_dataset_ids.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_missing_second_argument(self, mock_super_load_config):
        # prepare
        self.target.args = ['datasets']
        self.target._delete_datasets = Mock()
        self.target._gather_dataset_ids = Mock()

        # execute
        with self.assertRaises(SystemExit) as cm:
            self.target.command()

        # verify
        self.assertEqual(cm.exception.code, 1)
        mock_super_load_config.assert_called_once_with()
        self.target._delete_datasets.assert_not_called()
        self.target._gather_dataset_ids.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_invalid_second_argument(self, mock_super_load_config):
        # prepare
        self.target.args = ['datasets', 'title']
        self.target._delete_datasets = Mock()
        self.target._gather_dataset_ids = Mock()

        # execute
        with self.assertRaises(SystemExit) as cm:
            self.target.command()

        # verify
        self.assertEqual(cm.exception.code, 1)
        mock_super_load_config.assert_called_once_with()
        self.target._delete_datasets.assert_not_called()
        self.target._gather_dataset_ids.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_valid_command_dry_run_no_boolean(self, mock_super_load_config):
        # prepare
        self.target.args = ['datasets', 'title:waterfall']
        self.target.options.dry_run = 'F'
        self.target._delete_datasets = Mock()
        self.target._gather_dataset_ids = Mock()

        # execute
        with self.assertRaises(SystemExit) as cm:
            self.target.command()

        # verify
        self.assertEqual(cm.exception.code, 2)
        mock_super_load_config.assert_called_once_with()
        self.target._delete_datasets.assert_not_called()
        self.target._gather_dataset_ids.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_valid_command_single_param(self, mock_super_load_config):
        # prepare
        self.target.args = ['datasets', 'title:waterfall']
        self.target._gather_dataset_ids = Mock()
        self.target._gather_dataset_ids.return_value = ['id1']
        self.target._delete = Mock()

        # execute
        self.target.command()

        # verify
        mock_super_load_config.assert_called_once_with()
        self.target._gather_dataset_ids.assert_called_once_with()
        # dry-run
        self.target._delete.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_valid_command_multiple_param(self, mock_super_load_config):
        # prepare
        self.target.args = ['datasets', 'title:waterfall', 'description:hamburg']
        self.target._gather_dataset_ids = Mock()
        self.target._gather_dataset_ids.return_value = ['id1', 'id2']
        self.target._delete = Mock()

        # execute
        self.target.command()

        # verify
        mock_super_load_config.assert_called_once_with()
        self.target._gather_dataset_ids.assert_called_once_with()
        # dry-run
        self.target._delete.assert_not_called()

    @patch("ckan.lib.cli.CkanCommand._load_config")
    def test_command_delete_valid_command_dry_run_false(self, mock_super_load_config):
        # prepare
        self.target.args = ['datasets', 'title:waterfall']
        self.target.options.dry_run = 'False'
        self.target._gather_dataset_ids = Mock()
        self.target._gather_dataset_ids.return_value = ['id1', 'id2']
        self.target._delete = Mock()

        # execute
        self.target.command()

        # verify
        mock_super_load_config.assert_called_once_with()
        self.target._gather_dataset_ids.assert_called_once_with()
        self.target._delete.assert_has_calls([call("id1"), call("id2")])

    @patch('ckan.plugins.toolkit.get_action')
    def test_delete_datasets_dry_run(self, mock_get_action):
        # prepare
        package1 = {'id': 'abc', 'name': 'package1_name'}
        package2 = {'id': 'xyz', 'name': 'package2_name'}

        mock_action_methods = Mock("action-methods")
        package_search_result = {'results': [package1, package2]}
        mock_action_methods.side_effect = [package_search_result]
        mock_get_action.return_value = mock_action_methods
        self.target._delete = Mock()

        admin_user = 'default'
        self.target.admin_user = {'name': admin_user}
        filter_param = 'title:waterfall'
        self.target.package_search_filter_params = filter_param
        self.target.dry_run = True

        # execute
        self.target._delete_datasets()

        # verify
        self.assertEqual(1, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("package_search")])
        expected_package_search_calls = [call({}, {"fq": filter_param, "rows": 100, "start": 0})]
        mock_action_methods.assert_has_calls(expected_package_search_calls)
        # dry-run
        self.target._delete.assert_not_called()

    @patch('ckan.plugins.toolkit.get_action')
    def test_delete_datasets_dry_run_false(self, mock_get_action):
        # prepare
        package1 = {'id': 'abc', 'name': 'package1_name'}
        package2 = {'id': 'xyz', 'name': 'package2_name'}

        mock_action_methods = Mock("action-methods")
        package_search_result = {'results': [package1, package2]}
        mock_action_methods.side_effect = [package_search_result]
        mock_get_action.return_value = mock_action_methods
        method_delete_mock = Mock()
        self.target._delete = method_delete_mock

        admin_user = 'default'
        self.target.admin_user = {'name': admin_user}
        filter_param = 'title:waterfall'
        self.target.package_search_filter_params = filter_param
        self.target.dry_run = False

        # execute
        self.target._delete_datasets()

        # verify
        self.assertEqual(1, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("package_search")])
        expected_package_search_calls = [call({}, {"fq": filter_param, "rows": 100, "start": 0})]
        mock_action_methods.assert_has_calls(expected_package_search_calls)
        self.assertEqual(2, method_delete_mock.call_count)
        expected_delete_calls_ordered = []
        for id in set([x['id'] for x in package_search_result['results']]):
            expected_delete_calls_ordered.append(call(id))
        self.assertEqual(method_delete_mock.call_args_list, expected_delete_calls_ordered)

    @patch('ckan.plugins.toolkit.get_action')
    def test_delete(self, mock_get_action):
        # prepare
        package_id = 'abc'
        mock_action_methods = Mock("action-methods")
        mock_get_action.return_value = mock_action_methods

        admin_user = 'default'
        self.target.admin_user = {'name': admin_user}

        # execute
        self.target._delete(package_id)

        # verify
        self.assertEqual(1, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("package_delete")])
        expected_package_delete_calls = [call({'user': admin_user}, {'id': package_id})]
        mock_action_methods.assert_has_calls(expected_package_delete_calls)
