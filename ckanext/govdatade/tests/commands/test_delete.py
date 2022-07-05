import unittest

from mock import patch, Mock, call
import ckanext.govdatade.commands.command_util as util


class TestDeleteCommand(unittest.TestCase):

    def test_command_delete_missing_second_argument(self):
        # prepare
        package_search_params = []

        # execute
        with self.assertRaises(SystemExit) as cm:
            util.check_package_search_params(package_search_params)

        # verify
        self.assertEqual(cm.exception.code, 1)

    def test_command_delete_invalid_second_argument(self):
        # prepare
        package_search_params = ['title']

        # execute
        with self.assertRaises(SystemExit) as cm:
            util.check_package_search_params(package_search_params)

        # verify
        self.assertEqual(cm.exception.code, 1)

    @patch('ckanext.govdatade.commands.command_util._gather_dataset_ids')
    @patch('ckanext.govdatade.commands.command_util._delete')
    def test_command_delete_valid_command_single_param(self, mock_delete, mock_gather_dataset_ids):
        # prepare
        mock_gather_dataset_ids.return_value = ['id1']
        admin_user = dict(name='admin')
        dry_run = True
        package_search_params = ['title:waterfall']

        package_search_filter_params = util.check_package_search_params(package_search_params)

        # execute
        util.delete_datasets(dry_run, package_search_filter_params, admin_user)

        # verify
        mock_gather_dataset_ids.assert_called_once_with(package_search_filter_params)
        # dry-run
        mock_delete.assert_not_called()

    @patch('ckanext.govdatade.commands.command_util._gather_dataset_ids')
    @patch('ckanext.govdatade.commands.command_util._delete')
    def test_command_delete_valid_command_multiple_param(self, mock_delete, mock_gather_dataset_ids):
        # prepare
        mock_gather_dataset_ids.return_value = ['id1', 'id2']
        admin_user = dict(name='admin')
        dry_run = True
        package_search_params = ['title:waterfall', 'description:hamburg']
        package_search_filter_params = util.check_package_search_params(package_search_params)

        # execute
        util.delete_datasets(dry_run, package_search_filter_params, admin_user)

        # verify
        mock_gather_dataset_ids.assert_called_once_with(package_search_filter_params)
        # dry-run
        mock_delete.assert_not_called()

    @patch('ckanext.govdatade.commands.command_util._gather_dataset_ids')
    @patch('ckanext.govdatade.commands.command_util._delete')
    def test_command_delete_valid_command_dry_run_false(self, mock_delete, mock_gather_dataset_ids):
        # prepare
        mock_gather_dataset_ids.return_value = ['id1', 'id2']
        admin_user = dict(name='admin')
        dry_run = False
        package_search_params = ['title:waterfall']
        package_search_filter_params = util.check_package_search_params(package_search_params)

        # execute
        util.delete_datasets(dry_run, package_search_filter_params, admin_user)

        # verify
        mock_gather_dataset_ids.assert_called_once_with(package_search_filter_params)
        mock_delete.assert_has_calls([call("id1", admin_user), call("id2", admin_user)])

    @patch('ckan.plugins.toolkit.get_action')
    @patch('ckanext.govdatade.commands.command_util._delete')
    def test_delete_datasets_dry_run(self, mock_delete, mock_get_action):
        # prepare
        package1 = {'id': 'abc', 'name': 'package1_name'}
        package2 = {'id': 'xyz', 'name': 'package2_name'}

        mock_action_methods = Mock("action-methods")
        package_search_result = {'results': [package1, package2]}
        mock_action_methods.side_effect = [package_search_result]
        mock_get_action.return_value = mock_action_methods

        admin_user = {'name': 'default'}
        package_search_filter_params = 'title:waterfall'
        dry_run = True

        # execute
        util.delete_datasets(dry_run, package_search_filter_params, admin_user)

        # verify
        self.assertEqual(1, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("package_search")])
        expected_package_search_calls = [call({}, {"fq": package_search_filter_params, "rows": 100, "start": 0})]
        mock_action_methods.assert_has_calls(expected_package_search_calls)
        # dry-run
        mock_delete.assert_not_called()

    @patch('ckan.plugins.toolkit.get_action')
    @patch('ckanext.govdatade.commands.command_util._delete')
    def test_delete_datasets_dry_run_false(self, mock_delete, mock_get_action):
        # prepare
        package1 = {'id': 'abc', 'name': 'package1_name'}
        package2 = {'id': 'xyz', 'name': 'package2_name'}

        mock_action_methods = Mock("action-methods")
        package_search_result = {'results': [package1, package2]}
        mock_action_methods.side_effect = [package_search_result]
        mock_get_action.return_value = mock_action_methods

        admin_user = {'name': 'default'}
        package_search_filter_params = 'title:waterfall'
        dry_run = False

        # execute
        util.delete_datasets(dry_run, package_search_filter_params, admin_user)

        # verify
        self.assertEqual(1, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("package_search")])
        expected_package_search_calls = [call({}, {"fq": package_search_filter_params, "rows": 100, "start": 0})]
        mock_action_methods.assert_has_calls(expected_package_search_calls)
        self.assertEqual(2, mock_delete.call_count)
        expected_delete_calls_ordered = []
        for id in set([x['id'] for x in package_search_result['results']]):
            expected_delete_calls_ordered.append(call(id, admin_user))
        self.assertEqual(mock_delete.call_args_list, expected_delete_calls_ordered)

    @patch('ckan.plugins.toolkit.get_action')
    def test_delete(self, mock_get_action):
        # prepare
        package_id = 'abc'
        mock_action_methods = Mock("action-methods")
        mock_get_action.return_value = mock_action_methods

        admin_user = {'name': 'default'}

        # execute
        util._delete(package_id, admin_user)

        # verify
        self.assertEqual(1, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("package_delete")])
        expected_package_delete_calls = [call({'user': admin_user['name']}, {'id': package_id})]
        mock_action_methods.assert_has_calls(expected_package_delete_calls)
