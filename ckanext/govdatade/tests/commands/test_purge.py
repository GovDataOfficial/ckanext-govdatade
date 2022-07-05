import unittest

from mock import patch, Mock, ANY, call

from ckan import model
#from ckanext.govdatade.commands.purge import Purge as PurgeCommand
import ckanext.govdatade.commands.command_util as util

class DummyClass:
    pass

class TestPurgeCommand(unittest.TestCase):

    @patch("ckan.model.meta.Session.query")
    @patch('ckan.plugins.toolkit.get_action')
    @patch('ckanext.govdatade.commands.command_util._log_deleted_packages_in_file')
    def test_purge_deleted_datasets(self, mock_log_deleted_packages_in_file,
                                    mock_get_action, mock_session_query):
        # prepare
        package1 = DummyClass()
        package1.id = 'abc'
        package1.name = 'package1_name'
        package2 = DummyClass()
        package2.id = 'xyz'
        package2.name = 'package2_name'
        mock_session_query.return_value.filter_by.return_value.filter.return_value = [package1, package2]

        mock_action_methods = Mock("action-methods")
        mock_get_action.return_value = mock_action_methods

        admin_user_name = 'default'
        admin_user = {'name': admin_user_name}

        path_to_logfile = 'path_to_logfile'
        # execute
        util.purge_deleted_datasets(path_to_logfile, admin_user)

        # verify
        mock_session_query.assert_called_once_with(model.package.Package)
        mock_session_query.return_value.filter_by.assert_called_once_with(state=model.State.DELETED)
        mock_session_query.return_value.filter_by.return_value.filter.assert_called_once_with(ANY)
        expected_logging_calls = [call(package1, ANY, path_to_logfile), call(package2, ANY, path_to_logfile)]
        mock_log_deleted_packages_in_file.assert_has_calls(expected_logging_calls)
        self.assertEqual(2, mock_get_action.call_count)
        self.assertEqual(mock_get_action.call_args_list, [call("dataset_purge"), call("dataset_purge")])
        expected_purge_calls = [call({'user': admin_user_name}, {'id': package1.id}),
                                  call({'user': admin_user_name}, {'id': package2.id})]
        mock_action_methods.assert_has_calls(expected_purge_calls)
