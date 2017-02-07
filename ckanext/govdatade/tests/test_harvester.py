#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.govdatade.extras import Extras
from ckanext.govdatade.harvesters.ckanharvester import GovDataHarvester
from ckanext.govdatade.harvesters.ckanharvester import BerlinCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import RlpCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import HamburgCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import OpenNrwCKANHarvester
from mock import patch, Mock, ANY, call

import json
import httpretty
import unittest


class DummyClass:
    pass


class GovDataHarvesterTest(unittest.TestCase):

    def test_amend_package(self):
        # prepare
        harvester = HamburgCKANHarvester()
        harvester.portal = 'http://hamburg-harvester.de'

        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some tag"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some other tag"
        }]

        extras = [{
                'key': 'content_type',
                'value': 'Kartenebene',
            }, {
                'key': 'terms_of_use',
                'value': {
                    'license_id': 'cc-by'
                },
            }]

        package = {'author': 'Hamburg',
                   'author_email': 'hh@hamburg.de',
                   'groups': [{'name': 'transport_und_verkehr'}, {'id': 'geo'}],
                   'tags': tags_list_dict,
                   'license_id': 'cc-by',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'resources': [{'format': 'PDF'}],
                   'type': 'dataset',
                   'extras':extras,
                   'relationships_as_subject': [],
                   'relationships_as_object': []
                  }

        # execute
        GovDataHarvester.amend_package(harvester, package)

        # verify
        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some-tag"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some-other-tag"
        }]

        expected_extras = [
            {
                'key': 'content_type',
                'value': 'Kartenebene',
            },
            {
                'key': 'terms_of_use',
                'value': {'license_id': 'cc-by'},
            },
            {
                'key': 'metadata_original_portal',
                'value': 'http://hamburg-harvester.de',
            }]

        self.assertEqual(expected_tags_list_dict, package['tags'])
        self.assertEqual(expected_extras, package['extras'])
        self.assertListEqual(
            package['groups'], [{'id': 'transport_und_verkehr', 'name': 'transport_und_verkehr'},
                                {'id': 'geo'}])
        self.assertEqual(package['type'], 'dataset')
        self.assertEqual(package['resources'], [{'format': 'pdf'}])
        self.assertNotIn('relationships_as_subject', package)
        self.assertNotIn('relationships_as_object', package)

    def test_amend_package_without_extras_groups_tags(self):
        # prepare
        harvester = HamburgCKANHarvester()
        harvester.portal = 'http://hamburg-harvester.de'

        package = {'author': 'Hamburg',
                   'author_email': 'hh@hamburg.de',
                   'license_id': 'cc-by',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'resources': [{'format': 'PDF'}],
                   'type': 'dataset',
                   'relationships_as_subject': [],
                   'relationships_as_object': []
                  }

        # execute
        GovDataHarvester.amend_package(harvester, package)

        # verify
        expected_extras = [
            {
                'key': 'metadata_original_portal',
                'value': 'http://hamburg-harvester.de',
            }]
        self.assertEqual(package['type'], 'dataset')
        self.assertEqual(package['resources'], [{'format': 'pdf'}])
        self.assertNotIn('relationships_as_subject', package)
        self.assertNotIn('relationships_as_object', package)
        self.assertNotIn('tags', package)
        self.assertNotIn('groups', package)
        self.assertIn('extras', package)
        self.assertEqual(package['extras'], expected_extras)

    def test_get_min_package_dict(self):
        # prepare
        package_dict = {
            'id': '0123456789',
            'name': 'name-value',
            'metadata_modified': '2016-08-19T08:20:01.501641',
            'other_key': 'other_value',
            'extras': [
                {'key': 'moo', 'value': 'boo'}
            ]
        }

        # execute
        result = GovDataHarvester.get_min_package_dict(package_dict)

        # verify
        expected_package_dict = {
            'id': '0123456789',
            'name': 'name-value',
            'metadata_modified': '2016-08-19T08:20:01.501641'
        }

        self.assertEqual(result, expected_package_dict)

    def test_has_tag_success(self):
        harvester = GovDataHarvester()

        tags_list_dict = [{
            "name": "tag-one"
        }, {
            "name": "tag-two"
        }, {
            "name": "tag-three"
        }]

        self.assertTrue(
            harvester.has_tag(tags_list_dict, 'tag-one')
        )

    def test_has_tag_failure(self):
        harvester = GovDataHarvester()

        self.assertFalse(
            harvester.has_tag('Foo', 'tag-one')
        )

        tags_list_dict = [{
            "name": "tag-one"
        }, {
            "name": "tag-two"
        }, {
            "name": "tag-three"
        }]

        self.assertFalse(
            harvester.has_tag(tags_list_dict, 'tag-five')
        )

    def test_compare_metadata_modified_remote_is_newer(self):

        harvester = GovDataHarvester()

        localDatetime = '2016-03-22T06:30:10.00000'
        remoteDatetime = '2016-03-23T07:20:30.00000'

        result = harvester.compare_metadata_modified(remoteDatetime, localDatetime)

        self.assertTrue(result, 'remote is older than local metadata_modified')

    def test_compare_metadata_modified_remote_is_older(self):

        harvester = GovDataHarvester()

        localDatetime = '2016-03-23T07:20:30.00000'
        remoteDatetime = '2016-03-22T06:30:10.00000'

        result = harvester.compare_metadata_modified(remoteDatetime, localDatetime)

        self.assertFalse(result, 'remote is newer than local metadata_modified')

    def test_compare_metadata_modified_wrong_format(self):

        harvester = GovDataHarvester()

        localDatetime = '2016-03-23T07:20:30' # wrong format
        remoteDatetime = '2016-03-22T06:30:10.00000'

        self.assertRaises(ValueError, harvester.compare_metadata_modified, remoteDatetime, localDatetime)

    def test_set_portal(self):
        harvester = GovDataHarvester()
        harvester.portal = 'foo-portal'

        package_dict = {
            'id': '0123456789',
            'extras': [
                {'key': 'moo', 'value': 'boo'}
            ]
        }

        harvester.set_portal(package_dict)

        expected_package_dict = {
            'id': '0123456789',
            'extras': [
                {'key': 'moo', 'value': 'boo'},
                {'key': 'metadata_original_portal', 'value': 'foo-portal'}
            ]
        }

        self.assertEqual(package_dict, expected_package_dict)

    def test_lowercase_resources_formats(self):
        harvester = GovDataHarvester()

        package_dict = {
            'id': '0123456789',
            'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
        }
        harvester.lowercase_resources_formats(package_dict)

        expected_package_dict = {
            'id': '0123456789',
            'resources': [{'format':'csv', 'url': 'http://travis-ci.org'}],
        }

        self.assertEqual(package_dict, expected_package_dict)

        package_dict = {
            'id': '0123456789',
            'resources': [{'format':'UML', 'url': 'http://travis-ci.org'}, {'format':'XML', 'url': 'http://travis-ci.org'}],
        }
        harvester.lowercase_resources_formats(package_dict)

        expected_package_dict = {
            'id': '0123456789',
            'resources': [{'format':'uml', 'url': 'http://travis-ci.org'}, {'format':'xml', 'url': 'http://travis-ci.org'}],
        }

        self.assertEqual(package_dict, expected_package_dict)

    def test_tags_dict_to_list(self):
        harvester = GovDataHarvester()
        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag/one&$#?+\\"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag/two"
        }, {
            "vocabulary_id": 3,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "/+#`?tag/////three"
        }, {
            "vocabulary_id": 4,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag.four"
        }, {
            "vocabulary_id": 5,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag-five"
        }, {
            "vocabulary_id": 6,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag_six"
        }, {
            "vocabulary_id": 7,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag/one, tag/two, Tag/three"
        }, {
            "vocabulary_id": 8,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": u"Bevölkerung"
        }, {
            "vocabulary_id": 9,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag/one, tag/two,Tag/three"
        }, {
            "vocabulary_id": 10,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "Umwelt, Lebensmittel, Futtermittel, Strahlenschutzvorsorge"
        }]

        harvester.cleanse_tags(tags_list_dict)

        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagone"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagtwo"
        }, {
            "vocabulary_id": 3,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagthree"
        }, {
            "vocabulary_id": 4,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag.four"
        }, {
            "vocabulary_id": 5,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag-five"
        }, {
            "vocabulary_id": 6,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag_six"
        }, {
            "vocabulary_id": 7,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagone-tagtwo-tagthree"
        }, {
            "vocabulary_id": 8,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": u"bevölkerung"
        }, {
            "vocabulary_id": 9,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagone-tagtwotagthree"
        }, {
            "vocabulary_id": 10,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "umwelt-lebensmittel-futtermittel-strahlenschutzvorsorge"
        }]

        self.assertEqual(expected_tags_list_dict, tags_list_dict)

    def test_cleanse_tags_comma_separated(self):

        harvester = GovDataHarvester()
        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": 'tagone, tagtwo'
        }]

        harvester.cleanse_tags(tags_list_dict)

        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagone-tagtwo"
        }]

        self.assertEqual(expected_tags_list_dict, tags_list_dict)

        tags = 'tagone, tagtwo, tagthree, tagfour'

        self.assertEqual(
            harvester.cleanse_tags(tags),
            'tagone-tagtwo-tagthree-tagfour'
        )

    def test_cleanse_tags_special_character(self):
        harvester = GovDataHarvester()

        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag/one, tag/two, Tag/three"
        }]

        harvester.cleanse_tags(tags_list_dict)

        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagone-tagtwo-tagthree"
        }]

        self.assertEqual(expected_tags_list_dict, tags_list_dict)

    def test_cleanse_tags_erlaubte_zeichen(self):
        harvester = GovDataHarvester()

        tags_list_dict = [{
            "name": u' Tag\xe4\xfc\xdf\xf6\xc4\xd6\xdc-_ . '
        }]

        harvester.cleanse_tags(tags_list_dict)

        expected_tags_list_dict = [{
            "name": u'tag\xe4\xfc\xdf\xf6\xe4\xf6\xfc-_-.'
        }]

        self.assertEqual(expected_tags_list_dict, tags_list_dict)

    def test_cleanse_special_characters_erlaubte_zeichen(self):
        # prepare
        harvester = GovDataHarvester()

        tag = u' Tag\xe4\xfc\xdf\xf6\xc4\xd6\xdc-_ . '

        # execute
        result = harvester.cleanse_special_characters(tag)

        # verify

        self.assertEqual(result, u'tag\xe4\xfc\xdf\xf6\xe4\xf6\xfc-_-.')

    def test_cleanse_tags_replace_whitespace_characters(self):
        harvester = GovDataHarvester()

        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag  one"
        }, {
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag two"
        }]

        harvester.cleanse_tags(tags_list_dict)

        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag--one"
        }, {
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag-two"
        }]

        self.assertEqual(expected_tags_list_dict, tags_list_dict)

    @httpretty.activate
    @patch("ckanext.harvest.harvesters.ckanharvester.CKANHarvester.gather_stage")
    def test_gather_stage(self, mock_super):
        # prepare
        harvester = GovDataHarvester()
        package_ids = ['abc', 'efg']
        source_id = 'xyz'
        source_url = 'http://test.de/'
        source = DummyClass()
        source.id = source_id
        source.config = ''
        source.url = source_url
        harvest_job = DummyClass()
        harvest_job.source = source

        httpretty.HTTPretty.allow_net_connect = False
        url = source_url + 'api/2/rest/package'
        response = '[' + ",".join(['"' + package_id + '"' for package_id in package_ids]) + ']'
        # self._get_content(url) # url = harvest_job.source.url + '/api/2/rest/package'
        httpretty.register_uri(httpretty.GET, url, status=200, body=response)

        harvester.delete_deprecated_datasets = Mock()

        # execute
        harvester.gather_stage(harvest_job)

        # verify
        self.assertTrue(httpretty.has_request())
        harvester.delete_deprecated_datasets.assert_called_with(package_ids, harvest_job)
        mock_super.assert_called_once_with(harvest_job)

    @patch('ckan.plugins.toolkit.get_action')
    def test_delete_deprecated_datasets(self, mock_get_action):
        # prepare
        harvester = GovDataHarvester()
        harvester.portal = 'http://www.regionalstatisitk.de/'
        package_id_deprecated = 'abc'
        package_id_existent = 'efg'
        remote_package_ids = [package_id_existent]
        local_package_ids = [package_id_deprecated, package_id_existent]
        source_id = 'xyz'
        source_url = 'http://test.de/'
        source = DummyClass()
        source.id = source_id
        source.config = ''
        source.url = source_url
        harvest_job = DummyClass()
        harvest_job.source = source
        harvest_job.source_id = source_id

        # 1) harvester_package = get_action('package_show')(context, {'id': harvest_job.source_id})
        org_id = '1234567890'
        harvest_source_package_dict = {
                       'id': '0123456789',
                       'name': 'test-harvester',
                       'owner_org': org_id
                       }
        # 2) result = get_action('package_search')({}, query_object)
        package_deprecated = {
                       'id': package_id_deprecated,
                       'name': 'deprectated-package',
                       'metadata_modified': '2016-08-19T08:20:01.501641',
                       'owner_org': org_id}
        package_existent = {
                       'id': package_id_existent,
                       'name': 'existent-package',
                       'owner_org': org_id}
        package_search_result = {'results': [package_deprecated, package_existent]}
        mock_action_methods = Mock("action-methods")
        mock_action_methods.side_effect = [harvest_source_package_dict, package_search_result]
        # 3) get_action('bulk_update_delete')(context, {'datasets': deprecated_ids, 'org_id': organization_id})
        mock_get_action.return_value = mock_action_methods
        # self.rename_datasets_before_delete(deprecated_package_dicts)
        harvester.rename_datasets_before_delete = Mock()
        harvester.rename_datasets_before_delete.return_value = [package_id_deprecated]
        # self.delete_packages(deprecated_ids)
        harvester.delete_packages = Mock()
        harvester.delete_packages.return_value = [package_id_deprecated]
        # self.log_deleted_packages_in_file(deprecated_package_dicts, checkpoint_end)
        harvester.log_deleted_packages_in_file = Mock()

        # execute
        harvester.delete_deprecated_datasets(remote_package_ids, harvest_job)

        # verify
        expected_get_action_call_count = 2
        self.assertEqual(mock_get_action.call_count, expected_get_action_call_count)
        mock_get_action.assert_any_call("package_show")
        mock_get_action.assert_any_call("package_search")

        self.assertEqual(mock_action_methods.call_count, expected_get_action_call_count)
        expected_action_calls_original = [
            call({'ignore_auth': True, 'model': ANY, 'session': ANY, 'user': u'harvest', 'api_version': 1},
                 {'id': source_id}),
            call({}, {'fq': ('+owner_org:"%s" +metadata_original_portal:"%s" -type:"harvest"' %
                             (org_id, harvester.portal)),
                      'rows': 500, 'start': 0})
        ]
        mock_action_methods.assert_has_calls(expected_action_calls_original)

        package_deprecated_min = {
                       'id': package_id_deprecated,
                       'name': 'deprectated-package',
                       'metadata_modified': '2016-08-19T08:20:01.501641'}
        deprecated_package_dicts = [package_deprecated_min]
        harvester.rename_datasets_before_delete.assert_called_with(deprecated_package_dicts)
        harvester.delete_packages.assert_called_once_with([package_id_deprecated])
        harvester.log_deleted_packages_in_file.assert_called_with(deprecated_package_dicts, ANY)

    @patch('ckan.plugins.toolkit.get_action')
    def test_delete_packages(self, mock_get_action):
        # prepare
        package1_id = 'abc'
        package2_id = 'efg'

        mock_action_methods = Mock("action-methods")
        # 3) get_action('package_delete')(context, {'id': to_delete_id})
        mock_get_action.return_value = mock_action_methods

        package_ids_to_delete = [package1_id, package2_id]

        # execute
        GovDataHarvester.delete_packages(package_ids_to_delete)

        # verify
        self.assertEqual(mock_get_action.call_count, 1)
        mock_get_action.assert_any_call("package_delete")
        self.assertEqual(mock_action_methods.call_count, len(package_ids_to_delete))
        expected_action_calls_original = []
        for to_delete_id in package_ids_to_delete:
            expected_action_calls_original.append(
                call({'ignore_auth': True, 'model': ANY, 'session': ANY, 'user': u'harvest',
                      'api_version': 1},
                     {'id': to_delete_id}))
        mock_action_methods.assert_has_calls(expected_action_calls_original)

class BerlinHarvesterTest(unittest.TestCase):

    def test_tags_are_cleansed_when_amending(self):
        harvester = BerlinCKANHarvester()
        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagone"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tagtwo"
        }]

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'tags': tags_list_dict,
            'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        harvester.amend_package(dataset)
        self.assertTrue('tags' in dataset)
        self.assertEqual(dataset['tags'], tags_list_dict)

    def test_tags_are_not_cleansed_when_not_present(self):
        harvester = BerlinCKANHarvester()

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        valid = harvester.amend_package(dataset)
        self.assertFalse('tags' in dataset)
        self.assertTrue(valid)

    def test_sector_amendment(self):

        harvester = BerlinCKANHarvester()

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        valid = harvester.amend_package(dataset)

        extras = Extras(dataset['extras'])
        self.assertTrue(extras.key('sector'))
        self.assertEquals('oeffentlich', extras.value('sector'))

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None},
                {'key': 'sector', 'value': None},
            ]}

        valid = harvester.amend_package(dataset)

        extras = Extras(dataset['extras'])
        self.assertTrue(extras.key('sector'))
        self.assertEquals('oeffentlich', extras.value('sector'))
        self.assertTrue(valid)

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None},
                {'key': 'sector', 'value': 'privat'},
            ]}

        valid = harvester.amend_package(dataset)

        extras = Extras(dataset['extras'])
        self.assertTrue(extras.key('sector'))
        self.assertEquals('privat', extras.value('sector'))
        self.assertFalse(valid)

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None},
                {'key': 'sector', 'value': 'andere'},
            ]}

        valid = harvester.amend_package(dataset)

        extras = Extras(dataset['extras'])
        self.assertTrue(extras.key('sector'))
        self.assertEquals('andere', extras.value('sector'))
        self.assertFalse(valid)

    def test_type_amendment(self):

        harvester = BerlinCKANHarvester()

        package = {
            'type': None,
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None},
            ]}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

        package = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

        package = {
            'type': 'dokument',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'dokument')
        self.assertTrue(valid)

        package = {
            'type': 'app',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'},
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'app')
        self.assertTrue(valid)

        package = {
            'type': 'garbage',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'},
            ],
             'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

    def test_amend_portal(self):

        harvester = BerlinCKANHarvester()
        default = 'http://datenregister.berlin.de'

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': []
        }

        valid = harvester.amend_package(dataset)
        extras = Extras(dataset['extras'])

        self.assertTrue(extras.key('metadata_original_portal'))
        self.assertEquals(default, extras.value('metadata_original_portal'))
        self.assertTrue(valid)

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': None}
            ]}

        valid = harvester.amend_package(dataset)
        extras = Extras(dataset['extras'])

        self.assertTrue(extras.key('metadata_original_portal'))
        self.assertEquals(default, extras.value('metadata_original_portal'))
        self.assertTrue(valid)

        dataset = {
            'type': 'datensatz',
            'groups': [],
            'license_id': None,
            'resources': [
                {'format':'CSV', 'url': 'http://travis-ci.org'}
            ],
            'extras': [
                {'key': 'metadata_original_portal', 'value': 'www.example.com'},
            ]}

        valid = harvester.amend_package(dataset)
        extras = Extras(dataset['extras'])

        self.assertTrue(extras.key('metadata_original_portal'))
        self.assertEquals(default, extras.value('metadata_original_portal'))
        self.assertTrue(valid)

    def test_amend_package(self):

        package = {'license_title': '',
                   'maintainer': '',
                   'maintainer_email': '',
                   'id': 'f998d542-c652-467e-b31b-c3e5d0300589',
                   'metadata_created': '2013-03-11T11:53:20.283753',
                   'relationships': [],
                   'license': None,
                   'metadata_modified': '2013-03-11T11:53:20.283753',
                   'author': '',
                   'author_email': '',
                   'state': 'active',
                   'version': '',
                   'license_id': '',
                   'type': None,
                   'resources': [],
                   'tags': [],
                   'tracking_summary': {'total': 0, 'recent': 0},
                   'groups': ['arbeit', 'geo', 'umwelt', 'wohnen'],
                   'name': 'test-dataset',
                   'isopen': False,
                   'notes_rendered': '',
                   'url': '',
                   'notes': '',
                   'title': 'Test Dataset',
                   'ratings_average': None,
                   'extras': [],
                   'ratings_count': 0,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'revision_id': '411b25f9-1b8f-4f2a-90ae-05d3e8ff8d33'}

        harvester = BerlinCKANHarvester()

        self.assertEqual(package['license_id'], '')
        self.assertEqual(len(package['groups']), 4)
        self.assertTrue('arbeit' in package['groups'])
        self.assertTrue('geo' in package['groups'])
        self.assertTrue('umwelt' in package['groups'])
        self.assertTrue('wohnen' in package['groups'])

        harvester.amend_package(package)

        self.assertEqual(package['license_id'], 'notspecified')
        self.assertEqual(len(package['groups']), 4)
        self.assertListEqual(
            package['groups'],
            [{'id': 'wirtschaft_arbeit', 'name': 'wirtschaft_arbeit'},
            {'id': 'geo', 'name': 'geo'},
            {'id': 'umwelt_klima', 'name': 'umwelt_klima'},
            {'id': 'infrastruktur_bauen_wohnen', 'name': 'infrastruktur_bauen_wohnen'}])

class RlpHarvesterTest(unittest.TestCase):

    def test_gdi_rlp_package(self):
        extras = [{
                'key': 'content_type',
                'value': 'Kartenebene',
            }, {
                'key': 'terms_of_use',
                'value': {'license_id': 'cc-by'},
            }]

        package = {'author': 'RLP',
                   'author_email': 'rlp@rlp.de',
                   'groups': [{'name': 'gdi-rp'}, {'name': 'geo'}],
                   'license_id': 'cc-by',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'resources': [{'format': 'pdf'}, {'format': 'csv'}],
                   'type': None,
                   'extras': extras,
                   }

        harvester = RlpCKANHarvester()
        harvester.amend_package(package)
        self.assertListEqual(package['groups'], [{'id': 'geo', 'name': 'geo'}])
        self.assertEqual(package['type'], 'datensatz')
        self.assertEqual(package['license_id'], 'cc-by')

    def test_gdi_rlp_package_license_cc_by_nc(self):
        extras = [{
                'key': 'content_type',
                'value': 'Kartenebene',
            }, {
                'key': 'terms_of_use',
                'value': {'license_id': 'cc-by'},
            }]

        package = {'author': 'RLP',
                   'author_email': 'rlp@rlp.de',
                   'groups': [{'name': 'gdi-rp'}, {'name': 'geo'}],
                   'license_id': 'cc-by-nc',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'resources': [{'format': 'pdf'}],
                   'type': None,
                   'extras': extras,
                   }

        harvester = RlpCKANHarvester()
        harvester.amend_package(package)
        self.assertListEqual(package['groups'], [{'id': 'geo', 'name': 'geo'}])
        self.assertEqual(package['type'], 'dokument')
        self.assertEqual(package['license_id'], 'cc-nc')

class OpenNRWHarvesterTest(unittest.TestCase):

    @patch("ckanext.govdatade.harvesters.ckanharvester.GovDataHarvester.import_stage")
    def test_import_stage(self, mock_super_import_stage):
        # prepare
        harvester = OpenNrwCKANHarvester()
        default = 'http://open.nrw/'

        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag1"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "tag2"
        }]

        package = {'author': 'OpenNRW',
                   'author_email': 'opennrw@open.nrw',
                   'groups': ['gesundheit', 'geo'],
                   'tags': tags_list_dict,
                   'license_id': 'cc-by',
                   'resources': [{'format': 'PDF', 'url': 'http://test.de'}],
                   'type': 'dataset',
                   'extras': {
                       'metadata_original_portal': 'foo',
                       'metadata_transformer': 'bar',
                   }}

        harvest_object = DummyClass()
        harvest_object.content = json.dumps(package)

        # execute
        harvester.import_stage(harvest_object)

        # verify
        actual_harvest_object_content = json.loads(harvest_object.content)
        self.assertEqual(['gesundheit', 'geo'], actual_harvest_object_content['groups'])
        self.assertEqual(tags_list_dict, actual_harvest_object_content['tags'])
        self.assertEqual(actual_harvest_object_content['type'], 'dataset')
        self.assertEqual(actual_harvest_object_content['resources'][0]['format'], 'pdf')
        portal = actual_harvest_object_content['extras']['metadata_original_portal']
        self.assertEqual(portal, default)
        metadata_transformer = actual_harvest_object_content['extras']['metadata_transformer']
        self.assertEqual(metadata_transformer, '')
        mock_super_import_stage.assert_called_once_with(harvest_object)

class HamburgHarvesterTest(unittest.TestCase):

    def test_hamburg_package_document(self):

        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some tag"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "GovData"
        }]

        extras = [{
                'key': 'content_type',
                'value': 'Kartenebene',
            }, {
                'key': 'terms_of_use',
                'value': {
                    'license_id': 'cc-by'
                },
            }]

        package = {'author': 'Hamburg',
                   'author_email': 'hh@hamburg.de',
                   'groups': ['transport-und-verkehr'],
                   'tags': tags_list_dict,
                   'license_id': 'cc-by',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'resources': [{'format': 'pdf'}],
                   'type': 'document',
                   'extras': extras,
                   }

        harvester = HamburgCKANHarvester()
        result = harvester.amend_package(package)
        self.assertTrue(result)

        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some-tag"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "govdata"
        }]

        self.assertEqual(expected_tags_list_dict, package['tags'])

        self.assertListEqual(
            package['groups'], [{'id': 'transport_verkehr', 'name': 'transport_verkehr'}])
        self.assertEqual(package['type'], 'dokument')

    def test_hamburg_package_dataset(self):

        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some tag"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some other tag"
        }]

        extras = [{
                'key': 'content_type',
                'value': 'Kartenebene',
            }, {
                'key': 'terms_of_use',
                'value': {
                    'license_id': 'cc-by'
                },
            }]

        package = {'author': 'Hamburg',
                   'author_email': 'hh@hamburg.de',
                   'groups': ['transport-und-verkehr'],
                   'tags': tags_list_dict,
                   'license_id': 'cc-by',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'resources': [{'format': 'pdf'}],
                   'type': 'dataset',
                   'extras':extras,
                  }

        harvester = HamburgCKANHarvester()
        result = harvester.amend_package(package)

        self.assertTrue(result)

        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some-tag"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some-other-tag"
        }]


        self.assertEqual(expected_tags_list_dict, package['tags'])
        self.assertListEqual(
            package['groups'], [{'id': 'transport_verkehr', 'name': 'transport_verkehr'}])
        self.assertEqual(package['type'], 'datensatz')

    def test_hamburg_package_skipping_without_tag_govdata(self):

        tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some tag"
        }]

        extras = [{
                'key': 'content_type',
                'value': 'Kartenebene',
            }, {
                'key': 'terms_of_use',
                'value': {
                    'license_id': 'cc-by'
                },
            }]

        package = {'author': 'Hamburg',
                   'author_email': 'hh@hamburg.de',
                   'groups': ['transport-und-verkehr'],
                   'tags': tags_list_dict,
                   'license_id': 'cc-by',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'resources': [{'format': 'pdf'}],
                   'type': 'document',
                   'extras': extras,
                  }

        harvester = HamburgCKANHarvester()
        result = harvester.amend_package(package)

        expected_tags_list_dict = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "some-tag"
        }]

        self.assertFalse(result)
        self.assertEqual(expected_tags_list_dict, package['tags'])
        self.assertListEqual(package['groups'], ['transport-und-verkehr'])
        self.assertEqual(package['type'], 'document')

    def test_amend_portal_without_metadata_original_portal(self):

        harvester = HamburgCKANHarvester()
        default = 'http://suche.transparenz.hamburg.de/'

        package = {'author': 'Hamburg',
                   'author_email': 'hh@hamburg.de',
                   'groups': ['transport-und-verkehr'],
                   'tags': [],
                   'license_id': 'cc-by',
                   'point_of_contact': None,
                   'point_of_contact_address': {'email': None},
                   'type': 'datensatz',
                   'resources': [{
                       'format': 'CSV',
                       'url': 'http://travis-ci.org'
                   }],
                   'extras': []}

        valid = harvester.amend_package(package)
        extras = Extras(package['extras'])
        self.assertTrue(extras.key('metadata_original_portal'))
        self.assertEquals(default, extras.value('metadata_original_portal'))
        self.assertTrue(valid)


    def test_amend_portal_metadata_original_portal_none(self):

        harvester = HamburgCKANHarvester()
        default = 'http://suche.transparenz.hamburg.de/'

        package = {'author':                   'Hamburg',
                   'author_email':             'hh@hamburg.de',
                   'groups':                   ['transport-und-verkehr'],
                   'tags':                   ['some tag', 'GovData'],
                   'license_id':               'cc-by',
                   'point_of_contact':         None,
                   'point_of_contact_address': {'email': None},
                   'type': 'datensatz',
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': [{'key': 'metadata_original_portal', 'value': None}]}

        valid = harvester.amend_package(package)
        extras = Extras(package['extras'])
        self.assertTrue(extras.key('metadata_original_portal'))
        self.assertEquals(default, extras.value('metadata_original_portal'))
        self.assertTrue(valid)

    def test_amend_portal_metadata_original_portal_different(self):

        harvester = HamburgCKANHarvester()
        default = 'http://suche.transparenz.hamburg.de/'

        package = {'author':                   'Hamburg',
                   'author_email':             'hh@hamburg.de',
                   'groups':                   ['transport-und-verkehr'],
                   'tags':                   ['some tag', 'GovData'],
                   'license_id':               'cc-by',
                   'point_of_contact':         None,
                   'point_of_contact_address': {'email': None},
                   'type': 'datensatz',
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': [{'key': 'metadata_original_portal', 'value': 'www.example.com'}]}

        valid = harvester.amend_package(package)
        extras = Extras(package['extras'])
        self.assertTrue(extras.key('metadata_original_portal'))
        self.assertEquals(default, extras.value('metadata_original_portal'))
        self.assertTrue(valid)
