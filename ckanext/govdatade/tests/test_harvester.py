#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.govdatade.harvesters.ckanharvester import GovDataHarvester
from ckanext.govdatade.harvesters.ckanharvester import BerlinCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import GroupCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import RlpCKANHarvester

import os
import json
import unittest


class GovDataHarvesterTest(unittest.TestCase):

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

class GroupCKANHarvesterTest(unittest.TestCase):

    def test_cleanse_tags_comma_separated(self):

        harvester = GroupCKANHarvester()
        tags = ['tagone, tagtwo']

        cleansed_tags = harvester.cleanse_tags(tags)
        self.assertEqual(cleansed_tags, ['tagone-tagtwo'])

        tags = 'tagone, tagtwo, tagthree, tagfour'

        cleansed_tags = harvester.cleanse_tags(tags)
        self.assertEqual(cleansed_tags, 'tagone-tagtwo-tagthree-tagfour')

    def test_cleanse_tags_special_character(self):
        harvester = GroupCKANHarvester()

        tags = ['tag/one, tag/two, Tag/three']
        cleansed_tags = harvester.cleanse_tags(tags)

        self.assertEqual(cleansed_tags, ['tagone-tagtwo-tagthree'])

        tags = [
            'tag/one&$#?+\\',
            'tag/two',
            '/+#`?tag/////three',
            'tag.four',
            'tag-five',
            'tag_six',
            'tag/one, tag/two, Tag/three',
            u'Bevölkerung',
            'tag/one, tag/two,Tag/three',
            'Umwelt, Lebensmittel, Futtermittel, Strahlenschutzvorsorge',
        ]
        cleansed_tags = harvester.cleanse_tags(tags)

        self.assertEqual(cleansed_tags, [
            'tagone',
            'tagtwo',
            'tagthree',
            'tag.four',
            'tag-five',
            'tag_six',
            'tagone-tagtwo-tagthree',
            u'bevölkerung',
            'tagone-tagtwotagthree',
            'umwelt-lebensmittel-futtermittel-strahlenschutzvorsorge',
        ])

    def test_cleanse_tags_replace_whitespace_characters(self):
        harvester = GroupCKANHarvester()

        tags = ['tag  one', 'tag two']
        cleansed_tags = harvester.cleanse_tags(tags)

        self.assertEqual(cleansed_tags, ['tag--one', 'tag-two'])

class BerlinHarvesterTest(unittest.TestCase):

    def test_tags_are_cleansed_when_amending(self):
        harvester = BerlinCKANHarvester()

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'tags': ['tag/one&$#?+\\', 'tag/two'],
                   'extras': {'metadata_original_portal': None}}

        harvester.amend_package(dataset)
        self.assertTrue('tags' in dataset)
        self.assertEqual(dataset['tags'], ['tagone', 'tagtwo'])

    def test_tags_are_not_cleansed_when_not_present(self):
        harvester = BerlinCKANHarvester()

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(dataset)
        self.assertFalse('tags' in dataset)
        self.assertTrue(valid)

    def test_sector_amendment(self):

        harvester = BerlinCKANHarvester()

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'oeffentlich')
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None,
                              'sector':                   None}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'oeffentlich')
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None,
                              'sector':                   'privat'}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'privat')
        self.assertFalse(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None,
                              'sector':                   'andere'}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'andere')
        self.assertFalse(valid)

    def test_type_amendment(self):

        harvester = BerlinCKANHarvester()

        package = {'type': None,
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

        package = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

        package = {'type': 'dokument',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'dokument')
        self.assertTrue(valid)

        package = {'type': 'app',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'app')
        self.assertTrue(valid)

        package = {'type': 'garbage',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

    def test_amend_portal(self):

        harvester = BerlinCKANHarvester()
        default = 'http://datenregister.berlin.de'

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {}}

        valid = harvester.amend_package(dataset)
        portal = dataset['extras']['metadata_original_portal']
        self.assertEqual(portal, default)
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(dataset)
        portal = dataset['extras']['metadata_original_portal']
        self.assertEqual(portal, default)
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': 'www.example.com'}}

        valid = harvester.amend_package(dataset)
        portal = dataset['extras']['metadata_original_portal']
        self.assertEqual(portal, 'www.example.com')
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
                   'extras': {},
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
        self.assertTrue('wirtschaft_arbeit' in package['groups'])
        self.assertTrue('geo' in package['groups'])
        self.assertTrue('umwelt_klima' in package['groups'])
        self.assertTrue('infrastruktur_bauen_wohnen' in package['groups'])

class RlpHarvesterTest(unittest.TestCase):

    def test_gdi_rlp_package(self):

        package = {'author':                   'RLP',
                   'author_email':             'rlp@rlp.de',
                   'groups':                   ['gdi-rp', 'geo'],
                   'license_id':               'cc-by',
                   'point_of_contact':         None,
                   'point_of_contact_address': {'email': None},
                   'resources':                [{'format': 'pdf'}],
                   'type':                     None,
                   'extras':                   {'content_type': 'Kartenebene',
                                                'terms_of_use': {'license_id':
                                                                 'cc-by'}}}

        harvester = RlpCKANHarvester()
        harvester.amend_package(package)
        self.assertNotIn('gdi-rp', package['groups'])
        self.assertIn('geo', package['groups'])
        self.assertEqual(package['type'], 'datensatz')
