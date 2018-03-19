#!/usr/bin/python
# -*- coding: utf8 -*-

import unittest

from ckanext.govdatade.extras import Extras
from ckanext.govdatade.harvesters.ckanharvester import DatahubCKANHarvester


class DataHubIOHarvesterTest(unittest.TestCase):

    def test_amend_package(self):
        harvester = DatahubCKANHarvester()
        portal = 'http://datahub.io/'
        package = {
            'id': 'package-id',
            'name': 'package-name',
            'description': '   ',
            'groups': [
                {'name': 'bibliographic'},
                {'name': 'lld'},
                {'name': 'bibsoup'},
            ],
            'resources': [],
            'extras': []
        }

        valid = harvester.amend_package(package)

        extras = Extras(package['extras'])

        self.assertEqual(package['id'], 'package-id')
        self.assertEqual(package['name'], 'package-name')
        self.assertTrue(extras.key('metadata_original_portal'))
        self.assertEquals(portal, extras.value('metadata_original_portal'))
        self.assertTrue(valid)
        self.assertListEqual(package['groups'],
            [{'id': 'bibliographic', 'name': 'bibliographic'},
             {'id': 'lld', 'name': 'lld'},
             {'id': 'bibsoup', 'name': 'bibsoup'},
             {'id': 'bildung_wissenschaft', 'name': 'bildung_wissenschaft'}]
        )
        self.assertEqual(package['type'], 'datensatz')
        self.assertEqual(extras.value('guid'), 'package-id')
