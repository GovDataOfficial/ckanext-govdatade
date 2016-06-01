#!/usr/bin/python
# -*- coding: utf8 -*-

import unittest

from ckanext.govdatade.harvesters.ckanharvester import DatahubCKANHarvester
from ckanext.govdatade.config import config


class DataHubIOHarvesterTest(unittest.TestCase):

    def test_amend_package(self):
        harvester = DatahubCKANHarvester()
        package = {'name': 'Package Name',
                   'description': '   ',
                   'groups': ['bibliographic', 'lld', 'bibsoup'],
                   'resources': [],
                   'extras': {}
                   }

        harvester.amend_package(package)
        portal = package['extras']['metadata_original_portal']
        self.assertEqual(portal, 'http://datahub.io/')
        self.assertEqual(
            package['groups'],
            ['bibliographic', 'lld', 'bibsoup', 'bildung_wissenschaft']
        )
        self.assertEqual(package['type'], 'datensatz')
