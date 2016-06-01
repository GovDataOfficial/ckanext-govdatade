#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.govdatade.harvesters.jsonharvester import JSONZipBaseHarvester
from codecs import BOM_UTF8

import json
import unittest


class JsonHarvesterTest(unittest.TestCase):

    def test_bom_is_stripped(self):

        bom = BOM_UTF8
        harvester = JSONZipBaseHarvester()
        content = json.dumps({'a': '1', 'b': '2', 'c': '3'})
        bom_content = bom + content

        bom_free_content = harvester.lstrip_bom(bom_content)
        self.assertFalse(bom_free_content.startswith(bom))

        self.assertTrue(bom_content.startswith(bom))

    def test_non_bom_content_is_returned_as_is(self):
        harvester = JSONZipBaseHarvester()
        content = json.dumps({'a': '1', 'b': '2', 'c': '3'})

        self.assertEquals(content, harvester.lstrip_bom(content))
