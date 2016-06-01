#!/usr/bin/python
# -*- coding: utf8 -*-
from ckanext.govdatade.config import config
from ckanext.govdatade.harvesters.translator import translate_groups

import json
import unittest
import urllib2


class TestTranslator(unittest.TestCase):

    def test_unmapable_group(self):
        translate_result = translate_groups(
            ['test-unmapable-1', 'test-unmapable-2'],
            'hamburg'
        )
        self.assertEquals(
            translate_result,
            []
        )

    def test_mapable_group(self):
        translate_result = translate_groups(
            ['bevolkerung', 'umwelt-und-klima'],
            'hamburg'
        )

        self.assertEquals(
            type(translate_result).__name__,
            'list'
        )
        self.assertEquals(
            translate_result.__len__(),
            2
        )
        self.assertEquals(
            translate_result,
            [u'bevoelkerung', u'umwelt_klima']
        )
