#!/usr/bin/python
# -*- coding: utf8 -*-
from ckanext.govdatade.config import config
from nose.tools import raises

import unittest


class TestConfig(unittest.TestCase):

    def test_get_succesful_harvester_url(self):
        hamburg_url_dict = config.get_harvester_urls('hamburg_harvester')
        self.assertEquals(
            hamburg_url_dict,
            {
                "source_url": 'http://suche.transparenz.hamburg.de/',
                "portal_url": 'http://suche.transparenz.hamburg.de/',
            }
        )

    @raises(KeyError)
    def test_raises_expected_exception_when_not_resolvable(self):
        config.get_harvester_urls('test-stadt')
