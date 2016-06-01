#!/usr/bin/python
# -*- coding: utf8 -*-
from nose.tools import raises
from jsonschema import validate, ValidationError
from ckanext.govdatade.config import config

import json
import urllib2


class TestValidation:

    def setup(self):
        schema_url = config.get('ckanext.govdata.urls.schema')
        self.schema = json.loads(urllib2.urlopen(schema_url).read())

    @raises(ValidationError)
    def test_empty_package(self):
        validate({}, self.schema)

    def test_minimal_package(self):
        package = {'name': 'statistiken-2013',
                   'author': 'Eric Walter',
                   'notes': 'Statistiken von 2013.',
                   'title': 'Statistiken 2013',
                   'resources': [],
                   'groups': ['verwaltung'],
                   'license_id': 'cc-zero',
                   'type': 'app',
                   'extras': {'dates': []}}

        validate(package, self.schema)
