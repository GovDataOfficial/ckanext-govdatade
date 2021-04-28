#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from ckanext.govdatade.util import normalize_extras


def test_simple_json_object():
    source = json.dumps({'a': '1', 'b': '2', 'c': '3'})
    assert normalize_extras(source) == {'a': '1', 'b': '2', 'c': '3'}


def test_nested_json_object():
    source = json.dumps({'a': {'b': {'c': {}}}})
    assert normalize_extras(source) == {'a': {'b': {'c': {}}}}


def test_simple_json_array():
    source = json.dumps(['1', '2', '3'])
    assert normalize_extras(source) == ['1', '2', '3']


def test_nested_json_array():
    array = ['1', '2', '3']
    source = json.dumps([array] * 3)
    assert normalize_extras(source) == [['1', '2', '3']] * 3


def test_invalid_json():
    source = 'test'
    assert normalize_extras(source) == 'test'


def test_string_encoded_integer():
    source = json.dumps({'a': '1'})
    assert normalize_extras(source) == {'a': '1'}


def test_string_encoded_float():
    source = json.dumps({'a': '3.5'})
    assert normalize_extras(source) == {'a': '3.5'}


def test_string_encoded_boolean():
    source = json.dumps({'a': 'true'})
    assert normalize_extras(source) == {'a': 'true'}


def test_string_encoded_string():
    source = json.dumps({'a': '"string"'})
    assert normalize_extras(source) == {'a': 'string'}
