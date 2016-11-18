#!/usr/bin/python
# -*- coding: utf8 -*-
from ckanext.govdatade.harvesters.translator import translate_groups

import unittest


class TestTranslator(unittest.TestCase):

    def test_unmapable_group_flat_list(self):
        translate_result = translate_groups(
            ['test-unmapable-1', 'test-unmapable-2'],
            'hamburg'
        )
        self.assertEquals(
            translate_result,
            []
        )

    def test_unmapable_group_dict_list(self):
        dict_list = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "Group 1"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "Group 2"
        }]

        translate_result = translate_groups(
            dict_list,
            'hamburg'
        )
        self.assertEquals(
            translate_result,
            []
        )

    def test_mapable_group_flat_list(self):
        translate_result = translate_groups(
            ['bevolkerung', 'umwelt-und-klima', 'transport-und-verkehr'],
            'hamburg'
        )

        self.assertEquals(
            type(translate_result).__name__,
            'list'
        )
        self.assertEquals(
            translate_result.__len__(),
            3
        )
        self.assertEquals(
            translate_result,
            [{'id': u'bevoelkerung', 'name': u'bevoelkerung'},
             {'id': u'umwelt_klima', 'name': u'umwelt_klima'},
             {'id': u'transport_verkehr', 'name': u'transport_verkehr'}]
        )

    def test_mapable_group_dict_list(self):
        dict_list = [{
            "vocabulary_id": 1,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "bevolkerung"
        }, {
            "vocabulary_id": 2,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "umwelt-und-klima"
        }, {
            "vocabulary_id": 3,
            "state": "active",
            "display_name": "offene-daten-k\u00f6ln",
            "id": "07767723-df63-44fa-8bb1-002cf932c2f6",
            "name": "transport-und-verkehr"
        }]

        translate_result = translate_groups(
            dict_list,
            'hamburg'
        )

        self.assertEquals(
            type(translate_result).__name__,
            'list'
        )
        self.assertEquals(
            translate_result.__len__(),
            3
        )
        self.assertEquals(
            translate_result,
            [{'id': u'bevoelkerung', 'name': u'bevoelkerung'},
             {'id': u'umwelt_klima', 'name': u'umwelt_klima'},
             {'id': u'transport_verkehr', 'name': u'transport_verkehr'}]
        )
