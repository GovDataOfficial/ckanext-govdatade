#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.govdatade.harvesters.jsonharvester import JSONZipBaseHarvester, JSONDumpBaseCKANHarvester
from codecs import BOM_UTF8
from mock import patch, Mock

import httpretty
import json
import StringIO
import unittest
import zipfile


SQL_PACKAGE_ID = "123456"

class DummyClass:
    pass

def create_harvest_job(self, source_id, source_url):
    source = DummyClass()
    source.id = source_id
    source.config = {}
    source.url = source_url
    harvest_job = DummyClass()
    harvest_job.source = source
    return harvest_job

def mock_save(self):
    self.id = SQL_PACKAGE_ID


class JSONZipBaseHarvesterTest(unittest.TestCase):

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

    @httpretty.activate
    @patch("ckan.model.DomainObject.save", mock_save)
    @patch('ckanext.govdatade.harvesters.jsonharvester.JSONDumpBaseCKANHarvester.delete_deprecated_datasets')
    def test_gather_stage(self, mock_super_delete):
        # prepare
        harvester = JSONZipBaseHarvester()
        source_id = 'xyz'
        source_url = 'http://test.de/ckan.zip'
        harvest_job = create_harvest_job(self, source_id, source_url)

        httpretty.HTTPretty.allow_net_connect = False
        org_id = '1234567890'
        package1 = {
                       'id': 'abc',
                       'name': 'package-1',
                       'owner_org': org_id}
        package2 = {
                       'id': 'efg',
                       'name': 'package-2',
                       'owner_org': org_id}
        # Fake ZIP-File
        in_memory_zip = StringIO.StringIO()
        with zipfile.ZipFile(in_memory_zip, 'w') as myzip:
            myzip.writestr('package1.json', json.dumps(package1, encoding='ascii'))
            myzip.writestr('package2.json', json.dumps(package2, encoding='ascii'))
            myzip.writestr('eggs.txt', "Hello world!")
        response = in_memory_zip.getvalue()
        httpretty.register_uri(httpretty.GET, source_url, status=200, body=response)

        # execute
        harvester.gather_stage(harvest_job)

        # verify
        self.assertTrue(httpretty.has_request())
        remote_packages = [package1, package2]
        mock_super_delete.assert_called_once_with(remote_packages, harvest_job)


class JSONDumpBaseCKANHarvesterTest(unittest.TestCase):

    @httpretty.activate
    @patch("ckanext.govdatade.harvesters.ckanharvester.GovDataHarvester._set_config")
    @patch("ckan.model.DomainObject.save", mock_save)
    def test_gather_stage(self, mock_super_config):
        # prepare
        harvester = JSONDumpBaseCKANHarvester()
        source_id = 'xyz'
        source_url = 'http://test.de/ckan-dump'
        harvest_job = create_harvest_job(self, source_id, source_url)

        harvester.config = harvest_job.source.config # needed in self._get_content
        httpretty.HTTPretty.allow_net_connect = False
        org_id = '1234567890'
        package1 = {
                       'id': 'abc',
                       'name': 'package-1',
                       'owner_org': org_id}
        package2 = {
                       'id': 'efg',
                       'name': 'package-2',
                       'owner_org': org_id}
        response = json.dumps([package1, package2])
        httpretty.register_uri(httpretty.GET, source_url, status=200, body=response)

        harvester.delete_deprecated_datasets = Mock()

        # execute
        result = harvester.gather_stage(harvest_job)

        # verify
        self.assertEqual(result, [SQL_PACKAGE_ID, SQL_PACKAGE_ID])
        mock_super_config.assert_called_once_with(harvester.config)
        self.assertTrue(httpretty.has_request())
        remote_packages = [package1, package2]
        harvester.delete_deprecated_datasets.assert_called_once_with(remote_packages, harvest_job)

    @httpretty.activate
    @patch("ckanext.govdatade.harvesters.ckanharvester.GovDataHarvester.delete_deprecated_datasets")
    def test_delete_deprecated_datasets(self, mock_super):
        # prepare
        harvester = JSONDumpBaseCKANHarvester()
        source_id = 'xyz'
        source_url = 'http://test.de/ckan-dump'
        harvest_job = create_harvest_job(self, source_id, source_url)

        org_id = '1234567890'
        package1_id = 'abc'
        package1 = {
                       'id': package1_id,
                       'name': 'package-1',
                       'owner_org': org_id}
        package2_id = 'efg'
        package2 = {
                       'id': package2_id,
                       'name': 'package-2',
                       'owner_org': org_id}

        # execute
        harvester.delete_deprecated_datasets([package1, package2], harvest_job)

        # verify
        package_ids_generated_from_name = ['a48ef665-50dd-5a35-b439-285d6cb84b05',
                                           '2042fcbd-432f-5542-88b5-fc95a181c5c3']
        mock_super.assert_called_once_with(package_ids_generated_from_name, harvest_job)
