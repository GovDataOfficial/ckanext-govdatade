'''
Module for accessing and mutating the configuration.
'''
import logging
import json
import urllib2
import pylons.config

log = logging.getLogger(__name__)


class HarvesterConfig:
    conf = dict()

    def set(self, key, val):
        HarvesterConfig.conf[key] = val

    def get(self, key, default=None):
        if key in HarvesterConfig.conf:
            return HarvesterConfig.conf.get(key)
        elif key in pylons.config:
            return pylons.config.get(key)
        else:
            return default

    def get_harvester_urls(self, harvester_name):
        '''Gets the configured harvester URLs dict (source_url, portal_url).'''

        mapping_file = config.get(
            'ckanext.govdata.harvester.source.portal.mappings.file'
        )

        json_string = urllib2.urlopen(mapping_file).read()
        url_dict = json.loads(json_string)

        if harvester_name in url_dict:
            return url_dict[harvester_name]

        value_error = "No URLs configured for '{harvester_name}'.".format(
            harvester_name=harvester_name
        )
        raise KeyError(value_error)

config = HarvesterConfig()
