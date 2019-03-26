import sys, os

from setuptools import setup, find_packages

VERSION = '3.3.0'

with open('base-requirements.txt') as f:
    required = [line.strip() for line in f]

setup(
  name='ckanext-govdatade',
  version=VERSION,
  description="GovData.de specific CKAN extension",
  long_description="""\
  """,
  classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
  keywords='',
  author='SEITENBAU GmbH',
  author_email='info@seitenbau.com',
  url='https://github.com/GovDataOfficial/ckanext-govdatade',
  license='AGPL',
  packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
  exclude_package_data = {'': ['.gitignore', '.travis.yml', '.gitattributes', 'bin/*']},
  namespace_packages=['ckanext', 'ckanext.govdatade'],
  install_requires=required,
  include_package_data=True,
  zip_safe=False,
  entry_points=\
    """
    [ckan.plugins]
    hamburg_harvester=ckanext.govdatade.harvesters.ckanharvester:HamburgCKANHarvester
    rlp_harvester=ckanext.govdatade.harvesters.ckanharvester:RlpCKANHarvester
    berlin_harvester=ckanext.govdatade.harvesters.ckanharvester:BerlinCKANHarvester
    datahub_harvester=ckanext.govdatade.harvesters.ckanharvester:DatahubCKANHarvester
    rostock_harvester=ckanext.govdatade.harvesters.ckanharvester:RostockCKANHarvester
    opennrw_harvester=ckanext.govdatade.harvesters.ckanharvester:OpenNrwCKANHarvester
    bremen_harvester=ckanext.govdatade.harvesters.jsonharvester:BremenCKANHarvester
    gdi_harvester=ckanext.govdatade.harvesters.jsonharvester:GdiHarvester
    genesis_destatis_harvester=ckanext.govdatade.harvesters.jsonharvester:GenesisDestatisZipHarvester
    destatis_harvester=ckanext.govdatade.harvesters.jsonharvester:DestatisZipHarvester
    regionalstatistik_harvester=ckanext.govdatade.harvesters.jsonharvester:RegionalstatistikZipHarvester
    sachsen_harvester=ckanext.govdatade.harvesters.jsonharvester:SachsenZipHarvester
    bmbf_harvester=ckanext.govdatade.harvesters.jsonharvester:BmbfZipHarvester
    bfj_harvester=ckanext.govdatade.harvesters.jsonharvester:BfjHarvester

    [paste.paster_command]
    linkchecker = ckanext.govdatade.commands.linkchecker:LinkChecker
    report = ckanext.govdatade.commands.report:Report
    groupadder = ckanext.govdatade.commands.groupadder:GroupAdder
    purge = ckanext.govdatade.commands.purge:Purge
    cleanupdb = ckanext.govdatade.commands.cleanupdb:CleanUpDb

    [nose.plugins]
    pylons = pylons.test:PylonsPlugin
    """,
)
