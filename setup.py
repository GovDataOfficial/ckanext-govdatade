from setuptools import setup, find_packages
import sys, os

VERSION = '1.5.0-alpha3'

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
  author='Fraunhofer FOKUS',
  author_email='ogdd-harvesting@fokus.fraunhofer.de',
  url='https://github.com/fraunhoferfokus/ckanext-govdatade',
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
    schemachecker = ckanext.govdatade.commands.schemachecker:SchemaChecker
    linkchecker = ckanext.govdatade.commands.linkchecker:LinkChecker
    report = ckanext.govdatade.commands.report:Report
    groupadder = ckanext.govdatade.commands.groupadder:GroupAdder

    [nose.plugins]
    pylons = pylons.test:PylonsPlugin
    """,
)
