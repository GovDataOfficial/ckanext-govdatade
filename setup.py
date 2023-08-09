import sys, os

from setuptools import setup, find_packages

VERSION = '6.1.0'

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
    govdatade=ckanext.govdatade.plugins:GovDataDePlugin
    """,
)
