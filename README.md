# ckanext-govdatade

GovData.de specific CKAN extension contains some helpful tools, e.g. a link checker for testing the
availability of the provided links in the resources.

## Getting Started

If you are using Python virtual environment (virtualenv), activate it.

```bash
# use project on GitHub
$ cd /path/to/virtualenv
$ /path/to/virtualenv/bin/pip install -e git+git://github.com/GovDataOfficial/ckanext-govdatade.git#egg=ckanext-govdatade
$ cd src/ckanext-govdatade
$ /path/to/virtualenv/bin/pip install -r base-requirements.txt -f requirements
$ python setup.py develop
```

or

```bash
# use project on Open CoDE
$ cd /path/to/virtualenv
$ /path/to/virtualenv/bin/pip install -e git+git://gitlab.opencode.de/fitko/govdata/ckanext-govdatade.git#egg=ckanext-govdatade
$ cd src/ckanext-govdatade
$ /path/to/virtualenv/bin/pip install -r base-requirements.txt -f requirements
$ python setup.py develop
```

Add the following plugins to your CKAN configuration file:

```ini
ckan.plugins = govdatade
```
## Command line interface
The following operations can be run from the command line as described underneath:

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini cleanupdb activities

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini delete datasets title:"to delete"

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini purge deleted

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini linkchecker

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini report

The commands should be run with the pyenv activated and refer to your CKAN configuration file.

## Testing

Unit tests are placed in the `ckanext/govdatade/tests` directory and can be run with the pytest unit testing framework:

```bash
$ cd /path/to/virtualenv/src/ckanext-govdatade
$ pytest
```
