# ckanext-govdatade

GovData.de specific CKAN extension for importing data from several remote sources.

### Dependencies

The GovData.de harvester is based on the CKAN extension [ckanext-harvest](https://github.com/ckan/ckanext-harvest).

## Getting Started

If you are using Python virtual environment (virtualenv), activate it.

Install a specific version of the CKAN extension ckanext-harvest. It is tested that ckanext-govdatade is working well with the branch release-v2.0 of ckanext-harvest.

```bash
$ cd /path/to/virtualenv
$ /path/to/virtualenv/bin/pip install -e git+git://github.com/GovDataOfficial/ckanext-govdatade.git#egg=ckanext-govdatade
$ cd src/ckanext-govdatade
$ /path/to/virtualenv/bin/pip install -r base-requirements.txt -f requirements
$ python setup.py develop
```

Add the following plugins to your CKAN configuration file:

```ini
ckan.plugins = stats harvest ckan_harvester hamburg_harvester rlp_harvester berlin_harvester datahub_harvester rostock_harvester opennrw_harvester bremen_harvester gdi_harvester genesis_destatis_harvester destatis_harvester regionalstatistik_harvester sachsen_harvester bmbf_harvester bfj_harvester
```

Init the harvest tables in the database

```bash
$ path/to/virtualenv/bin/paster --plugin=ckanext-harvest harvester initdb --config=mysite.ini
```

Create the harvest user

- create ckan harvest user

    sudo -u ckan /path/to/virtualenv/bin/paster --plugin=ckan user add harvest password=harvest email=harvest@example.com --config=/etc/ckan/default/production.ini
  
- give sysadmin privileges to ckan harvest user

    sudo -u ckan /path/to/virtualenv/bin/paster --plugin=ckan sysadmin add harvest --config=/etc/ckan/default/production.ini

## Creating ogd conform groups
If you want to create the standard open data groups you can use the ckan command "groupadder" by following the instructions:

    source /path/to/ckan/env/bin/activate
    sudo -u ckan /path/to/ckan/env/bin/paster --plugin=ckanext-govdatade groupadder --config=/etc/ckan/default/production.ini

## Testing

Unit tests are placed in the `ckanext/govdatade/tests` directory and can be run with the nose unit testing framework:

```bash
$ cd /path/to/virtualenv/src/ckanext-govdatade
$ nosetests
```
