# ckanext-govdatade

GovData.de specific CKAN extension for importing data from several remote sources.

### Dependencies

The GovData.de harvester is based on the CKAN extension [ckanext-harvest](https://github.com/ckan/ckanext-harvest).

As harvested data is converted into DCAT format, the [ckanext-dcatde](https://github.com/GovDataOfficial/ckanext-dcatde) is required.
*You do not have to install ckanext-harvest by yourself here, as it is a dependency of ckanext-dcat.*

## Getting Started

If you are using Python virtual environment (virtualenv), activate it.

Install the current version of ckanext-dcatde.

```bash
$ cd /path/to/virtualenv
$ /path/to/virtualenv/bin/pip install -e git+git://github.com/GovDataOfficial/ckanext-govdatade.git#egg=ckanext-govdatade
$ cd src/ckanext-govdatade
$ /path/to/virtualenv/bin/pip install -r base-requirements.txt -f requirements
$ python setup.py develop
```

Add the following plugins to your CKAN configuration file:

```ini
ckan.plugins = stats ckan_harvester hamburg_harvester rlp_harvester berlin_harvester datahub_harvester rostock_harvester opennrw_harvester bremen_harvester gdi_harvester genesis_destatis_harvester destatis_harvester regionalstatistik_harvester sachsen_harvester bmbf_harvester bfj_harvester
```

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
