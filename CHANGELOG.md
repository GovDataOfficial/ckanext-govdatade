# Changelog

## v6.9.0 2025-03-24
* Fixes dry-run option for CKAN command `delete`

## v6.1.0 2023-08-01

* Updates and cleans up dependencies
* Standardization of the `test.ini` file

## v6.0.0 2023-07-05

* Removes support for old CKAN versions prior 2.9 and Python 2

## v5.13.0 2023-05-04

* Adds support for CKAN 2.10.0

## v5.9.0 2023-01-23

* Removes version strings from the development dependencies `isort` and `httpretty`

## v5.6.0 2022-11-03

* Updates pylint configuration to latest version and fixes several warnings

## v5.4.0 2022-09-12

* Internal release: Switches Python environment from Python 3.6 to Python 3.8 and updating deployment scripts

## v5.1.0 2022-04-07

* Support for Python 3

## v4.7.1 2022-03-10

* Updates Jinja2 to match version in ckan requirements
* Fix loading of old values from redis

## v4.6.4 2021-12-21

* Adds support for Redis 3.x
* Fixes dev-requirements.txt: Broken version 1.7.0 of lazy-object-proxy was banned

## v4.5.6 2021-09-30

* Changes metadata export from daily to weekly

## v4.5.4 2021-09-10

* Removed deprecated OGD schemachecker completely

## v4.4.2 2021-05-20

* Fix dataset URLs in link checker reports and remove deprecated OGD schema check reporting

## v3.11.0 2020-09-29

* Removed the deprecated code for the OGD harvesters
* Removed the deprecated `groupadder` command for OGD categories

## v3.9.2 2020-04-30

* Linkchecker: Avoid re-using context when calling CKAN-Action multiple
* Linkchecker: Add additional debug logging

## v3.7.0 2019-12-19

* Rename environment names for internal ci/cd pipeline
* Remove the restriction to a specific version of CKAN

## v3.4.1 2019-05-16

* Added `delete` paster command for deleting datasets by package_search filter params

## v3.3.0 2019-03-12

* Improve filter query in `cleanupdb` command
* Moved supervisor config for harvesting `gather_consumer` and `fetch_consumer` to ckanext-dcatde
* Moved cronjob scripts to run and clear harvest jobs to ckanext-dcatde
* Remove patches and requirements subfolder

## v3.2.0 2018-12-21

* Allow more than 200 rows in the link checker report

## v3.1.3 2018-11-09

* Re-activate Link-Checker by adding paster commands again

## v3.1.1 2018-04-27

* Use dct:identifier (extras.identifier) instead of adms:identfier (extras.alternate_identifier) for
    * the mapping from OGD to DCAT-AP.de
    * the duplicate detection

## v3.1.0 2018-03-20

* The OGD harvester is adding a guid in package extras
* The dependency to ckanext-harvest was removed
* Added hint about the new dependency to ckanext-dcatde (since version 3.0.1) in the README file

## v3.0.1 2017-12-20

* Harvests OGD source portals to DCAT-AP.de CKAN fields now
* Deactivated schema and link checker (removed paster commands in setup file)
* Added `cleanupdb` paster command for deleting activities of user 'harvest' in the CKAN database
* Added shell script for purging OGD CKAN groups

## v2.4.5 2017-02-07

* Preserve character 'ß' in tags
* Added new Creative Commons 3.0 licence bundle

## v2.4.4 2016-11-18

* Upgraded dependency of the library ckanext-harvest from branch "release-v2.0" to release "v0.0.5"
* Improved deletion of deprecated datasets from remote
* Improved error handling in purge command

## v2.4.3 2016-09-19

* Added automated synchronization of datasets with the remote endpoints while harvesting.
* Fixed schema checker:
    * Deleted datasets will be removed now from report data source, if the dataset contains schema validation errors and not available links
    * Cast field "temporal_granularity_factor" to number before validating them
* Little imrovements/fixes for the harvesters of Hamburg, Berlin and Open.NRW

## v2.4.2 2016-09-16

* Added automated clearing harvest source job history without deleting all harvested/related datasets.

## v2.4.1 2016-08-15

* Support for CKAN 2.5.x
* Added licences "geonutz-be-2013-10-01" and "cc-pdm-1.0" to JSON file for schema validation

## v2.3.0 2016-06-01

* Initial commit "Regelbetrieb" (Version 2.3.0)
