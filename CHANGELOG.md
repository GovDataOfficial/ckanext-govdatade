# Changelog

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
