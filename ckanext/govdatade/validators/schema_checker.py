import ast
import logging
import redis

from jsonschema.validators import Draft3Validator
from jsonschema import FormatChecker


class SchemaChecker(object):
    '''
    Class providing the actual schema validation logic.
    '''

    SCHEMA_RECORD_KEY = 'schema'

    def __init__(self, config, schema=None):
        self.schema = schema

        self.redis_client = redis.StrictRedis(
            host=config.get('ckanext.govdata.validators.redis.host'),
            port=int(config.get('ckanext.govdata.validators.redis.port')),
            db=int(config.get('ckanext.govdata.validators.redis.database'))
        )
        self.logger = logging.getLogger(
            'ckanext.govdatade.reports.validators.schemachecker'
        )

    def process_record(self, dataset):
        '''
        Validates a given dataset
        '''
        dataset_id = dataset['id']
        dataset_name = dataset['name']
        dataset_maintainer = dataset['maintainer'] if 'maintainer' in dataset else ''
        portal = dataset['extras'].get('metadata_original_portal', 'null').lower()
        record = self.redis_client.get(dataset_id)

        if record is not None:
            try:
                record = ast.literal_eval(record)
                record['name'] = dataset_name
                record['maintainer'] = dataset_maintainer
                record['metadata_original_portal'] = portal
                self.logger.debug('Record id: %s', record['id'])
            except ValueError:
                self.logger.error(
                    'Redis dataset record evaluation error: %s',
                    record
                )

        if record is None:
            record = {
                'id': dataset_id,
                'name': dataset_name,
                'maintainer': dataset_maintainer,
                'metadata_original_portal': portal}
            record[self.SCHEMA_RECORD_KEY] = []

        broken_rules = []

        format_checker = FormatChecker(('date-time',))
        if not Draft3Validator(self.schema, format_checker=format_checker).is_valid(dataset):
            errors = Draft3Validator(self.schema, format_checker=format_checker).iter_errors(dataset)

            for error in errors:
                path = [e for e in error.path if isinstance(e, basestring)]
                path = str('.'.join(map((lambda e: str(e)), path)))

                field_path_message = [path, error.message]
                broken_rules.append(field_path_message)

        dataset_groups = dataset['groups']

        if len(dataset_groups) >= 4:
            path = 'groups'
            field_path_message = [path, 'WARNING: too many groups set']
            broken_rules.append(field_path_message)

        self.logger.debug('Broken rules id: %s', broken_rules)

        try:
            record[self.SCHEMA_RECORD_KEY] = broken_rules
        except:
            self.logger.error('Schema broken rules error')

        self.redis_client.set(dataset_id, record)

        return not broken_rules

    def get_records(self):
        '''
        Returns the dataset records from Redis
        '''
        records = []
        for dataset_id in self.redis_client.keys('*'):
            if dataset_id == 'general':
                continue
            try:
                records.append(
                    ast.literal_eval(self.redis_client.get(dataset_id))
                )
            except ValueError:
                self.logger.error('Data set error: %s', dataset_id)

        return records
