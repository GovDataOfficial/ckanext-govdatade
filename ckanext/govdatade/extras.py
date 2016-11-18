'''
Module for accessing and mutating the extras.
'''
import logging

LOGGER = logging.getLogger(__name__)


class Extras(object):
    '''
    Wrapper around the extras data structure.
    '''

    def __init__(self, extras):
        self.extras = extras

    def get(self):
        '''
        Accessor for the extras data structure.
        '''
        return self.extras

    def len(self):
        '''
        Returns the length of the extras.
        '''
        if self.extras is None:
            return 0

        if isinstance(self.extras, dict):
            return len(self.extras.keys())

        if isinstance(self.extras, list):
            return len(self.extras)

        return 0

    def key(self, key, disallow_empty=False):
        '''
        Checks if the given key exists.
        When disallow_empty is set and the
        value of key is '' or None the key is
        considered non existent.
        '''
        if self.extras is None:
            return False

        if key in self.extras:
            if disallow_empty and not self.extras[key]:
                return False

            return True

        # Handle list of dicts
        if isinstance(self.extras, list) and len(self.extras) >= 1:
            for extra in self.extras:
                if isinstance(extra, dict) and extra['key'] == key:
                    value = extra.get('value')
                    if isinstance(value, str):
                        value = value.strip()
                    if disallow_empty and not value:
                        return False
                    return True

        return False

    def value(self, key, default=None):
        '''
        Returns the value for the given key. When the
        key is not found a default is returned.
        '''
        if not self.key(key) and default is None:
            raise KeyError

        if key in self.extras:
            return self.extras.get(key)

        # Handle list of dicts
        if isinstance(self.extras, list) and len(self.extras) >= 1:
            for extra in self.extras:
                if isinstance(extra, dict) and extra['key'] == key:
                    return extra.get('value')

        return default

    def update(self, key, value, upsert=False):
        '''
        Updates the value of the given key.
        '''
        if not self.key(key) and upsert is False:
            raise KeyError

        if key in self.extras:
            self.extras[key] = value
            return True

        # Handle list of dicts
        if isinstance(self.extras, list) and len(self.extras) >= 1:
            for extra in self.extras:
                if isinstance(extra, dict) and extra['key'] == key:
                    extra['value'] = value
                    return True

        if upsert:
            self.extras.append({'key': key, 'value': value})
            return True

        return False

    def remove(self, key):
        '''
        Removes the give key.
        '''
        if not self.key(key):
            raise KeyError

        if key in self.extras:
            del self.extras[key]
            return True

        # Handle list of dicts
        if isinstance(self.extras, list) and len(self.extras) >= 1:
            for index, extra in enumerate(self.extras):
                if isinstance(extra, dict) and extra['key'] == key:
                    del self.extras[index]
                    return True

        return False
