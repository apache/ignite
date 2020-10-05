from collections import OrderedDict

from pyignite import Client, GenericObjectMeta
from pyignite.datatypes import *
from pyignite.datatypes.prop_codes import *

# Open a connection
client = Client()
client.connect('127.0.0.1', 10800)

cache_config = {
    PROP_NAME: 'my_cache',
    PROP_BACKUPS_NUMBER: 2,
    PROP_CACHE_KEY_CONFIGURATION: [
        {
            'type_name': 'PersonKey',
            'affinity_key_field_name': 'companyId'
        }
    ]
}

my_cache = client.create_cache(cache_config)


class PersonKey(metaclass=GenericObjectMeta, type_name='PersonKey', schema=OrderedDict([
    ('personId', IntObject),
    ('companyId', IntObject),
])):
    pass


personKey = PersonKey(personId=1, companyId=1)
my_cache.put(personKey, 'test')

print(my_cache.get(personKey))
