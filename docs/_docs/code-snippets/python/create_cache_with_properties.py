# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#tag::example-block[]
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
#end::example-block[]
