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

from collections import OrderedDict
from datetime import date
from decimal import Decimal

from pyignite.connection import Connection
from pyignite.datatypes import (
    BinaryObject, BoolObject, DateObject, DecimalObject, LongObject, String,
)
from pyignite.datatypes.internal import tc_map


# prepare old data
old_schema = OrderedDict([
    ('date', DateObject),
    ('reported', BoolObject),
    ('purpose', String),
    ('sum', DecimalObject),
    ('recipient', String),
    ('cashier_id', LongObject),
])

old_data = [
    (1, OrderedDict([
        ('date', date(2017, 9, 21)),
        ('reported', True),
        ('purpose', 'Praesent eget fermentum massa'),
        ('sum', Decimal('666.67')),
        ('recipient', 'John Doe'),
        ('cachier_id', 8),
    ])),
    (2, OrderedDict([
        ('date', date(2017, 10, 11)),
        ('reported', True),
        ('purpose', 'Proin in bibendum nulla'),
        ('sum', Decimal('333.33')),
        ('recipient', 'Jane Roe'),
        ('cachier_id', 9),
    ])),
    (3, OrderedDict([
        ('date', date(2017, 10, 11)),
        ('reported', True),
        ('purpose', 'Suspendisse nec dolor auctor, scelerisque ex eu, iaculis odio'),
        ('sum', Decimal('400.0')),
        ('recipient', 'Jane Roe'),
        ('cachier_id', 8),
    ])),
    (4, OrderedDict([
        ('date', date(2017, 10, 24)),
        ('reported', False),
        ('purpose', 'Quisque ut leo ligula'),
        ('sum', Decimal('1234.5')),
        ('recipient', 'Joe Bloggs'),
        ('cachier_id', 10),
    ])),
    (5, OrderedDict([
        ('date', date(2017, 12, 1)),
        ('reported', True),
        ('purpose', 'Quisque ut leo ligula'),
        ('sum', Decimal('800.0')),
        ('recipient', 'Richard Public'),
        ('cachier_id', 12),
    ])),
    (6, OrderedDict([
        ('date', date(2017, 12, 1)),
        ('reported', True),
        ('purpose', 'Aenean eget bibendum lorem, a luctus libero'),
        ('sum', Decimal('135.79')),
        ('recipient', 'Joe Bloggs'),
        ('cachier_id', 10),
    ])),
]

# - add `report_date`
# - set `report_date` to the current date if `reported` is True, None if False
# - delete `reported`
#
# new_schema = {
#     'date': DateObject,
#     'report_date': DateObject,
#     'purpose': String,
#     'sum': DecimalObject,
#     'recipient': String,
#     'cashier': LongObject,
# }

conn = Connection()
conn.connect('127.0.0.1', 10800)

accounting = conn.create_cache('accounting')

result = conn.put_binary_type(
    'ExpenseVoucher',
    schema=old_schema,
)

type_id = result['type_id']
old_schema_id = result['schema_id']

for key, value in old_data:
    accounting.put(
        key,
        {
            'version': 1,
            'type_id': type_id,
            'schema_id': old_schema_id,
            'fields': value,
        },
        value_hint=BinaryObject,
    )

result = conn.get_binary_type('ExpenseVoucher')
print(result)

# {
#     'type_id': -1171639466,
#     'type_name': 'ExpenseVoucher',
#     'is_enum': False,
#     'affinity_key_field': None,
#     'binary_fields': [
#         {'type_id': 11, 'field_id': 3076014, 'field_name': 'date'},
#         {'type_id': 8, 'field_id': -427039533, 'field_name': 'reported'},
#         {'type_id': 9, 'field_id': -220463842, 'field_name': 'purpose'},
#         {'type_id': 30, 'field_id': 114251, 'field_name': 'sum'},
#         {'type_id': 9, 'field_id': 820081177, 'field_name': 'recipient'},
#         {'type_id': 4, 'field_id': -2030736361, 'field_name': 'cashier_id'},
#     ],
#     'schema': {
#         -231598180: [
#             3076014,
#             -427039533,
#             -220463842,
#             114251,
#             820081177,
#             -2030736361,
#         ],
#     },
#     'type_exists': True,
# }

schema = OrderedDict([
    (field['field_name'], tc_map(bytes([field['type_id']])))
    for field in result['binary_fields']
])

schema['expense_date'] = schema['date']
del schema['date']
schema['report_date'] = DateObject
del schema['reported']
schema['sum'] = DecimalObject

result = conn.put_binary_type(
    'ExpenseVoucher',
    schema=schema,
)
new_schema_id = result['schema_id']

result = conn.get_binary_type(type_id)
print(result)

# {
#     'type_id': -1171639466,
#     'type_name': 'ExpenseVoucher',
#     'is_enum': False,
#     'affinity_key_field': None,
#     'binary_fields': [
#         {'type_id': 11, 'field_id': 3076014, 'field_name': 'date'},
#         {'type_id': 8, 'field_id': -427039533, 'field_name': 'reported'},
#         {'type_id': 9, 'field_id': -220463842, 'field_name': 'purpose'},
#         {'type_id': 30, 'field_id': 114251, 'field_name': 'sum'},
#         {'type_id': 9, 'field_id': 820081177, 'field_name': 'recipient'},
#         {'type_id': 4, 'field_id': -2030736361, 'field_name': 'cashier_id'},
#         {'type_id': 11, 'field_id': 1264342837, 'field_name': 'expense_date'},
#         {'type_id': 11, 'field_id': -247041063, 'field_name': 'report_date'},
#     ], 'schema': {
#         -231598180: [
#             3076014,
#             -427039533,
#             -220463842,
#             114251,
#             820081177,
#             -2030736361,
#         ],
#         547629991: [
#             -220463842,
#             114251,
#             820081177,
#             -2030736361,
#             1264342837,
#             -247041063,
#         ]
#     },
#     'type_exists': True,
# }


def migrate(cache, data):
    """ Migrate given data pages. """
    for key, value in data:
        # read data
        print(value)
        # {
        #     'cashier_id': 8,
        #     'date': datetime.datetime(2017, 9, 21, 0, 0),
        #     'sum': Decimal('666.67'),
        #     'reported': True,
        #     'purpose': 'Praesent eget fermentum massa',
        #     'recipient': 'John Doe',
        # }

        # process data
        fields = value['fields']
        fields['expense_date'] = fields['date']
        del fields['date']
        fields['report_date'] = date.today() if fields['reported'] else None
        del fields['reported']
        value['schema_id'] = new_schema_id

        # replace data
        cache.put(key, value, value_hint=BinaryObject)

        # verify data
        verify = cache.get(key)
        print(verify['fields'])
        # {
        #     'cashier_id': 8,
        #     'sum': Decimal('666.67'),
        #     'report_date': datetime.datetime(2018, 7, 24, 0, 0),
        #     'expense_date': datetime.datetime(2017, 9, 21, 0, 0),
        #     'recipient': 'John Doe',
        #     'purpose': 'Praesent eget fermentum massa',
        # }


# migrate data
result = accounting.scan()
migrate(accounting, result)

# cleanup
accounting.destroy()
conn.close()
