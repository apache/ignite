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

from pyignite.api import (
    hashcode, get_binary_type, put_binary_type, cache_get_or_create, cache_put,
    scan, scan_cursor_get_page, cache_get,
)
from pyignite.connection import Connection
from pyignite.datatypes import (
    BinaryObject, BoolObject, DateObject, DecimalObject, DoubleObject,
    LongObject, String,
)
from pyignite.datatypes.internal import tc_map


# prepare old data
old_schema = OrderedDict([
    ('date', DateObject),
    ('reported', BoolObject),
    ('purpose', String),
    ('sum', DoubleObject),
    ('recipient', String),
    ('cashier_id', LongObject),
])

old_data = [
    (1, OrderedDict([
        ('date', date(2017, 9, 21)),
        ('reported', True),
        ('purpose', 'Praesent eget fermentum massa'),
        ('sum', 666.67),
        ('recipient', 'John Doe'),
        ('cachier_id', 8),
    ])),
    (2, OrderedDict([
        ('date', date(2017, 10, 11)),
        ('reported', True),
        ('purpose', 'Proin in bibendum nulla'),
        ('sum', 333.33),
        ('recipient', 'Jane Roe'),
        ('cachier_id', 9),
    ])),
    (3, OrderedDict([
        ('date', date(2017, 10, 11)),
        ('reported', True),
        ('purpose', 'Suspendisse nec dolor auctor, scelerisque ex eu, iaculis odio'),
        ('sum', 400.0),
        ('recipient', 'Jane Roe'),
        ('cachier_id', 8),
    ])),
    (4, OrderedDict([
        ('date', date(2017, 10, 24)),
        ('reported', False),
        ('purpose', 'Quisque ut leo ligula'),
        ('sum', 1234.5),
        ('recipient', 'Joe Bloggs'),
        ('cachier_id', 10),
    ])),
    (5, OrderedDict([
        ('date', date(2017, 12, 1)),
        ('reported', True),
        ('purpose', 'Quisque ut leo ligula'),
        ('sum', 800.0),
        ('recipient', 'Richard Public'),
        ('cachier_id', 12),
    ])),
    (6, OrderedDict([
        ('date', date(2017, 12, 1)),
        ('reported', True),
        ('purpose', 'Aenean eget bibendum lorem, a luctus libero'),
        ('sum', 135.79),
        ('recipient', 'Joe Bloggs'),
        ('cachier_id', 10),
    ])),
]

# - rename `date` to `expense_date`
# - add `report_date`
# - set `report_date` to the current date if `reported` is True, None if False
# - delete `reported`
# - change `sum` type from float to decimal
#
# new_schema = {
#     'expense_date': DateObject,
#     'report_date': DateObject,
#     'purpose': String,
#     'sum': DecimalObject,
#     'recipient': String,
#     'cashier_id': LongObject,
# }

conn = Connection()
conn.connect('127.0.0.1', 10800)

cache_get_or_create(conn, 'accounting')

result = put_binary_type(conn, 'ExpenseVoucher', schema=old_schema)

old_type_id = result.value['type_id']
old_schema_id = result.value['schema_id']

for key, value in old_data:
    cache_put(
        conn,
        hashcode('accounting'),
        key,
        {
            'version': 1,
            'type_id': old_type_id,
            'schema_id': old_schema_id,
            'fields': value,
        },
        value_hint=BinaryObject,
    )

# get the old schema by its type id, which can be stored in code
# or calculated as `hashcode('ExpenseVoucher'.lower())`
result = get_binary_type(conn, old_type_id)

schema = OrderedDict([
    (field['field_name'], tc_map(bytes([field['type_id']])))
    for field in result.value['binary_fields']
])

# modify schema
schema['expense_date'] = schema['date']
del schema['date']
schema['report_date'] = DateObject
del schema['reported']
schema['sum'] = DecimalObject

# register the new binary type
result = put_binary_type(
    conn,
    'NewExpenseVoucher',
    schema=schema,
)
new_type_id = result.value['type_id']
new_schema_id = result.value['schema_id']


def migrate(data):
    """ Migrate given data pages. """
    for key, value in data.items():
        # read data
        blob, offset = value
        mock_conn = conn.make_buffered(blob)
        mock_conn.pos = offset
        data_class, data_bytes = BinaryObject.parse(mock_conn)
        fields = BinaryObject.to_python(
            data_class.from_buffer_copy(data_bytes)
        )['fields']

        # process data
        fields['expense_date'] = fields['date']
        del fields['date']
        fields['report_date'] = date.today() if fields['reported'] else None
        del fields['reported']
        fields['sum'] = Decimal(fields['sum']).quantize(Decimal('1.00'))

        # replace data
        cache_put(
            conn,
            hashcode('accounting'),
            key,
            {
                'version': 1,
                'type_id': new_type_id,
                'schema_id': new_schema_id,
                'fields': fields,
            },
            value_hint=BinaryObject,
        )

        # verify data
        verify = cache_get(conn, hashcode('accounting'), key)
        blob, offset = verify.value
        mock_conn = conn.make_buffered(blob)
        mock_conn.pos = offset
        data_class, data_bytes = BinaryObject.parse(mock_conn)
        fields = BinaryObject.to_python(
            data_class.from_buffer_copy(data_bytes)
        )['fields']
        print(dict(fields))


# migrate data
result = scan(conn, hashcode('accounting'), 2)
migrate(result.value['data'])

cursor = result.value['cursor']
while result.value['more']:
    result = scan_cursor_get_page(conn, cursor)
    migrate(result.value['data'])
