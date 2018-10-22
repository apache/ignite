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

from pyignite import Client, GenericObjectMeta
from pyignite.datatypes import (
    BoolObject, DateObject, DecimalObject, LongObject, String,
)


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
    (1, {
        'date': date(2017, 9, 21),
        'reported': True,
        'purpose': 'Praesent eget fermentum massa',
        'sum': Decimal('666.67'),
        'recipient': 'John Doe',
        'cashier_id': 8,
    }),
    (2, {
        'date': date(2017, 10, 11),
        'reported': True,
        'purpose': 'Proin in bibendum nulla',
        'sum': Decimal('333.33'),
        'recipient': 'Jane Roe',
        'cashier_id': 9,
    }),
    (3, {
        'date': date(2017, 10, 11),
        'reported': True,
        'purpose': 'Suspendisse nec dolor auctor, scelerisque ex eu, iaculis odio',
        'sum': Decimal('400.0'),
        'recipient': 'Jane Roe',
        'cashier_id': 8,
    }),
    (4, {
        'date': date(2017, 10, 24),
        'reported': False,
        'purpose': 'Quisque ut leo ligula',
        'sum': Decimal('1234.5'),
        'recipient': 'Joe Bloggs',
        'cashier_id': 10,
    }),
    (5, {
        'date': date(2017, 12, 1),
        'reported': True,
        'purpose': 'Quisque ut leo ligula',
        'sum': Decimal('800.0'),
        'recipient': 'Richard Public',
        'cashier_id': 12,
    }),
    (6, {
        'date': date(2017, 12, 1),
        'reported': True,
        'purpose': 'Aenean eget bibendum lorem, a luctus libero',
        'sum': Decimal('135.79'),
        'recipient': 'Joe Bloggs',
        'cashier_id': 10,
    }),
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
#     'cashier_id': LongObject,
# }


class ExpenseVoucher(
    metaclass=GenericObjectMeta,
    schema=old_schema,
):
    pass


client = Client()
client.connect('127.0.0.1', 10800)

accounting = client.get_or_create_cache('accounting')

for key, value in old_data:
    accounting.put(key, ExpenseVoucher(**value))

data_classes = client.query_binary_type('ExpenseVoucher')
print(data_classes)
# {
#     -231598180: <class '__main__.ExpenseVoucher'>
# }

s_id, data_class = data_classes.popitem()
schema = data_class.schema

schema['expense_date'] = schema['date']
del schema['date']
schema['report_date'] = DateObject
del schema['reported']
schema['sum'] = DecimalObject


# define new data class
class ExpenseVoucherV2(
    metaclass=GenericObjectMeta,
    type_name='ExpenseVoucher',
    schema=schema,
):
    pass


def migrate(cache, data, new_class):
    """ Migrate given data pages. """
    for key, old_value in data:
        # read data
        print(old_value)
        # ExpenseVoucher(
        #     date=datetime(2017, 9, 21, 0, 0),
        #     reported=True,
        #     purpose='Praesent eget fermentum massa',
        #     sum=Decimal('666.67'),
        #     recipient='John Doe',
        #     cashier_id=8,
        #     version=1
        # )

        # create new binary object
        new_value = new_class()

        # process data
        new_value.sum = old_value.sum
        new_value.purpose = old_value.purpose
        new_value.recipient = old_value.recipient
        new_value.cashier_id = old_value.cashier_id
        new_value.expense_date = old_value.date
        new_value.report_date = date.today() if old_value.reported else None

        # replace data
        cache.put(key, new_value)

        # verify data
        verify = cache.get(key)
        print(verify)
        # ExpenseVoucherV2(
        #     purpose='Praesent eget fermentum massa',
        #     sum=Decimal('666.67'),
        #     recipient='John Doe',
        #     cashier_id=8,
        #     expense_date=datetime(2017, 9, 21, 0, 0),
        #     report_date=datetime(2018, 8, 29, 0, 0),
        #     version=1,
        # )


# migrate data
result = accounting.scan()
migrate(accounting, result, ExpenseVoucherV2)

# cleanup
accounting.destroy()
client.close()
