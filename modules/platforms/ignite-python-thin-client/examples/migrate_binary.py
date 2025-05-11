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

from datetime import date
from decimal import Decimal
from pprint import pprint

from helpers.converters import obj_to_dict
from pyignite import Client, GenericObjectMeta
from pyignite.datatypes import BoolObject, DateObject, DecimalObject, LongObject, String

# prepare old data
old_schema = {'date': DateObject,
              'reported': BoolObject,
              'purpose': String,
              'sum': DecimalObject,
              'recipient': String,
              'cashier_id': LongObject
              }

old_data = {
    1: {
        'date': date(2017, 9, 21),
        'reported': True,
        'purpose': 'Praesent eget fermentum massa',
        'sum': Decimal('666.67'),
        'recipient': 'John Doe',
        'cashier_id': 8,
    },
    2: {
        'date': date(2017, 10, 11),
        'reported': True,
        'purpose': 'Proin in bibendum nulla',
        'sum': Decimal('333.33'),
        'recipient': 'Jane Roe',
        'cashier_id': 9,
    },
    3: {
        'date': date(2017, 10, 11),
        'reported': True,
        'purpose': 'Suspendisse nec dolor auctor, scelerisque ex eu, iaculis odio',
        'sum': Decimal('400.0'),
        'recipient': 'Jane Roe',
        'cashier_id': 8,
    },
    4: {
        'date': date(2017, 10, 24),
        'reported': False,
        'purpose': 'Quisque ut leo ligula',
        'sum': Decimal('1234.5'),
        'recipient': 'Joe Bloggs',
        'cashier_id': 10,
    },
    5: {
        'date': date(2017, 12, 1),
        'reported': True,
        'purpose': 'Quisque ut leo ligula',
        'sum': Decimal('800.0'),
        'recipient': 'Richard Public',
        'cashier_id': 12,
    },
    6: {
        'date': date(2017, 12, 1),
        'reported': True,
        'purpose': 'Aenean eget bibendum lorem, a luctus libero',
        'sum': Decimal('135.79'),
        'recipient': 'Joe Bloggs',
        'cashier_id': 10,
    }
}


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

with client.connect('127.0.0.1', 10800):
    accounting = client.get_or_create_cache('accounting')

    for item, value in old_data.items():
        print(item)
        accounting.put(item, ExpenseVoucher(**value))

    data_classes = client.query_binary_type('ExpenseVoucher')
    print(data_classes)
    # {
    #     {547629991: <class 'pyignite.binary.ExpenseVoucher'>, -231598180: <class '__main__.ExpenseVoucher'>}
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
        print('Old value:')
        pprint(obj_to_dict(old_value))
        # Old value:
        # {'cashier_id': 10,
        #  'date': datetime.datetime(2017, 12, 1, 0, 0),
        #  'purpose': 'Aenean eget bibendum lorem, a luctus libero',
        #  'recipient': 'Joe Bloggs',
        #  'reported': True,
        #  'sum': Decimal('135.79'),
        #  'type_name': 'ExpenseVoucher'}

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
        print('New value:')
        pprint(obj_to_dict(verify))
        # New value:
        # {'cashier_id': 10,
        #  'expense_date': datetime.datetime(2017, 12, 1, 0, 0),
        #  'purpose': 'Aenean eget bibendum lorem, a luctus libero',
        #  'recipient': 'Joe Bloggs',
        #  'report_date': datetime.datetime(2022, 5, 6, 0, 0),
        #  'sum': Decimal('135.79'),
        #  'type_name': 'ExpenseVoucher'}

        print('-' * 20)


# migrate data
with client.connect('127.0.0.1', 10800):
    accounting = client.get_or_create_cache('accounting')

    with accounting.scan() as cursor:
        migrate(accounting, cursor, ExpenseVoucherV2)

    # cleanup
    accounting.destroy()
