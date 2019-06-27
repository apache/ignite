#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from pygridgain.datatypes import Int, Bool, String, Struct, StructArray


binary_fields_struct = StructArray([
    ('field_name', String),
    ('type_id', Int),
    ('field_id', Int),
])

body_struct = Struct([
    ('type_id', Int),
    ('type_name', String),
    ('affinity_key_field', String),
    ('binary_fields', binary_fields_struct),
    ('is_enum', Bool),
])

enum_struct = StructArray([
    ('literal', String),
    ('type_id', Int),
])

schema_fields_struct = StructArray([
    ('schema_field_id', Int),
])

schema_struct = StructArray([
    ('schema_id', Int),
    ('schema_fields', schema_fields_struct),
])
