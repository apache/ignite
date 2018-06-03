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

import ctypes
import socket

from datatypes.class_configs import simple_type_config
from datatypes.type_codes import *


def simple_data_object(connection: socket.socket):
    buffer = connection.recv(1)
    type_code = int.from_bytes(buffer, byteorder='little')
    class_name, ctypes_type = simple_type_config[type_code]
    data_class = type(
        class_name,
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': [
                ('type_code', ctypes.c_byte),
                ('value', ctypes_type),
            ],
        },
    )
    buffer += connection.recv(ctypes.sizeof(simple_type_config[type_code][1]))
    data_object = data_class.from_buffer_copy(buffer)
    return data_object


def string_object(connection: socket.socket):
    buffer = connection.recv(1)
    type_code = int.from_bytes(buffer, byteorder='little')
    assert type_code == TC_STRING, 'Can not create string: wrong type code.'
    length_buffer = connection.recv(4)
    length = int.from_bytes(length_buffer, byteorder='little')
    data_class = type(
        'String',
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': [
                ('type_code', ctypes.c_byte),
                ('length', ctypes.c_int),
                ('data', ctypes.c_char * length),
            ],
        },
    )
    buffer += length_buffer + connection.recv(length)
    data_object = data_class.from_buffer_copy(buffer)
    return data_object
