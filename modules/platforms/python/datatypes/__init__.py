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


class BaseDataType(ctypes.LittleEndianStructure):

    @staticmethod
    def __new__(cls, connection: socket.socket, *args, **kwargs) -> object:
        if cls is BaseDataType:
            type_code = int.from_bytes(
                connection.recv(1),
                byteorder='little',
            )
            class_name, ctypes_type = simple_type_config[type_code]
            data_class = type(
                class_name,
                (BaseDataType,),
                {
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('value', ctypes_type)
                    ],
                    '_type_code': type_code,
                },
            )
            return data_class.__new__(data_class, connection)
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, connection):
        super().__init__()
        self.type_code = self._type_code
        self.value = self.value.from_bytes(
            connection.recv(ctypes.sizeof(simple_type_config[self.type_code][1])),
            byteorder='little',
        )
