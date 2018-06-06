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

import socket
from typing import Any, ByteString, Union

from datatypes.class_configs import *
from datatypes.type_codes import *
from .null import null_class, null_object
from .simple import simple_data_class, simple_data_object
from .standard import standard_data_class, standard_data_object
from .string import string_class, string_object


NoneType = type(None)


def data_class(
    python_var: Union[Any, None],
    tc_hint: ByteString=None,
    **kwargs,
) -> object:
    """
    Dispatcher function for data class creation functions.

    :param python_var: variable of native Python type to be converted
    to DataObject. The choice of data class depends on its type.
    If None, tc_hint is used,
    :param tc_hint: direct indicator of required data class type,
    :param kwargs: data class-specific arguments,
    :return: data class.
    """
    python_type = type(python_var)

    if python_type is NoneType:
        return null_class()

    if python_type in simple_python_types or (
        python_type is NoneType and tc_hint in simple_types
    ):
        return simple_data_class(python_var, tc_hint, **kwargs)

    if python_type in standard_python_types or (
        python_type is NoneType and tc_hint in standard_types
    ):
        return standard_data_class(python_var, tc_hint, **kwargs)

    if python_type in (str, bytes) or (
        python_type is NoneType and tc_hint == TC_STRING
    ):
        return string_class(python_var, tc_hint, **kwargs)

    else:
        raise NotImplementedError('This data type is not supported.')


def data_object(connection: socket.socket):
    """
    Dispatcher function for parsing binary stream into data objects.

    :param connection: socket.socket-compatible data stream,
    :return: data object.
    """
    initial = connection.recv(1)
    if initial == TC_NULL:
        return null_object(connection, initial=initial)
    if initial in simple_types:
        return simple_data_object(connection, initial=initial)
    if initial in standard_types:
        return standard_data_object(connection, initial=initial)
    if initial == TC_STRING:
        return string_object(connection, initial=initial)
    raise NotImplementedError('This data type is not supported.')
