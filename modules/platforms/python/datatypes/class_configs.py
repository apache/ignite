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

from .type_codes import *


simple_type_config = {
    TC_BYTE: ('Byte', ctypes.c_byte),
    TC_SHORT: ('Short', ctypes.c_short),
    TC_INT: ('Int', ctypes.c_int),
    TC_LONG: ('Long', ctypes.c_long),
    TC_FLOAT: ('Float', ctypes.c_float),
    TC_DOUBLE: ('Double', ctypes.c_double),
    TC_CHAR: ('Char', ctypes.c_short),
    TC_BOOL: ('Bool', ctypes.c_byte),
}

simple_types = list(simple_type_config.keys())
