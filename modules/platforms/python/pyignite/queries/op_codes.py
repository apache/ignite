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

"""
Named constants that represents request operation codes. These are the way
of telling Ignite server what one want to do in their request.
"""

OP_SUCCESS = 0

OP_RESOURCE_CLOSE = 0

OP_CACHE_GET = 1000
OP_CACHE_PUT = 1001
OP_CACHE_PUT_IF_ABSENT = 1002
OP_CACHE_GET_ALL = 1003
OP_CACHE_PUT_ALL = 1004
OP_CACHE_GET_AND_PUT = 1005
OP_CACHE_GET_AND_REPLACE = 1006
OP_CACHE_GET_AND_REMOVE = 1007
OP_CACHE_GET_AND_PUT_IF_ABSENT = 1008
OP_CACHE_REPLACE = 1009
OP_CACHE_REPLACE_IF_EQUALS = 1010
OP_CACHE_CONTAINS_KEY = 1011
OP_CACHE_CONTAINS_KEYS = 1012
OP_CACHE_CLEAR = 1013
OP_CACHE_CLEAR_KEY = 1014
OP_CACHE_CLEAR_KEYS = 1015
OP_CACHE_REMOVE_KEY = 1016
OP_CACHE_REMOVE_IF_EQUALS = 1017
OP_CACHE_REMOVE_KEYS = 1018
OP_CACHE_REMOVE_ALL = 1019
OP_CACHE_GET_SIZE = 1020

OP_CACHE_GET_NAMES = 1050
OP_CACHE_CREATE_WITH_NAME = 1051
OP_CACHE_GET_OR_CREATE_WITH_NAME = 1052
OP_CACHE_CREATE_WITH_CONFIGURATION = 1053
OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION = 1054
OP_CACHE_GET_CONFIGURATION = 1055
OP_CACHE_DESTROY = 1056

OP_QUERY_SCAN = 2000
OP_QUERY_SCAN_CURSOR_GET_PAGE = 2001
OP_QUERY_SQL = 2002
OP_QUERY_SQL_CURSOR_GET_PAGE = 2003
OP_QUERY_SQL_FIELDS = 2004
OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE = 2005

P_GET_BINARY_TYPE_NAME = 3000
OP_REGISTER_BINARY_TYPE_NAME = 3001
OP_GET_BINARY_TYPE = 3002
OP_PUT_BINARY_TYPE = 3003
