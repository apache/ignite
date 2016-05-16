##
## Licensed to the Apache Software Foundation (ASF) under one or more
## contributor license agreements.  See the NOTICE file distributed with
## this work for additional information regarding copyright ownership.
## The ASF licenses this file to You under the Apache License, Version 2.0
## (the "License"); you may not use this file except in compliance with
## the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##

ACLOCAL_AMFLAGS =-I m4

if COND_ODBC
  MAYBE_ODBC = odbc

if COND_TESTS
    MAYBE_ODBC_TESTS = odbc-test
endif
endif

if COND_CORE
  MAYBE_CORE = core
  MAYBE_JNI = jni

if COND_TESTS
    MAYBE_CORE_TESTS = core-test
endif
endif

if COND_NODE
  MAYBE_CORE = core
  MAYBE_JNI = jni
  MAYBE_NODE = ignite
endif

SUBDIRS = \
    common \
    binary \
    $(MAYBE_JNI) \
    $(MAYBE_ODBC) \
    $(MAYBE_CORE) \
    $(MAYBE_NODE) \
    $(MAYBE_ODBC_TESTS) \
    $(MAYBE_CORE_TESTS)

DIST_SUBDIRS = \
    common \
    binary \
    jni \
    odbc \
    core \
    ignite \
    core-test \
    odbc-test
