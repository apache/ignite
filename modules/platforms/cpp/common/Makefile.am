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

lib_LTLIBRARIES = libignite-common.la

SUBDIRS = \
    include \
    os/linux/include

AM_CPPFLAGS = \
    -I$(srcdir)/include \
    -I$(srcdir)/os/linux/include \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

libignite_common_la_LDFLAGS = \
    -no-undefined \
    -L/usr/local/lib \
    -ldl \
    -version-info 0:0:0 \
    -release $(PACKAGE_VERSION)

libignite_common_la_SOURCES = \
    os/linux/src/common/concurrent_os.cpp \
    os/linux/src/common/platform_utils.cpp \
    src/common/big_integer.cpp \
    src/common/concurrent.cpp \
    src/common/decimal.cpp \
    src/common/bits.cpp \
    src/common/utils.cpp \
    src/date.cpp \
    src/ignite_error.cpp \
    src/guid.cpp \
    src/timestamp.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
