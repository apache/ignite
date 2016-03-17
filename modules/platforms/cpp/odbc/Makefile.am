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

ACLOCAL_AMFLAGS = "-Im4"

SUBDIRS = .
DIST_SUBDIRS = .

AM_CPPFLAGS = -I$(srcdir)/include -DIGNITE_IMPL
AM_CXXFLAGS = -Wall -std=c++0x
LIB_LDFLAGS = -no-undefined -version-info 1

COMMON_SRC = os/linux/src/system/socket_client.cpp        \
             src/app/application_data_buffer.cpp          \
             src/app/parameter.cpp                        \
             src/common_types.cpp                         \
             src/config/configuration.cpp                 \
             src/config/connection_info.cpp               \
             src/connection.cpp                           \
             src/cursor.cpp                               \
             src/decimal.cpp                              \
             src/diagnostic/diagnosable_adapter.cpp       \
             src/diagnostic/diagnostic_record.cpp         \
             src/diagnostic/diagnostic_record_storage.cpp \
             src/environment.cpp                          \
             src/meta/column_meta.cpp                     \
             src/meta/table_meta.cpp                      \
             src/odbc.cpp                                 \
             src/entry_points.cpp                         \
             src/query/column_metadata_query.cpp          \
             src/query/data_query.cpp                     \
             src/query/foreign_keys_query.cpp             \
             src/query/primary_keys_query.cpp             \
             src/query/table_metadata_query.cpp           \
             src/query/type_info_query.cpp                \
             src/query/special_columns_query.cpp          \
             src/result_page.cpp                          \
             src/row.cpp                                  \
             src/column.cpp                               \
             src/statement.cpp                            \
             src/type_traits.cpp                          \
             src/utility.cpp


lib_LTLIBRARIES = libignite-odbc.la
libignite_odbc_la_SOURCES = $(COMMON_SRC)
libignite_odbc_la_LDFLAGS = $(LIB_LDFLAGS) -L/usr/local/lib -lignite-utils -lignite-binary -ldl -version-info 0:0:0 -release $(PACKAGE_VERSION)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = ignite-odbc.pc

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
