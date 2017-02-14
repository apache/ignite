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

lib_LTLIBRARIES = libignite-odbc.la

SUBDIRS = \
    include

AM_CPPFLAGS = \
    -I$(srcdir)/include \
    -I@top_srcdir@/common/include \
    -I@top_srcdir@/common/os/linux/include \
    -I@top_srcdir@/binary/include \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

libignite_odbc_la_LIBADD = \
    @top_srcdir@/binary/libignite-binary.la \
    -lodbcinst

libignite_odbc_la_LDFLAGS = \
    -no-undefined \
    -ldl \
    -version-info 0:0:0 \
    -release $(PACKAGE_VERSION)

libignite_odbc_la_DEPENDENCIES = \
    @top_srcdir@/binary/libignite-binary.la

libignite_odbc_la_SOURCES = \
    os/linux/src/system/socket_client.cpp \
    src/app/application_data_buffer.cpp \
    src/app/parameter.cpp \
    src/common_types.cpp \
    src/config/configuration.cpp \
    src/config/connection_info.cpp \
    src/connection.cpp \
    src/cursor.cpp \
    src/diagnostic/diagnosable_adapter.cpp \
    src/diagnostic/diagnostic_record.cpp \
    src/diagnostic/diagnostic_record_storage.cpp \
    src/environment.cpp \
    src/meta/column_meta.cpp \
    src/meta/table_meta.cpp \
    src/odbc.cpp \
    src/entry_points.cpp \
    src/dsn_config.cpp \
    src/query/column_metadata_query.cpp \
    src/query/data_query.cpp \
    src/query/foreign_keys_query.cpp \
    src/query/primary_keys_query.cpp \
    src/query/table_metadata_query.cpp \
    src/query/type_info_query.cpp \
    src/query/special_columns_query.cpp \
    src/protocol_version.cpp \
    src/result_page.cpp \
    src/row.cpp \
    src/column.cpp \
    src/statement.cpp \
    src/type_traits.cpp \
    src/utility.cpp \
    src/log.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
