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

noinst_PROGRAMS = ignite-odbc-tests

SUBDIRS = \
    include

AM_CPPFLAGS = \
    -I$(srcdir)/include \
    -I@top_srcdir@/common/include \
    -I@top_srcdir@/common/os/linux/include \
    -I@top_srcdir@/binary/include \
    -I@top_srcdir@/odbc/include \
    -I@top_srcdir@/jni/include \
    -I@top_srcdir@/jni/os/linux/include \
    -I@top_srcdir@/core/include \
    -I@top_srcdir@/core/os/linux/include \
    -I$(JAVA_HOME)/include \
    -I$(JAVA_HOME)/include/linux \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

ignite_odbc_tests_LDADD = \
    @top_srcdir@/core/libignite.la \
    -ldl \
    -lodbc \
    -lpthread \
    -lboost_regex

ignite_odbc_tests_LDFLAGS = \
    -static-libtool-libs

ignite_odbc_tests_SOURCES = \
    src/teamcity/teamcity_boost.cpp \
    src/teamcity/teamcity_messages.cpp \
    src/parser_test.cpp \
    src/cursor_test.cpp \
    src/connection_info_test.cpp \
    src/connection_test.cpp \
    src/application_data_buffer_test.cpp \
    src/column_test.cpp \
    src/configuration_test.cpp \
    src/row_test.cpp \
    src/meta_queries_test.cpp \
    src/utility_test.cpp \
    src/queries_test.cpp \
    src/queries_ssl_test.cpp \
    src/test_utils.cpp \
    src/sql_test_suite_fixture.cpp \
    src/sql_string_functions_test.cpp \
    src/sql_numeric_functions_test.cpp \
    src/sql_aggregate_functions_test.cpp \
    src/sql_system_functions_test.cpp \
    src/sql_esc_convert_function_test.cpp \
    src/sql_operators_test.cpp \
    src/sql_value_expressions_test.cpp \
    src/sql_types_test.cpp \
    src/sql_date_time_functions_test.cpp \
    src/sql_outer_join_test.cpp \
    src/sql_get_info_test.cpp \
    src/api_robustness_test.cpp \
    src/attributes_test.cpp \
    src/errors_test.cpp \
    src/odbc_test_suite.cpp \
    src/types_test.cpp \
    src/transaction_test.cpp \
    src/authentication_test.cpp \
    ../odbc/src/log.cpp \
    ../odbc/src/cursor.cpp \
    ../odbc/src/diagnostic/diagnostic_record.cpp \
    ../odbc/src/diagnostic/diagnostic_record_storage.cpp \
    ../odbc/src/config/config_tools.cpp \
    ../odbc/src/config/configuration.cpp \
    ../odbc/src/config/connection_info.cpp \
    ../odbc/src/config/connection_string_parser.cpp \
    ../odbc/src/app/application_data_buffer.cpp \
    ../odbc/src/ssl/ssl_mode.cpp \
    ../odbc/src/row.cpp \
    ../odbc/src/protocol_version.cpp \
    ../odbc/src/column.cpp \
    ../odbc/src/common_types.cpp \
    ../odbc/src/utility.cpp \
    ../odbc/src/result_page.cpp \
    ../odbc/src/nested_tx_mode.cpp

run-check: check
	./ignite-odbc-tests -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(ignite_odbc_tests_OBJECTS)
