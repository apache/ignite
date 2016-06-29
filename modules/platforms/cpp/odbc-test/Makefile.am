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
    -DIGNITE_IMPL

AM_CXXFLAGS = \
    -Wall \
    -std=c++0x

ignite_odbc_tests_LDADD = \
    @top_srcdir@/core/libignite.la \
    -ldl \
    -lodbc \
    -lpthread

ignite_odbc_tests_LDFLAGS = \
    -static-libtool-libs

ignite_odbc_tests_SOURCES = \
    src/teamcity/teamcity_boost.cpp \
    src/teamcity/teamcity_messages.cpp \
    src/parser_test.cpp \
    src/cursor_test.cpp \
    src/connection_info_test.cpp \
    src/application_data_buffer_test.cpp \
    src/column_test.cpp \
    src/configuration_test.cpp \
    src/row_test.cpp \
    src/utility_test.cpp \
    src/queries_test.cpp \
    ../odbc/src/cursor.cpp \
    ../odbc/src/config/connection_info.cpp \
    ../odbc/src/app/application_data_buffer.cpp \
    ../odbc/src/config/configuration.cpp \
    ../odbc/src/row.cpp \
    ../odbc/src/column.cpp \
    ../odbc/src/utility.cpp \
    ../odbc/src/result_page.cpp

run-check: check
	./ignite-odbc-tests -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(ignite_odbc_tests_OBJECTS)
