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

AM_CPPFLAGS = -I$(srcdir)/include -I$(srcdir)/../odbc-driver/include -DIGNITE_IMPL
AM_CXXFLAGS = -Wall -std=c++0x

noinst_PROGRAMS = ignite-odbc-tests

ignite_odbc_tests_SOURCES = src/teamcity_boost.cpp                        \
                       src/teamcity_messages.cpp                          \
                       src/parser_test.cpp                                \
                       src/cursor_test.cpp                                \
                       src/connection_info_test.cpp                       \
                       src/application_data_buffer_test.cpp               \
                       src/configuration_test.cpp                         \
                       src/row_test.cpp                                   \
                       src/utility_test.cpp                               \
                       ../odbc-driver/src/cursor.cpp                      \
                       ../odbc-driver/src/config/connection_info.cpp      \
                       ../odbc-driver/src/app/application_data_buffer.cpp \
                       ../odbc-driver/src/config/configuration.cpp        \
                       ../odbc-driver/src/row.cpp                         \
                       ../odbc-driver/src/column.cpp                      \
                       ../odbc-driver/src/decimal.cpp                     \
                       ../odbc-driver/src/utility.cpp                     \
                       ../odbc-driver/src/result_page.cpp

ignite_odbc_tests_LDFLAGS = -static-libtool-libs -L/usr/local/lib -lignite-binary -ldl

run-check: check
	./ignite-odbc-tests -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(ignite_odbc_tests_OBJECTS)
