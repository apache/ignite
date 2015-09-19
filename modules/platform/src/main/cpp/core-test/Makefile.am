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

SUBDIRS = . include
DIST_SUBDIRS = . include

AM_CPPFLAGS = -I$(srcdir)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -DIGNITE_IMPL
AM_CXXFLAGS = -Wall -std=c++0x

noinst_PROGRAMS = ignite-tests

ignite_tests_SOURCES = src/cache_test.cpp \
                         src/cache_query_test.cpp \
                         src/concurrent_test.cpp \
                         src/ignition_test.cpp \
                         src/handle_registry_test.cpp \
                         src/portable_test_defs.cpp \
                         src/portable_reader_writer_raw_test.cpp \
                         src/portable_reader_writer_test.cpp \
                         src/portable_session_test.cpp \
                         src/teamcity_messages.cpp \
                         src/teamcity_boost.cpp

ignite_tests_LDFLAGS = -static-libtool-libs -L/usr/local/lib -lignite

run-check: check
	./ignite-tests -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(ignite_tests_OBJECTS)
