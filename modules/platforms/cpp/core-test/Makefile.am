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

noinst_PROGRAMS = ignite-tests

SUBDIRS = \
    include

AM_CPPFLAGS = \
    -I$(srcdir)/include \
    -I@top_srcdir@/core/include \
    -I@top_srcdir@/core/os/linux/include \
    -I@top_srcdir@/common/include \
    -I@top_srcdir@/common/os/linux/include \
    -I@top_srcdir@/binary/include \
    -I@top_srcdir@/jni/include \
    -I@top_srcdir@/jni/os/linux/include \
    -I$(JAVA_HOME)/include \
    -I$(JAVA_HOME)/include/linux \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

ignite_tests_LDADD = \
    @top_srcdir@/core/libignite.la \
    -lpthread \
    -lboost_thread \
    -lboost_system \
    -lboost_chrono

ignite_tests_LDFLAGS = \
    -static-libtool-libs

ignite_tests_SOURCES = \
    src/reference_test.cpp \
    src/bits_test.cpp \
	src/binary_identity_resolver_test.cpp \
    src/cache_test.cpp \
    src/cache_query_test.cpp \
    src/continuous_query_test.cpp \
    src/concurrent_test.cpp \
    src/ignition_test.cpp \
    src/interop_memory_test.cpp \
    src/interop_test.cpp \
    src/handle_registry_test.cpp \
    src/ignite_error_test.cpp \
    src/binary_test_defs.cpp \
    src/binary_object_test.cpp \
    src/binary_reader_writer_raw_test.cpp \
    src/binary_reader_writer_test.cpp \
    src/binary_session_test.cpp \
    src/decimal_test.cpp \
    src/dynamic_size_array_test.cpp \
    src/fixed_size_array_test.cpp \
    src/transactions_test.cpp \
    src/teamcity_messages.cpp \
    src/teamcity_boost.cpp \
	src/test_utils.cpp

run-check: check
	./ignite-tests -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(ignite_tests_OBJECTS)
