##
##                   GridGain Community Edition Licensing
##                   Copyright 2019 GridGain Systems, Inc.
##
## Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
## Restriction;  you may not use this file except in compliance with the License. You may obtain a
## copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software distributed under the
## License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied. See the License for the specific language governing permissions
## and limitations under the License.
##
## Commons Clause Restriction
##
## The Software is provided to you by the Licensor under the License, as defined below, subject to
## the following condition.
##
## Without limiting other conditions in the License, the grant of rights under the License will not
## include, and the License does not grant to you, the right to Sell the Software.
## For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
## under the License to provide to third parties, for a fee or other consideration (including without
## limitation fees for hosting or consulting/ support services related to the Software), a product or
## service whose value derives, entirely or substantially, from the functionality of the Software.
## Any license notice or attribution required by the License must also include this Commons Clause
## License Condition notice.
##
## For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
## the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
## Edition software provided with this notice.
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
	-D__STDC_CONSTANT_MACROS \
	-DBOOST_TEST_DYN_LINK

AM_CXXFLAGS = \
	-Wall \
	-std=c++03

ignite_tests_LDADD = \
	@top_srcdir@/core/libignite.la \
	-lpthread \
	-lboost_thread \
	-lboost_system \
	-lboost_chrono \
	-lboost_unit_test_framework

ignite_tests_LDFLAGS = \
	-static-libtool-libs \
	-rdynamic

ignite_tests_SOURCES = \
	src/reference_test.cpp \
	src/bits_test.cpp \
	src/binary_identity_resolver_test.cpp \
	src/cache_test.cpp \
	src/cache_query_test.cpp \
	src/cache_store_test.cpp \
	src/continuous_query_test.cpp \
	src/concurrent_test.cpp \
	src/compute_test.cpp \
	src/ignition_test.cpp \
	src/interop_memory_test.cpp \
	src/interop_test.cpp \
	src/cluster_test.cpp \
	src/cache_invoke_test.cpp \
	src/handle_registry_test.cpp \
	src/ignite_error_test.cpp \
	src/binary_test_defs.cpp \
	src/binary_object_test.cpp \
	src/binary_reader_writer_raw_test.cpp \
	src/binary_reader_writer_test.cpp \
	src/binary_session_test.cpp \
	src/date_time_test.cpp \
	src/decimal_test.cpp \
	src/dynamic_size_array_test.cpp \
	src/fixed_size_array_test.cpp \
	src/future_test.cpp \
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
