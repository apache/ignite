#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ACLOCAL_AMFLAGS =-I m4

noinst_PROGRAMS = ignite-continuous-query-example

AM_CPPFLAGS = \
    -I@top_srcdir@/include \
    -I@top_srcdir@/../core/include \
    -I@top_srcdir@/../core/os/linux/include \
    -I@top_srcdir@/../common/include \
    -I@top_srcdir@/../common/os/linux/include \
    -I@top_srcdir@/../binary/include \
    -I@top_srcdir@/../jni/include \
    -I@top_srcdir@/../jni/os/linux/include \
    -I$(JAVA_HOME)/include \
    -I$(JAVA_HOME)/include/linux \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++0x

ignite_continuous_query_example_LDADD = \
    @top_srcdir@/../core/libignite.la \
    -lpthread

ignite_continuous_query_example_LDFLAGS = \
    -static-libtool-libs

ignite_continuous_query_example_SOURCES = \
    src/continuous_query_example.cpp

run-check: check
	./ignite-continuous-query-example -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(ignite_continuous_query_example_OBJECTS)
