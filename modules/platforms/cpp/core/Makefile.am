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

lib_LTLIBRARIES = libignite.la

SUBDIRS = \
    include

AM_CPPFLAGS = \
    -I$(srcdir)/include \
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

libignite_la_LIBADD = \
    @top_srcdir@/jni/libignite-jni.la \
    @top_srcdir@/binary/libignite-binary.la

libignite_la_LDFLAGS = \
    -no-undefined \
    -ldl \
    -version-info 0:0:0 \
    -release $(PACKAGE_VERSION)

libignite_la_DEPENDENCIES = \
    @top_srcdir@/jni/libignite-jni.la \
    @top_srcdir@/binary/libignite-binary.la

libignite_la_SOURCES = \
    src/ignite.cpp \
    src/ignition.cpp \
    src/impl/ignite_environment.cpp \
    src/impl/binary/binary_type_updater_impl.cpp \
    src/impl/handle_registry.cpp \
    src/impl/cache/query/continuous/continuous_query_handle_impl.cpp \
    src/impl/cache/query/query_impl.cpp \
    src/impl/cache/cache_impl.cpp \
    src/impl/cache/query/query_batch.cpp \
    src/impl/interop/interop_external_memory.cpp \
    src/impl/interop/interop_target.cpp \
    src/impl/transactions/transaction_impl.cpp \
    src/impl/transactions/transactions_impl.cpp \
    src/impl/ignite_impl.cpp \
    src/transactions/transaction.cpp \
    src/transactions/transactions.cpp

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = $(srcdir)/ignite.pc

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
