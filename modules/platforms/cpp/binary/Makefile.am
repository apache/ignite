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

lib_LTLIBRARIES = libignite-binary.la

SUBDIRS = \
    include

AM_CPPFLAGS = \
    -I$(srcdir)/include \
    -I@top_srcdir@/common/include \
    -I@top_srcdir@/common/os/linux/include \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

libignite_binary_la_LIBADD = \
    @top_srcdir@/common/libignite-common.la

libignite_binary_la_LDFLAGS = \
    -no-undefined \
    -version-info 0:0:0 \
    -release $(PACKAGE_VERSION)

libignite_binary_la_DEPENDENCIES = \
    @top_srcdir@/common/libignite-common.la

libignite_binary_la_SOURCES = \
    src/binary/binary_containers.cpp \
    src/binary/binary_raw_writer.cpp \
    src/binary/binary_writer.cpp \
    src/binary/binary_reader.cpp \
    src/binary/binary_type.cpp \
    src/binary/binary_raw_reader.cpp \
    src/binary/binary_array_identity_resolver.cpp \
    src/impl/binary/binary_type_manager.cpp \
    src/impl/binary/binary_utils.cpp \
    src/impl/binary/binary_reader_impl.cpp \
    src/impl/binary/binary_type_handler.cpp \
    src/impl/binary/binary_writer_impl.cpp \
    src/impl/binary/binary_type_updater.cpp \
    src/impl/binary/binary_schema.cpp \
    src/impl/binary/binary_type_snapshot.cpp \
    src/impl/binary/binary_object_header.cpp \
    src/impl/binary/binary_object_impl.cpp \
    src/impl/interop/interop_memory.cpp \
    src/impl/interop/interop_output_stream.cpp \
    src/impl/interop/interop_input_stream.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
